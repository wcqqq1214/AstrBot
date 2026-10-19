import asyncio
import re
import time
import wave
from contextvars import ContextVar
from copy import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import ServerDisconnectedError, web
from aiohttp.test_utils import TestServer
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.web.async_slack_response import AsyncSlackResponse

from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import At, Image, Plain, Record, Reply
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    PlatformMetadata,
)
from astrbot.core.agent.response import AgentResponse
from astrbot.core.astr_agent_run_util import run_agent
from astrbot.core.message.message_event_result import (
    MessageEventResult,
    ResultContentType,
)
from astrbot.core.pipeline.respond.stage import RespondStage
from astrbot.core.platform.sources.slack.slack_event import SlackMessageEvent


@pytest.fixture(params=[False, True], ids=["channel-edit", "thread-native"])
def stream(request, monkeypatch):
    message = AstrBotMessage()
    message.type = MessageType.GROUP_MESSAGE
    message.group_id = "C123"
    message.sender = MessageMember(user_id="U123", nickname="Tester")
    message.raw_message = {"channel": "C123", "team": "T123"}
    if request.param:
        message.raw_message["thread_ts"] = "0.1"
    client = AsyncWebClient(token="xoxb-test")
    client.api_call = AsyncMock(return_value={"ok": True, "ts": "1.0"})
    event = SlackMessageEvent(
        "test",
        message,
        PlatformMetadata(name="slack", id="slack", description="Slack"),
        "C123",
        client,
    )
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(
        "astrbot.core.platform.sources.slack.slack_stream.time",
        SimpleNamespace(monotonic=lambda: clock.now),
    )
    monkeypatch.setattr(AstrMessageEvent, "send", AsyncMock())
    monkeypatch.setattr(AstrMessageEvent, "send_streaming", AsyncMock())
    monkeypatch.setattr(
        "astrbot.core.pipeline.respond.stage.call_event_hook",
        AsyncMock(return_value=False),
    )
    return SimpleNamespace(
        event=event, api=client.api_call, clock=clock, native=request.param
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("strategy", ["realtime_segmenting", "buffer"])
async def test_progressive_lists_through_respond_stage(stream, strategy):
    parts = [
        "- 父项。\n",
        "  - 子项！\n    - 孙项。",
        "\n\n独立段落。\n\n1. 准备\n2. 执行\n   1. 检查\n3. 完成",
    ]

    async def chunks():
        yield MessageChain([Reply(id="0.1"), Plain(parts[0])])
        assert stream.api.await_count == 1  # Visible before generation completes.
        yield MessageChain([Plain(parts[1])])
        assert stream.api.await_count == 1  # Throttled, without dropping deltas.
        stream.clock.now = 2.0
        yield MessageChain([Plain(parts[2]), At(qq="U456")])

    stream.event.set_result(
        MessageEventResult(
            result_content_type=ResultContentType.STREAMING_RESULT,
            async_stream=chunks(),
        )
    )
    stage = RespondStage()
    stage.config = {"provider_settings": {"unsupported_streaming_strategy": strategy}}
    await stage.process(stream.event)
    calls = stream.api.await_args_list
    if stream.native:
        assert [call.args[0] for call in calls] == [
            "chat.startStream",
            "chat.appendStream",
            "chat.appendStream",
            "chat.stopStream",
        ]
        assert calls[0].kwargs["json"] == {
            "channel": "C123",
            "thread_ts": "0.1",
            "recipient_team_id": "T123",
            "recipient_user_id": "U123",
            "markdown_text": parts[0],
        }
        assert (
            "".join(call.kwargs["json"].get("markdown_text", "") for call in calls)
            == "".join(parts) + "<@U456>"
        )
    else:
        assert [call.args[0] for call in calls] == [
            "chat.postMessage",
            "chat.update",
            "chat.update",
        ]
        assert calls[-1].kwargs["json"]["markdown_text"] == "".join(parts) + "<@U456>"
        assert "thread_ts" not in calls[0].kwargs["json"]
    assert all(call.kwargs["json"].get("ts", "1.0") == "1.0" for call in calls)


@pytest.mark.asyncio
async def test_media_and_tool_boundaries_close_before_next_send(stream):
    async def chunks():
        yield MessageChain([Plain("图片前。\n  - 子项")])
        yield MessageChain(
            [Image.fromURL("https://example.com/image.png"), Plain("图片后")]
        )
        yield MessageChain([], type="break")
        await stream.event.send(MessageChain([Plain("工具状态")]))
        yield MessageChain([Plain("工具结果")])

    await stream.event.send_streaming(chunks(), use_fallback=True)
    calls = [(c.args[0], c.kwargs["json"]) for c in stream.api.await_args_list]
    if stream.native:
        assert [m for m, _ in calls] == [
            "chat.startStream",
            "chat.stopStream",
            "chat.postMessage",
            "chat.startStream",
            "chat.stopStream",
            "chat.postMessage",
            "chat.startStream",
            "chat.stopStream",
        ]
    else:
        assert [m for m, _ in calls] == ["chat.postMessage"] * 5
    messages = [p for m, p in calls if m in {"chat.postMessage", "chat.startStream"}]
    assert [p.get("markdown_text", "image") for p in messages] == [
        "图片前。\n  - 子项",
        "image",
        "图片后",
        "工具状态",
        "工具结果",
    ]
    assert messages[1]["blocks"][0]["type"] == "image"
    assert all(
        p.get("thread_ts") == ("0.1" if stream.native else None) for p in messages
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stream", "failure"),
    [
        (False, "timeout"),
        (False, "ratelimited"),
        (True, "timeout"),
        (True, "ratelimited"),
        (True, "missing_scope"),
        (True, "append_timeout"),
        (True, "stop_timeout"),
    ],
    indirect=["stream"],
)
async def test_stream_failure_recovery_does_not_duplicate_text(
    stream, monkeypatch, failure
):
    sleep = AsyncMock()
    monkeypatch.setattr(
        "astrbot.core.platform.sources.slack.slack_stream.asyncio.sleep", sleep
    )
    failed = False

    async def api(method, **kwargs):
        nonlocal failed
        target = (
            "chat.stopStream"
            if failure == "stop_timeout"
            else "chat.appendStream"
            if failure == "append_timeout"
            else ("chat.startStream" if stream.native else "chat.postMessage")
        )
        if failure == "stop_timeout" and method == "chat.stopStream" and failed:
            raise SlackApiError(
                "Already stopped", {"error": "message_not_in_streaming_state"}
            )
        if method == target and not failed:
            failed = True
            if "timeout" in failure:
                raise TimeoutError("Unknown delivery outcome")
            response = AsyncSlackResponse(
                client=stream.event.web_client,
                http_verb="POST",
                api_url="test",
                req_args={},
                data={"ok": False, "error": failure},
                headers={"Retry-After": "2"},
                status_code=429 if failure == "ratelimited" else 200,
            )
            raise SlackApiError("Rejected", response)
        return {"ok": True, "ts": "1.0"}

    stream.api.side_effect = api

    async def chunks():
        yield MessageChain([Plain("- Parent\n")])
        yield MessageChain([Plain("  - Child")])

    if failure == "timeout":
        with pytest.raises(TimeoutError):
            await stream.event.send_streaming(chunks())
        assert stream.api.await_count == 1
        return
    await stream.event.send_streaming(chunks())
    calls = stream.api.await_args_list
    if failure == "append_timeout":
        assert [c.args[0] for c in calls] == [
            "chat.startStream",
            "chat.appendStream",
            "chat.stopStream",
            "chat.update",
        ]
        assert calls[-1].kwargs["json"] == {
            "channel": "C123",
            "ts": "1.0",
            "markdown_text": "- Parent\n  - Child",
        }
    elif failure == "stop_timeout":
        assert [c.args[0] for c in calls] == [
            "chat.startStream",
            "chat.appendStream",
            "chat.stopStream",
            "chat.stopStream",
        ]
    elif failure == "missing_scope":
        assert [c.args[0] for c in calls] == [
            "chat.startStream",
            "chat.postMessage",
            "chat.update",
        ]
        assert calls[1].kwargs["json"]["thread_ts"] == "0.1"
    else:
        sleep.assert_awaited_once_with(2.0)
        assert calls[0] == calls[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [True], indirect=True)
@pytest.mark.parametrize(
    "failure",
    [
        "missing_scope",
        "feature_not_enabled",
        "invalid_thread_ts",
        "not_allowed",
        "timeout",
        "ratelimited",
        "auth_scope",
    ],
)
async def test_native_capability_cache_is_limited_to_client_and_feature_errors(
    stream, monkeypatch, failure
):
    monkeypatch.setattr(
        "astrbot.core.platform.sources.slack.slack_stream.asyncio.sleep", AsyncMock()
    )
    attempts = 0
    if failure == "auth_scope":
        stream.event.message_obj.raw_message.pop("team")

    async def api(method, **kwargs):
        nonlocal attempts
        if method == "auth.test" and failure == "auth_scope":
            raise SlackApiError("Auth rejected", {"error": "missing_scope"})
        if method == "chat.startStream":
            attempts += 1
            if attempts == 1 and failure != "auth_scope":
                if failure == "timeout":
                    raise TimeoutError("Unknown delivery outcome")
                raise SlackApiError("Rejected", {"error": failure})
        return {"ok": True, "ts": "1.0"}

    stream.api.side_effect = api

    async def chunks():
        yield MessageChain([Plain("- Parent\n  - Child")])

    if failure == "timeout":
        with pytest.raises(TimeoutError):
            await stream.event.send_streaming(chunks())
    else:
        await stream.event.send_streaming(chunks())
    after_first = attempts
    incoming = copy(stream.event.message_obj)
    incoming.raw_message = {"channel": "C456", "thread_ts": "0.2", "team": "T123"}
    next_event = SlackMessageEvent(
        "next", incoming, stream.event.platform_meta, "C456", stream.event.web_client
    )
    await next_event.send_streaming(chunks())
    cached = failure in {"missing_scope", "feature_not_enabled"}
    assert attempts == after_first + (0 if cached else 1)
    next_post = stream.api.await_args_list[-1 if cached else -2]
    assert next_post.kwargs["json"]["channel"] == "C456"
    assert next_post.kwargs["json"]["thread_ts"] == "0.2"

    # Recreating the adapter's client (e.g. after changing app permissions)
    # permits a new capability probe without affecting other bots.
    next_event.web_client = AsyncWebClient(token="another-bot")
    next_event.web_client.api_call = stream.api
    before_new_client = attempts
    await next_event.send_streaming(chunks())
    assert attempts == before_new_client + 1


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["error", "cancel", "aborted"])
async def test_stream_closes_on_early_termination(stream, ending):
    async def chunks():
        yield MessageChain([Plain("Partial")])
        if ending == "aborted":
            yield MessageChain([], type="aborted")
            pytest.fail("Aborted generator was advanced")
        raise (
            asyncio.CancelledError()
            if ending == "cancel"
            else RuntimeError("Generation failed")
        )

    if ending == "aborted":
        await stream.event.send_streaming(chunks())
    else:
        with pytest.raises(
            asyncio.CancelledError if ending == "cancel" else RuntimeError
        ):
            await stream.event.send_streaming(chunks())
    methods = [c.args[0] for c in stream.api.await_args_list]
    assert methods == (
        ["chat.startStream", "chat.stopStream"]
        if stream.native
        else ["chat.postMessage"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ["finish", "cancel", "send_error"])
async def test_paused_generation_flushes_without_advancing_boundaries(
    stream, monkeypatch, ending
):
    # Use the real clock: the next delta remains blocked until Slack is updated.
    monkeypatch.setattr("astrbot.core.platform.sources.slack.slack_stream.time", time)
    flushed = asyncio.Event()
    closed = asyncio.Event()
    never = asyncio.Event()
    context = ContextVar("stream_test_context", default=None)
    methods = []

    async def api(method, **kwargs):
        methods.append(method)
        if method in {"chat.update", "chat.appendStream"}:
            flushed.set()
            if ending == "send_error":
                # Editing recovery may also fail; the pending producer must stop.
                raise RuntimeError("Update rejected")
        return {"ok": True, "ts": "1.0"}

    stream.api.side_effect = api

    async def chunks():
        token = context.set("same generation context")
        try:
            yield MessageChain([Plain("- Parent\n")])
            yield MessageChain([Plain("  - Child")])
            await asyncio.wait_for(flushed.wait(), timeout=3)
            if ending != "finish":
                await never.wait()
            yield MessageChain([], type="break")
            assert methods[-1] == (
                "chat.stopStream" if stream.native else "chat.update"
            )
            yield MessageChain([Image.fromURL("https://example.com/image.png")])
        finally:
            context.reset(token)
            closed.set()

    sending = asyncio.create_task(stream.event.send_streaming(chunks()))
    try:
        await asyncio.wait_for(flushed.wait(), timeout=3)
        if ending == "cancel":
            sending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await sending
        elif ending == "send_error":
            with pytest.raises(RuntimeError, match="Update rejected"):
                await sending
        else:
            await sending
            assert methods[-1] == "chat.postMessage"
            assert stream.api.await_args.kwargs["json"]["blocks"][0]["type"] == "image"
        assert closed.is_set()
    finally:
        sending.cancel()
        await asyncio.gather(sending, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("content", ["plain", "code", "already_visible_list"])
async def test_oversized_stream_preserves_all_characters(stream, content):
    code = "    print('缩进')\n" * 3000
    text = {
        "plain": "a" * 12001 + "b" * 28000,
        "code": "```python\n" + code + "```",
        "already_visible_list": "a" * 5000 + "\n\n- Parent\n" + "  - Child\n" * 750,
    }[content]
    messages = {}
    methods = []

    async def api(method, **kwargs):
        methods.append(method)
        body = kwargs["json"]
        ts = body.get("ts", str(len(messages) + 1))
        if method in {"chat.postMessage", "chat.startStream", "chat.update"}:
            assert len(body["markdown_text"]) <= 12_000
            messages[ts] = body["markdown_text"]
        elif method == "chat.appendStream":
            messages[ts] += body["markdown_text"]
        return {"ok": True, "ts": ts}

    stream.api.side_effect = api
    split = 11990 if content == "already_visible_list" else 10

    async def chunks():
        yield MessageChain([Plain(text[:split])])
        yield MessageChain([Plain(text[split:])])

    await stream.event.send_streaming(chunks())
    calls = stream.api.await_args_list
    assert calls[0].args[0] == (
        "chat.startStream" if stream.native else "chat.postMessage"
    )
    assert len(messages) > 1
    assert all(len(part) <= 12_000 for part in messages.values())
    if content == "code":
        assert (
            "".join(
                block
                for part in messages.values()
                for block in re.findall(r"```python\n(.*?)```", part, re.S)
            )
            == code
        )
    else:
        assert "".join(messages.values()) == text
    if content == "already_visible_list":
        assert messages["2"].startswith("- Parent\n  - Child")
        if stream.native:
            assert methods[:3] == ["chat.startStream", "chat.stopStream", "chat.update"]


@pytest.mark.asyncio
async def test_empty_metadata_and_visible_reasoning(stream):
    async def chunks():
        yield MessageChain([Reply(id="0.1"), Plain(" \n")])
        yield MessageChain([Plain("Encoded audio")], type="audio_chunk")
        assert stream.api.await_count == 0
        yield MessageChain([Plain("Visible reasoning")], type="reasoning")
        yield MessageChain([Plain("Answer")])

    await stream.event.send_streaming(chunks())
    texts = [
        c.kwargs["json"]["markdown_text"]
        for c in stream.api.await_args_list
        if "markdown_text" in c.kwargs["json"]
    ]
    assert texts == ["Visible reasoning", "Answer"]


@pytest.mark.asyncio
async def test_stream_audio_uses_incoming_dm_and_thread(stream, tmp_path):
    audio = tmp_path / "speech.wav"
    with wave.open(str(audio), "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(16000)
        wav.writeframes(b"\x00\x00" * 160)
    stream.event.message_obj.type = MessageType.FRIEND_MESSAGE
    stream.event.message_obj.group_id = ""
    stream.event.message_obj.raw_message["channel"] = "D123"
    upload = AsyncMock(return_value={"ok": True})
    stream.event.web_client.files_upload_v2 = upload

    async def chunks():
        yield MessageChain([Record(file=str(audio), text="Speech")])

    await stream.event.send_streaming(chunks())
    destination = {"channel": "D123"}
    if stream.native:
        destination["thread_ts"] = "0.1"
    upload.assert_awaited_once_with(
        file=audio.read_bytes(), filename="speech.wav", **destination
    )
    stream.api.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stream", "disconnected_method"),
    [
        (False, "chat.postMessage"),
        (True, "chat.startStream"),
        (True, "chat.appendStream"),
    ],
    indirect=["stream"],
)
async def test_sdk_disconnect_does_not_replay_delivered_text(
    stream, disconnected_method
):
    messages = {}
    calls = []
    disconnected = False

    async def handle(request):
        nonlocal disconnected
        method = request.match_info["method"]
        body = await request.json()
        calls.append(method)
        ts = body.get("ts", str(len(messages) + 1))
        if method in {"chat.postMessage", "chat.startStream", "chat.update"}:
            messages[ts] = body["markdown_text"]
        elif method == "chat.appendStream":
            messages[ts] += body["markdown_text"]
        if method == disconnected_method and not disconnected:
            disconnected = True
            # Accept the mutation, then lose its response below the SDK layer.
            request.transport.close()
            return web.Response()
        return web.json_response({"ok": True, "ts": ts})

    app = web.Application()
    app.router.add_post("/{method}", handle)
    async with TestServer(app) as server:
        client = AsyncWebClient(token="test", base_url=str(server.make_url("/")))
        client.proxy = None
        handlers = client.retry_handlers[:]
        assert handlers  # Exercise the SDK's real default connection retry handler.
        stream.event.web_client = client

        async def chunks():
            yield MessageChain([Plain("- Parent\n")])
            yield MessageChain([Plain("  - Child\n")])

        if disconnected_method == "chat.appendStream":
            await stream.event.send_streaming(chunks())
            assert calls == [
                "chat.startStream",
                "chat.appendStream",
                "chat.stopStream",
                "chat.update",
            ]
            assert messages == {"1": "- Parent\n  - Child\n"}
        else:
            with pytest.raises(ServerDisconnectedError):
                await stream.event.send_streaming(chunks())
            assert calls == [disconnected_method]
            assert messages == {"1": "- Parent\n"}
        assert client.retry_handlers == handlers


@pytest.mark.asyncio
async def test_tool_direct_image_through_agent_and_respond_preserves_order(stream):
    messages = []
    closed = set()

    async def api(method, **kwargs):
        body = kwargs["json"]
        if method in {"chat.postMessage", "chat.startStream"}:
            if "blocks" in body:
                # Pending text must be committed before the direct image send.
                assert messages == ["- 准备。\n  - 子项。"]
                if stream.native:
                    assert closed == {"1"}
                assert body["blocks"][0]["type"] == "image"
            messages.append(body.get("markdown_text", "[image]"))
            ts = str(len(messages))
            assert body.get("thread_ts") == ("0.1" if stream.native else None)
        else:
            ts = body["ts"]
            if method == "chat.update":
                messages[int(ts) - 1] = body["markdown_text"]
            elif method == "chat.appendStream":
                messages[int(ts) - 1] += body["markdown_text"]
            elif method == "chat.stopStream":
                closed.add(ts)
        return {"ok": True, "ts": ts}

    stream.api.side_effect = api

    class Runner:
        streaming = True
        req = None
        run_context = SimpleNamespace(context=SimpleNamespace(event=stream.event))

        def done(self):
            return True

        async def step(self):
            for kind, chain in [
                ("streaming_delta", MessageChain([Plain("- 准备。\n")])),
                ("streaming_delta", MessageChain([Plain("  - 子项。")])),
                ("tool_call", MessageChain([], type="tool_call")),
                (
                    "tool_call_result",
                    MessageChain(
                        [Image.fromURL("https://example.com/image.png")],
                        type="tool_direct_result",
                    ),
                ),
                ("streaming_delta", MessageChain([Plain("完成。")])),
            ]:
                yield AgentResponse(type=kind, data={"chain": chain})

    stream.event.set_result(
        MessageEventResult(
            result_content_type=ResultContentType.STREAMING_RESULT,
            async_stream=run_agent(Runner(), show_tool_use=False),
        )
    )
    stage = RespondStage()
    stage.config = {
        "provider_settings": {"unsupported_streaming_strategy": "realtime_segmenting"}
    }
    await stage.process(stream.event)
    assert messages == ["- 准备。\n  - 子项。", "[image]", "完成。"]
