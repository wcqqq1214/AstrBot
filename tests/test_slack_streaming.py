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
from astrbot.api.message_components import At, Image, Json, Plain, Record, Reply
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    PlatformMetadata,
)
from astrbot.core.agent.events import MessageCompleted, TextDelta, TurnResult
from astrbot.core.agent.runtime import AgentInput, AgentPermissions, AgentSession
from astrbot.core.agent_output import present_turn
from astrbot.core.agent_output_content import content_from_chain
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
@pytest.mark.parametrize("complete_heading", [True, False])
async def test_empty_markdown_prefix_is_buffered_through_respond_stage(
    stream, complete_heading
):
    accepted = []

    async def api(method, **kwargs):
        body = kwargs["json"]
        if body.get("markdown_text") == "#":
            raise SlackApiError("Empty rendered Markdown", {"error": "no_text"})
        if "markdown_text" in body or "text" in body:
            accepted.append(body)
        return {"ok": True, "ts": "1.0"}

    stream.api.side_effect = api
    content = "# Heading\n\n- Parent\n  - Child\n\nFollowing paragraph."

    async def chunks():
        yield MessageChain([Plain("#")])
        assert stream.api.await_count == 1
        if complete_heading:
            yield MessageChain([Plain(content[1:])])

    stream.event.set_result(
        MessageEventResult(
            result_content_type=ResultContentType.STREAMING_RESULT,
            async_stream=chunks(),
        )
    )
    stage = RespondStage()
    stage.config = {}
    await stage.process(stream.event)
    destination = {"channel": "C123"}
    if stream.native:
        destination["thread_ts"] = "0.1"
    assert len(accepted) == 1
    assert accepted[0]["channel"] == "C123"
    assert accepted[0].get("thread_ts") == destination.get("thread_ts")
    if complete_heading:
        assert accepted[0]["markdown_text"] == content
    else:
        assert accepted[0] == {**destination, "text": "#", "mrkdwn": False}
    assert stream.event._stream is None


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary_error", ["no_text", "invalid_auth", "timeout"])
@pytest.mark.parametrize("already_visible", [False, True])
async def test_empty_markdown_at_size_boundary_through_respond_stage(
    stream, boundary_error, already_visible
):
    messages = {}
    attempts = []
    content = "a" * 12_000

    async def api(method, **kwargs):
        body = kwargs["json"]
        attempts.append((method, body))
        text = body.get("markdown_text", body.get("text"))
        if text == "#" or body.get("markdown_text") == "#\n\n":
            if text != "#" and boundary_error == "timeout":
                raise TimeoutError("Unknown delivery outcome")
            error = "no_text" if text == "#" else boundary_error
            raise SlackApiError("Rejected", {"error": error})
        ts = body.get("ts", str(len(messages) + 1))
        if method in {"chat.postMessage", "chat.startStream", "chat.update"}:
            messages[ts] = text
        elif method == "chat.appendStream":
            messages[ts] += text
        return {"ok": True, "ts": ts}

    stream.api.side_effect = api

    async def chunks():
        yield MessageChain([Plain("Before")])
        yield MessageChain([], type="break")
        yield MessageChain([Plain("#")])
        if already_visible:
            yield MessageChain([Plain("\n\na")])
            yield MessageChain([Plain(content[1:])])
        else:
            yield MessageChain([Plain("\n\n" + content)])

    stream.event.set_result(
        MessageEventResult(
            result_content_type=ResultContentType.STREAMING_RESULT,
            async_stream=chunks(),
        )
    )
    stage = RespondStage()
    stage.config = {}
    if boundary_error == "no_text":
        await stage.process(stream.event)
        assert list(messages.values()) == ["Before", "#\n\n", content]
        literal = [(method, body) for method, body in attempts if "text" in body]
        target = (
            {"ts": "2"}
            if already_visible
            else ({"thread_ts": "0.1"} if stream.native else {})
        )
        assert literal == [
            (
                "chat.update" if already_visible else "chat.postMessage",
                {"channel": "C123", **target, "text": "#\n\n", "mrkdwn": False},
            )
        ]
        assert next(
            method for method, body in attempts if body.get("markdown_text") == content
        ) == (
            "chat.startStream"
            if stream.native and not already_visible
            else "chat.postMessage"
        )
    else:
        with pytest.raises(
            TimeoutError if boundary_error == "timeout" else SlackApiError
        ):
            await stage.process(stream.event)
        assert list(messages.values()) == [
            "Before",
            *(["#\n\na"] if already_visible else []),
        ]
        assert not any("text" in body for _, body in attempts)
    expected = 2 if boundary_error == "no_text" else 1
    assert (
        sum(
            body.get("markdown_text", body.get("text")) == "#\n\n"
            for _, body in attempts
        )
        == expected
    )
    assert all(
        body["channel"] == "C123"
        and body.get("thread_ts") == ("0.1" if stream.native else None)
        for method, body in attempts
        if method in {"chat.postMessage", "chat.startStream"}
    )
    assert stream.event._stream is None


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
    assert stream.event._stream is None
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
        "code": "```python\n" + code + "```\n\nAfter code.",
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
        # Continue generating after multiple message-size boundaries.
        size = 4096 if content == "code" else len(text)
        for start in range(split, len(text), size):
            yield MessageChain([Plain(text[start : start + size])])

    await stream.event.send_streaming(chunks())
    calls = stream.api.await_args_list
    assert calls[0].args[0] == (
        "chat.startStream" if stream.native else "chat.postMessage"
    )
    assert len(messages) > 1
    assert all(len(part) <= 12_000 for part in messages.values())
    if content == "code":
        assert all(part.count("```") == 2 for part in messages.values())
        assert list(messages.values())[-1].endswith("```\n\nAfter code.")
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
@pytest.mark.parametrize("delivery", ["yield-result", "event-send", "model-image"])
async def test_tool_direct_image_through_agent_and_respond_preserves_order(
    stream, delivery
):
    direct_send = delivery == "event-send"
    messages = []
    closed = set()

    async def api(method, **kwargs):
        body = kwargs["json"]
        if method in {"chat.postMessage", "chat.startStream"}:
            if "blocks" in body:
                # Pending text must be committed before the direct image send.
                assert messages == ["- 准备。\n  - 子项。"] or (
                    direct_send and messages == ["- 准备。\n  - 子项。", "[image]"]
                )
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

    class ImageSession(AgentSession):
        async def execute_turn(self, turn):
            turn.emit(TextDelta(message_id="before", part_index=0, text="- 准备。\n"))
            turn.emit(TextDelta(message_id="before", part_index=0, text="  - 子项。"))
            chain = MessageChain(
                [Image.fromURL("https://example.com/image.png")],
                type=None if direct_send else "tool_direct_result",
            )
            if delivery == "model-image":
                turn.emit(
                    MessageCompleted(
                        message_id="image", content=content_from_chain(chain)
                    )
                )
            else:
                # The new tool executor sends yielded results through event.send too.
                await stream.event.send(chain)
                if direct_send:
                    await stream.event.send(chain)
            turn.emit(TextDelta(message_id="after", part_index=0, text="完成。"))
            return TurnResult()

        async def history(self):
            return []

    session = ImageSession("images", stream.event.unified_msg_origin)
    turn = await session.run_turn(AgentInput(stream.event, AgentPermissions()))
    stream.event.bind_agent_turn(turn)
    stream.event.set_result(
        MessageEventResult(
            result_content_type=ResultContentType.STREAMING_RESULT,
            async_stream=present_turn(turn, streaming=True, show_tool_use=False),
        )
    )
    stage = RespondStage()
    stage.config = {
        "provider_settings": {"unsupported_streaming_strategy": "realtime_segmenting"}
    }
    try:
        await asyncio.wait_for(stage.process(stream.event), timeout=3)
    finally:
        await session.close()
        stream.event.bind_agent_turn(None)
    assert (await turn.result()).status == "completed"
    assert messages == [
        "- 准备。\n  - 子项。",
        *(["[image]"] * (2 if direct_send else 1)),
        "完成。",
    ]
    assert stream.event._stream is None


@pytest.mark.asyncio
async def test_direct_send_waits_for_inflight_timed_flush(stream, monkeypatch):
    monkeypatch.setattr("astrbot.core.platform.sources.slack.slack_stream.time", time)
    updating = asyncio.Event()
    release = asyncio.Event()
    direct_send = asyncio.Event()
    methods = []

    async def api(method, **kwargs):
        methods.append(method)
        if method in {"chat.update", "chat.appendStream"}:
            updating.set()
            await release.wait()
        return {"ok": True, "ts": str(len(methods))}

    stream.api.side_effect = api

    async def chunks():
        yield MessageChain([Plain("Before")])
        yield MessageChain([Plain(" tool")])
        await updating.wait()
        direct_send.set()
        await stream.event.send(
            MessageChain([Image.fromURL("https://example.com/image.png")])
        )
        yield MessageChain([Plain("After")])

    sending = asyncio.create_task(stream.event.send_streaming(chunks()))
    try:
        await asyncio.wait_for(direct_send.wait(), timeout=3)
        # The producer has entered send() while the timer's request is still pending.
        assert len(methods) == 2
        release.set()
        await asyncio.wait_for(sending, timeout=3)
        assert methods == (
            [
                "chat.startStream",
                "chat.appendStream",
                "chat.stopStream",
                "chat.postMessage",
                "chat.startStream",
                "chat.stopStream",
            ]
            if stream.native
            else [
                "chat.postMessage",
                "chat.update",
                "chat.postMessage",
                "chat.postMessage",
            ]
        )
    finally:
        sending.cancel()
        await asyncio.gather(sending, return_exceptions=True)
    assert stream.event._stream is None


@pytest.mark.asyncio
async def test_failed_direct_send_boundary_stops_even_if_tool_catches_error(stream):
    failure = RuntimeError("Flush response lost")

    async def api(method, **kwargs):
        if method in {"chat.update", "chat.appendStream"}:
            raise failure
        return {"ok": True, "ts": "1"}

    stream.api.side_effect = api

    async def chunks():
        yield MessageChain([Plain("Before")])
        yield MessageChain([Plain(" tool")])
        with pytest.raises(RuntimeError, match="Flush response lost"):
            await stream.event.send(
                MessageChain([Image.fromURL("https://example.com/image.png")])
            )
        yield MessageChain([Plain("After")])

    with pytest.raises(RuntimeError, match="Flush response lost"):
        await stream.event.send_streaming(chunks())
    calls = stream.api.await_args_list
    assert not any("blocks" in call.kwargs["json"] for call in calls)
    assert sum(call.args[0] == "chat.update" for call in calls) == 1
    assert stream.event._stream is None


@pytest.mark.asyncio
async def test_tool_timeout_during_flush_does_not_replay_accepted_text(stream):
    messages = {}
    accepted = False
    generator_closed = asyncio.Event()

    async def api(method, **kwargs):
        nonlocal accepted
        body = kwargs["json"]
        ts = body.get("ts", str(len(messages) + 1))
        if method in {"chat.startStream", "chat.postMessage", "chat.update"}:
            messages[ts] = body.get("markdown_text", "[image]")
        elif method == "chat.appendStream":
            messages[ts] += body["markdown_text"]
        if method in {"chat.update", "chat.appendStream"} and not accepted:
            accepted = True
            # Slack accepted the write, but the tool times out before its response.
            await asyncio.Event().wait()
        return {"ok": True, "ts": ts}

    stream.api.side_effect = api

    async def chunks():
        try:
            yield MessageChain([Plain("Before")])
            yield MessageChain([Plain(" tool")])
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    stream.event.send(
                        MessageChain([Image.fromURL("https://example.com/image.png")])
                    ),
                    timeout=0.02,
                )
            # The tool executor reports timeout and permits the model to continue.
            yield MessageChain([Plain(" After")])
        finally:
            generator_closed.set()

    with pytest.raises(asyncio.CancelledError):
        await stream.event.send_streaming(chunks())
    assert messages == {"1": "Before tool"}
    assert [call.args[0] for call in stream.api.await_args_list] == (
        ["chat.startStream", "chat.appendStream", "chat.stopStream"]
        if stream.native
        else ["chat.postMessage", "chat.update"]
    )
    assert generator_closed.is_set()
    assert stream.event._stream is None


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [True], indirect=True)
@pytest.mark.parametrize(
    "outcome",
    [
        "success",
        "exception",
        "mcp_error",
        "timeout",
        "missing",
        "off",
        "hidden",
        "tts",
        "direct_success",
        "direct_rejected",
        "direct_lost",
    ],
)
async def test_task_cards_through_real_tool_runner_and_respond(stream, outcome):
    from mcp.types import CallToolResult, TextContent

    from astrbot.core.agent.hooks import BaseAgentRunHooks
    from astrbot.core.agent.run_context import ContextWrapper
    from astrbot.core.agent.runners.tool_loop_agent_runner import ToolLoopAgentRunner
    from astrbot.core.agent.tool import FunctionTool, ToolSet
    from astrbot.core.astr_agent_runtime.tool_executor import FunctionToolExecutor
    from astrbot.core.provider.entities import LLMResponse, ProviderRequest

    stream.event.native_task_cards = outcome != "off"
    enabled = outcome not in {"off", "hidden", "tts"}
    requests = []

    async def api(method, **kwargs):
        body = kwargs["json"]
        requests.append((method, body))
        if "blocks" in body:
            if outcome == "direct_rejected":
                raise SlackApiError("image rejected", {"error": "channel_not_found"})
            if outcome == "direct_lost":
                raise TimeoutError("image accepted but response lost")
        return {"ok": True, "ts": body.get("ts", str(len(requests)))}

    stream.api.side_effect = api

    async def handler(event):
        if outcome == "exception":
            raise ValueError("controlled tool failure")
        if outcome == "timeout":
            await asyncio.Event().wait()
        if outcome == "mcp_error":
            return CallToolResult(
                isError=True, content=[TextContent(type="text", text="failed")]
            )
        if outcome.startswith("direct_"):
            event.set_result(
                MessageEventResult(
                    chain=[Image.fromURL("https://example.com/tool.png")]
                )
            )
            return None
        await event.send(MessageChain([Image.fromURL("https://example.com/tool.png")]))
        return "success"

    calls = 0

    async def model(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            yield LLMResponse(role="assistant", completion_text="Before", is_chunk=True)
            yield LLMResponse(
                role="assistant",
                completion_text="Before",
                tools_call_name=["missing" if outcome == "missing" else "probe"],
                tools_call_args=[{}],
                tools_call_ids=["call-probe"],
            )
        else:
            yield LLMResponse(role="assistant", completion_text="Final", is_chunk=True)
            yield LLMResponse(role="assistant", completion_text="Final")

    context = ContextWrapper(context=SimpleNamespace(event=stream.event))
    context.tool_call_timeout = 0.02 if outcome == "timeout" else 5
    runner = ToolLoopAgentRunner()
    await runner.reset(
        provider=SimpleNamespace(provider_config={}, text_chat_stream=model, get_model=lambda: "offline"),
        request=ProviderRequest(
            prompt="probe",
            func_tool=ToolSet(
                tools=[
                    FunctionTool(
                        name="probe",
                        description="probe",
                        parameters={"type": "object", "properties": {}},
                        handler=handler,
                    )
                ]
            ),
        ),
        run_context=context,
        tool_executor=FunctionToolExecutor(),
        agent_hooks=BaseAgentRunHooks(),
        streaming=True,
    )
    from astrbot.core.astr_agent_runtime.execution import execute_native_loop

    class ToolSession(AgentSession):
        async def execute_turn(self, turn):
            return await execute_native_loop(runner, turn, 4)

        async def history(self):
            return []

    if outcome == "tts":
        stream.event.set_extra("action_type", "live")
    session = ToolSession("tools", stream.event.unified_msg_origin)
    turn = await session.run_turn(AgentInput(stream.event, AgentPermissions()))
    stream.event.bind_agent_turn(turn)
    try:
        stream.event.set_result(
            MessageEventResult(
                result_content_type=ResultContentType.STREAMING_RESULT,
                async_stream=present_turn(
                    turn, streaming=True, show_tool_use=outcome != "hidden"
                ),
            )
        )
        stage = RespondStage()
        stage.config = {}
        await stage.process(stream.event)
    finally:
        await session.close()
        stream.event.bind_agent_turn(None)
    starts = [
        body for method, body in requests if body.get("task_display_mode") == "plan"
    ]
    assert len(starts) == int(enabled)
    if enabled:
        updates = [
            chunk
            for _, body in requests
            for chunk in body.get("chunks", [])
            if chunk["type"] == "task_update"
        ]
        assert [chunk["status"] for chunk in updates] == [
            "in_progress",
            "complete" if outcome in {"success", "direct_success"} else "error",
        ]
        assert len({chunk["id"] for chunk in updates}) == 1
    if outcome.startswith("direct_"):
        assert calls == 1  # Direct delivery still ends the loop, even on failure.
        assert sum("blocks" in body for _, body in requests) == 1
        results = [m for m in runner.run_context.messages if m.role == "tool"]
        assert len(results) == 1
        if outcome != "direct_success":
            assert "Direct tool delivery failed or is unconfirmed" in str(
                results[0].content
            )
        assert not any(body.get("markdown_text") == "Final" for _, body in requests)
    else:
        assert any(body.get("markdown_text") == "Final" for _, body in requests)
    if outcome in {"success", "off", "hidden", "tts"}:
        media = next(i for i, (_, body) in enumerate(requests) if "blocks" in body)
        final = next(
            i
            for i, (_, body) in enumerate(requests)
            if body.get("markdown_text") == "Final"
        )
        assert media < final
    assert stream.event._stream is None


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [True], indirect=True)
@pytest.mark.parametrize(
    "failure", ["rejected", "start_lost", "append_lost", "stop_lost", "rate_limit"]
)
async def test_task_card_failure_does_not_suppress_answer(stream, monkeypatch, failure):
    stream.event.native_task_cards = True
    requests = []
    failed = False
    monkeypatch.setattr(
        "astrbot.core.platform.sources.slack.slack_stream.asyncio.sleep", AsyncMock()
    )

    async def api(method, **kwargs):
        nonlocal failed
        body = kwargs["json"]
        requests.append((method, body))
        targeted = (
            (
                body.get("task_display_mode")
                and failure in {"rejected", "start_lost", "rate_limit"}
            )
            or (
                "chunks" in body
                and method == "chat.appendStream"
                and failure == "append_lost"
            )
            or (
                method == "chat.stopStream"
                and body["ts"] == "1"
                and failure == "stop_lost"
            )
        )
        if targeted and not failed:
            failed = True
            if failure in {"rejected", "rate_limit"}:
                raise SlackApiError(
                    "controlled failure",
                    {
                        "error": "invalid_arguments"
                        if failure == "rejected"
                        else "ratelimited"
                    },
                )
            raise TimeoutError("accepted but response lost")
        return {"ok": True, "ts": body.get("ts", str(len(requests)))}

    stream.api.side_effect = api

    async def chunks():
        yield MessageChain(
            [Json(data={"id": "tool-1", "name": "probe"})], type="tool_call"
        )
        yield MessageChain(
            [Json(data={"id": "tool-1", "is_error": False})], type="tool_call_result"
        )
        yield MessageChain([Plain("Final")])

    await stream.event.send_streaming(chunks())
    starts = [body for _, body in requests if body.get("task_display_mode")]
    assert len(starts) == (2 if failure == "rate_limit" else 1)
    assert any(body.get("markdown_text") == "Final" for _, body in requests)
    if failure == "start_lost":
        assert not any(
            body.get("text", "").startswith("AstrBot 工具进度") for _, body in requests
        )
    if failure == "rejected":
        assert any(
            body.get("text") == "AstrBot 工具进度\n• probe：进行中"
            for _, body in requests
        )
    if failure == "append_lost":
        updates = [
            chunk
            for _, body in requests
            for chunk in body.get("chunks", [])
            if chunk["type"] == "task_update"
        ]
        assert [chunk["status"] for chunk in updates] == [
            "in_progress",
            "complete",
            "error",
        ]
        assert updates[-1]["title"].endswith("（状态未确认）")
    assert stream.event._stream is None


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [True], indirect=True)
@pytest.mark.parametrize(
    "renewal",
    ["success", "invalid_arguments", "message_not_in_streaming_state", "lost"],
)
async def test_expired_task_card_renews_once_through_respond(stream, renewal):
    stream.event.native_task_cards = True
    requests = []
    starts = []

    async def api(method, **kwargs):
        body = kwargs["json"]
        requests.append((method, body))
        if body.get("task_display_mode") == "plan":
            starts.append(body)
            if len(starts) == 2:
                if renewal == "lost":
                    raise TimeoutError("renewed card accepted but response lost")
                if renewal != "success":
                    raise SlackApiError("renewal rejected", {"error": renewal})
            return {"ok": True, "ts": f"card-{len(starts)}"}
        if (
            method == "chat.appendStream"
            and body["ts"] == "card-1"
            and body["chunks"][0]["status"] == "error"
        ):
            raise SlackApiError(
                "card expired", {"error": "message_not_in_streaming_state"}
            )
        return {"ok": True, "ts": body.get("ts", "answer")}

    stream.api.side_effect = api

    async def chunks():
        for task_id in ("first", "second", "third"):
            yield MessageChain(
                [Json(data={"id": task_id, "name": task_id})], type="tool_call"
            )
            yield MessageChain(
                [Json(data={"id": task_id, "is_error": task_id == "second"})],
                type="tool_call_result",
            )
        yield MessageChain([Plain("Final")])

    stream.event.set_result(
        MessageEventResult(
            result_content_type=ResultContentType.STREAMING_RESULT,
            async_stream=chunks(),
        )
    )
    stage = RespondStage()
    stage.config = {}
    await stage.process(stream.event)
    assert len(starts) == 2
    assert all(
        body["channel"] == "C123" and body["thread_ts"] == "0.1" for body in starts
    )
    assert [(task["id"], task["status"]) for task in starts[1]["chunks"][1:]] == [
        ("first", "complete"),
        ("second", "error"),
    ]
    assert any(body.get("markdown_text") == "Final" for _, body in requests)
    fallback = [
        body["text"]
        for _, body in requests
        if body.get("text", "").startswith("AstrBot 工具进度")
    ]
    assert fallback == (
        [
            "AstrBot 工具进度\n• first：已完成\n• second：失败",
            "AstrBot 工具进度\n• first：已完成\n• second：失败\n• third：进行中",
            "AstrBot 工具进度\n• first：已完成\n• second：失败\n• third：已完成",
        ]
        if renewal not in {"success", "lost"}
        else []
    )
    updates = [
        body["chunks"][0]["status"]
        for method, body in requests
        if method == "chat.appendStream" and body["ts"] == "card-2"
    ]
    assert updates == (["in_progress", "complete"] if renewal == "success" else [])
    stopped_cards = [
        body["ts"]
        for method, body in requests
        if method == "chat.stopStream" and body["ts"].startswith("card-")
    ]
    assert stopped_cards == (["card-2"] if renewal == "success" else [])
    assert stream.event._stream is None


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [True], indirect=True)
@pytest.mark.parametrize(
    "failure", [None, "post_lost", "update_lost", "update_rejected"]
)
async def test_task_card_text_fallback_edits_one_message_through_respond(
    stream, failure
):
    stream.event.native_task_cards = True
    requests = []
    visible_text = {}

    async def api(method, **kwargs):
        body = kwargs["json"]
        requests.append((method, body))
        if body.get("task_display_mode") == "plan":
            raise SlackApiError("cards unavailable", {"error": "invalid_arguments"})
        if body.get("text", "").startswith("AstrBot 工具进度"):
            assert body["channel"] == "C123" and body["parse"] == "full"
            if method == "chat.postMessage":
                assert body["thread_ts"] == "0.1"
            else:
                assert method == "chat.update" and body["ts"] == "progress"
                assert "thread_ts" not in body
                if failure == "update_rejected":
                    raise SlackApiError(
                        "message deleted", {"error": "message_not_found"}
                    )
            visible_text["progress"] = body["text"]
            if (method == "chat.postMessage" and failure == "post_lost") or (
                method == "chat.update" and failure == "update_lost"
            ):
                raise TimeoutError("write accepted but response lost")
            return {"ok": True, "ts": "progress"}
        return {"ok": True, "ts": body.get("ts", "answer")}

    stream.api.side_effect = api

    async def chunks():
        for task_id in ("first", "second"):
            yield MessageChain(
                [Json(data={"id": task_id, "name": "same_tool"})], type="tool_call"
            )
            yield MessageChain(
                [Json(data={"id": task_id, "is_error": task_id == "second"})],
                type="tool_call_result",
            )
        yield MessageChain([Plain("Final")])

    stream.event.set_result(
        MessageEventResult(
            result_content_type=ResultContentType.STREAMING_RESULT,
            async_stream=chunks(),
        )
    )
    stage = RespondStage()
    stage.config = {}
    await stage.process(stream.event)
    progress = [
        (method, body["text"])
        for method, body in requests
        if body.get("text", "").startswith("AstrBot 工具进度")
    ]
    expected = [
        ("chat.postMessage", "AstrBot 工具进度\n• same_tool：进行中"),
        ("chat.update", "AstrBot 工具进度\n• same_tool：已完成"),
        ("chat.update", "AstrBot 工具进度\n• same_tool：已完成\n• same_tool：进行中"),
        ("chat.update", "AstrBot 工具进度\n• same_tool：已完成\n• same_tool：失败"),
    ]
    assert progress == expected[: 1 if failure == "post_lost" else 2 if failure else 4]
    assert visible_text == {
        "progress": expected[
            0 if failure in {"post_lost", "update_rejected"} else 1 if failure else 3
        ][1]
    }
    assert sum(bool(body.get("task_display_mode")) for _, body in requests) == 1
    assert any(body.get("markdown_text") == "Final" for _, body in requests)
    assert stream.event._stream is None


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [True], indirect=True)
@pytest.mark.parametrize("ending", ["cancel", "error", "aborted", "rejected", "lost"])
async def test_text_progress_closes_pending_tasks_through_respond(stream, ending):
    stream.event.native_task_cards = True
    started = asyncio.Event()
    progress = []

    async def api(method, **kwargs):
        body = kwargs["json"]
        if body.get("task_display_mode") == "plan":
            raise SlackApiError("cards unavailable", {"error": "invalid_arguments"})
        if body.get("text", "").startswith("AstrBot 工具进度"):
            progress.append((method, body))
            if "状态未确认" in body["text"]:
                if ending == "rejected":
                    raise SlackApiError(
                        "message deleted", {"error": "message_not_found"}
                    )
                if ending == "lost":
                    raise TimeoutError("write accepted but response lost")
            return {"ok": True, "ts": "progress"}
        return {"ok": True, "ts": body.get("ts", "answer")}

    stream.api.side_effect = api

    async def chunks():
        for task_id in ("done", "failed", "pending"):
            yield MessageChain(
                [Json(data={"id": task_id, "name": task_id})], type="tool_call"
            )
            if task_id != "pending":
                yield MessageChain(
                    [Json(data={"id": task_id, "is_error": task_id == "failed"})],
                    type="tool_call_result",
                )
        started.set()
        if ending == "cancel":
            await asyncio.Event().wait()
        elif ending == "error":
            raise RuntimeError("generation failed")
        elif ending == "aborted":
            yield MessageChain([], type="aborted")
        else:
            yield MessageChain([Plain("Final")])

    stream.event.set_result(
        MessageEventResult(
            result_content_type=ResultContentType.STREAMING_RESULT,
            async_stream=chunks(),
        )
    )
    stage = RespondStage()
    stage.config = {}
    sending = asyncio.create_task(stage.process(stream.event))
    try:
        if ending == "cancel":
            await asyncio.wait_for(started.wait(), timeout=2)
            sending.cancel()
        if ending in {"cancel", "error"}:
            with pytest.raises(
                asyncio.CancelledError if ending == "cancel" else RuntimeError
            ):
                await sending
        else:
            await sending
    finally:
        sending.cancel()
        await asyncio.gather(sending, return_exceptions=True)

    assert [method for method, _ in progress] == ["chat.postMessage"] + [
        "chat.update"
    ] * 5
    assert progress[-1][1] == {
        "channel": "C123",
        "ts": "progress",
        "text": "AstrBot 工具进度\n• done：已完成\n• failed：失败\n• pending：状态未确认",
        "parse": "full",
    }
    if ending in {"rejected", "lost"}:
        assert any(
            call.kwargs["json"].get("markdown_text") == "Final"
            for call in stream.api.await_args_list
        )
    assert stream.event._stream is None


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [True], indirect=True)
async def test_cancelled_task_card_is_not_marked_complete(stream):
    stream.event.native_task_cards = True

    async def chunks():
        yield MessageChain(
            [Json(data={"id": "tool-1", "name": "probe"})], type="tool_call"
        )
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await stream.event.send_streaming(chunks())
    updates = [
        chunk
        for call in stream.api.await_args_list
        for chunk in call.kwargs["json"].get("chunks", [])
        if chunk["type"] == "task_update"
    ]
    assert [chunk["status"] for chunk in updates] == ["in_progress", "error"]
    assert stream.api.await_args_list[-1].args[0] == "chat.stopStream"
    assert stream.event._stream is None


@pytest.mark.parametrize("enabled", [False, True])
def test_task_cards_require_opt_in_and_existing_thread(stream, enabled):
    from astrbot.core.platform.sources.slack.slack_adapter import SlackAdapter

    adapter = SlackAdapter(
        {
            "id": "slack",
            "bot_token": "test",
            "app_token": "test",
            "slack_native_task_cards": enabled,
        },
        {},
        asyncio.Queue(),
    )
    stream.event.message_obj.message_str = "probe"
    stream.event.message_obj.session_id = "C123"
    event = adapter.create_event(stream.event.message_obj)
    assert event.native_task_cards is (enabled and stream.native)
