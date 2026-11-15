"""Exercise image tools through the shared dispatcher and Slack reply path."""

import asyncio
from contextlib import nullcontext
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

from astrbot.api.event import MessageChain
from astrbot.api.message_components import Plain
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    PlatformMetadata,
)
from astrbot.core.agent.events import TextDelta, ToolFinished, ToolStarted, TurnResult
from astrbot.core.agent.runtime import AgentInput, AgentPermissions, AgentSession
from astrbot.core.astr_agent_runtime.tools.message_tools import SendMessageToUserTool
from astrbot.core.config.default import DEFAULT_CONFIG
from astrbot.core.pipeline.process_stage.method.agent_sub_stages import internal
from astrbot.core.pipeline.process_stage.stage import ProcessStage
from astrbot.core.pipeline.respond.stage import RespondStage
from astrbot.core.pipeline.scheduler import PipelineScheduler
from astrbot.core.platform.agent_wakeup_event import AgentWakeupEvent
from astrbot.core.platform.astr_message_event import (
    current_message_event,
    event_context,
)
from astrbot.core.platform.sources.slack.slack_adapter import SlackAdapter
from astrbot.core.platform.sources.slack.slack_event import SlackMessageEvent
from astrbot.core.star.context import Context


@pytest.fixture
def dispatch(monkeypatch, tmp_path):
    client = AsyncWebClient(token="xoxb-test")
    client.api_call = AsyncMock(return_value={"ok": True, "ts": "1.0"})
    client.files_upload_v2 = AsyncMock(
        return_value={
            "ok": True,
            "files": [{"url_private": "https://example.com/image.png"}],
        }
    )
    adapter = object.__new__(SlackAdapter)
    adapter.web_client = client
    adapter.metadata = PlatformMetadata(name="slack", id="slack", description="Slack")
    message = AstrBotMessage()
    message.type = MessageType.GROUP_MESSAGE
    message.group_id = "C123"
    message.sender = MessageMember(user_id="U123", nickname="Tester")
    message.raw_message = {"channel": "C123", "team": "T123", "thread_ts": "0.1"}
    event = SlackMessageEvent("test", message, adapter.metadata, "C123", client)
    event.role = "admin"
    monkeypatch.setattr(event.trace, "record", Mock())
    context = Context.__new__(Context)
    context.platform_manager = SimpleNamespace(platform_insts=[adapter])
    context.get_config = lambda umo: {
        "provider_settings": {"computer_use_runtime": "local"},
        "provider_ltm_settings": {"group_message_history_enable": True},
    }
    history = AsyncMock()
    context.message_history_manager = SimpleNamespace(insert_message_chain=history)
    metrics = AsyncMock()
    monkeypatch.setattr("astrbot.core.utils.metrics.Metric.upload", metrics)
    monkeypatch.setattr(
        "astrbot.core.astr_agent_runtime.tools.message_tools.workspace_root_for_context",
        AsyncMock(return_value=tmp_path),
    )
    monkeypatch.setattr(
        "astrbot.core.platform.sources.slack.slack_stream.time",
        SimpleNamespace(monotonic=lambda: 0.0),
    )
    image = tmp_path / "image.png"
    image.write_bytes(b"image fixture; upload is mocked")
    return SimpleNamespace(
        event=event,
        context=context,
        client=client,
        history=history,
        metrics=metrics,
        tool=SendMessageToUserTool(),
        wrapper=SimpleNamespace(
            context=SimpleNamespace(
                event=event, context=context, _output_turn=None, send_to_user=True
            )
        ),
        messages=[
            {"type": "plain", "text": "Before image"},
            {"type": "image", "path": str(image)},
            {"type": "plain", "text": "After image"},
        ],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("session", [None, "", "slack:GroupMessage:C123"])
@pytest.mark.parametrize("streaming", [False, True], ids=["static", "streaming"])
async def test_current_thread_image_tool_preserves_order(dispatch, session, streaming):
    d = dispatch
    kwargs = {} if session is None else {"session": session}

    async def chunks():
        if streaming:
            yield MessageChain([Plain("Opening")])
            yield MessageChain([Plain(" buffered tail")])
        result = await d.tool.call(d.wrapper, messages=d.messages, **kwargs)
        assert result == "Message sent to session slack:GroupMessage:C123"
        if streaming:
            yield MessageChain([Plain("Final")])

    with event_context(d.event):
        if streaming:
            await asyncio.wait_for(d.event.send_streaming(chunks()), timeout=3)
        else:
            async for _ in chunks():
                pass
    calls = d.client.api_call.await_args_list
    assert [c.args[0] for c in calls] == (
        [
            "chat.startStream",
            "chat.appendStream",
            "chat.stopStream",
            "chat.postMessage",
            "chat.startStream",
            "chat.stopStream",
        ]
        if streaming
        else ["chat.postMessage"]
    )
    bodies = [c.kwargs["json"] for c in calls]
    if streaming:
        assert bodies[1]["markdown_text"] == " buffered tail"
        assert bodies[-2]["markdown_text"] == "Final"
    for call, body in zip(calls, bodies):
        assert body["channel"] == "C123"
        if call.args[0] in {"chat.postMessage", "chat.startStream"}:
            assert body["thread_ts"] == "0.1"
    media = next(b for b in bodies if "blocks" in b)
    assert [b["type"] for b in media["blocks"]] == ["markdown", "image", "markdown"]
    assert [b["text"] for b in media["blocks"] if b["type"] == "markdown"] == [
        "Before image",
        "After image",
    ]
    d.client.files_upload_v2.assert_awaited_once()
    d.history.assert_awaited_once()
    if not streaming:
        assert (
            len(
                [
                    c
                    for c in d.event.trace.record.call_args_list
                    if c.args[0] == "response_sent"
                ]
            )
            == 1
        )
        await asyncio.sleep(0)
        d.metrics.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["other-session", "cron", "inactive", "unbound", "channel"]
)
async def test_image_tool_keeps_proactive_routing(dispatch, mode, monkeypatch):
    d = dispatch
    adapter = d.context.platform_manager.platform_insts[0]
    proactive_send = AsyncMock(wraps=adapter.send_by_session)
    monkeypatch.setattr(adapter, "send_by_session", proactive_send)
    target = "slack:GroupMessage:C456" if mode == "other-session" else ""
    if mode == "cron":
        d.event = AgentWakeupEvent(
            context=d.context,
            session=d.event.session,
            platform_meta=d.event.platform_meta,
            prompt="Scheduled",
            source="cron",
        )
        d.event.role = "admin"
        d.wrapper.context.event = d.event
    elif mode == "channel":
        d.event.message_obj.raw_message.pop("thread_ts")
    # A background task may inherit the context variable after its event has ended.
    token = current_message_event.set(d.event) if mode == "inactive" else None
    try:
        with (
            nullcontext() if mode in {"inactive", "unbound"} else event_context(d.event)
        ):
            result = await d.tool.call(d.wrapper, messages=d.messages, session=target)
    finally:
        if token is not None:
            current_message_event.reset(token)
    assert result.startswith("Message sent to session ")
    d.client.api_call.assert_awaited_once()
    body = d.client.api_call.await_args.kwargs["json"]
    assert body["channel"] == ("C456" if mode == "other-session" else "C123")
    assert "thread_ts" not in body
    proactive_send.assert_awaited_once()
    d.history.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "show_tool_use"),
    [(None, False), (None, True), ("rejected", False), ("cancelled", False)],
)
async def test_channel_image_tool_seals_runtime_output(
    dispatch, monkeypatch, failure, show_tool_use
):
    d = dispatch
    d.event.message_obj.raw_message.pop("thread_ts")
    monkeypatch.setattr(
        "astrbot.core.pipeline.respond.stage.call_event_hook",
        AsyncMock(return_value=False),
    )
    messages = []
    posting = asyncio.Event()

    async def api(method, **kwargs):
        body = kwargs["json"]
        if failure is not None:
            posting.set()
            if failure == "cancelled":
                await asyncio.Event().wait()
            raise SlackApiError("Rejected", {"ok": False, "error": "invalid_auth"})
        if method == "chat.postMessage":
            messages.append(body.get("markdown_text", "[image]"))
            ts = str(len(messages))
        else:
            ts = body["ts"]
            messages[int(ts) - 1] = body["markdown_text"]
        return {"ok": True, "ts": ts}

    d.client.api_call.side_effect = api

    class ImageSession(AgentSession):
        async def execute_turn(self, turn):
            turn.emit(TextDelta(message_id="before", part_index=0, text="Opening。"))
            turn.emit(
                TextDelta(message_id="before", part_index=0, text=" buffered tail。")
            )
            turn.emit(
                ToolStarted(
                    execution_id="tool",
                    call_id="tool",
                    name="send_message_to_user",
                    arguments={},
                )
            )
            d.wrapper.context._output_turn = turn
            # No consumer handshake: tools may run ahead of Slack presentation.
            result = await d.tool.call(d.wrapper, messages=[d.messages[1]])
            assert result == "Message sent to session slack:GroupMessage:C123"
            turn.emit(
                ToolFinished(execution_id="tool", call_id="tool", status="completed")
            )
            turn.emit(TextDelta(message_id="after", part_index=0, text="Final。"))
            return TurnResult()

        async def history(self):
            return []

    session = ImageSession("images", d.event.unified_msg_origin)
    config = deepcopy(DEFAULT_CONFIG)
    config["provider_settings"].update(
        streaming_response=True,
        show_tool_use_status=show_tool_use,
        computer_use_runtime="local",
    )
    config["provider_ltm_settings"]["group_message_history_enable"] = True
    d.context.get_config = lambda umo: config
    runtime = SimpleNamespace(
        active_turn=lambda _: session.active_turn,
        open_session=AsyncMock(return_value=session),
    )
    pipeline_context = SimpleNamespace(
        astrbot_config=config,
        plugin_manager=SimpleNamespace(context=d.context),
        agent_runtimes={"astrbot": runtime},
    )
    monkeypatch.setattr(
        internal,
        "prepare_agent_input",
        AsyncMock(
            return_value=AgentInput(d.event, AgentPermissions.from_event(d.event))
        ),
    )
    monkeypatch.setattr(internal, "admit_agent_input", AsyncMock(return_value=True))
    d.event.is_at_or_wake_command = True
    process, respond = ProcessStage(), RespondStage()
    await process.initialize(pipeline_context)
    await respond.initialize(pipeline_context)
    scheduler = object.__new__(PipelineScheduler)
    scheduler.stages = [process, respond]
    with event_context(d.event):
        try:
            task = asyncio.create_task(scheduler._process_stages(d.event))
            if failure is None:
                await asyncio.wait_for(task, timeout=3)
            else:
                await asyncio.wait_for(posting.wait(), timeout=3)
                if failure == "cancelled":
                    task.cancel()
                with pytest.raises(
                    asyncio.CancelledError if failure == "cancelled" else SlackApiError
                ):
                    await asyncio.wait_for(task, timeout=3)
        finally:
            await session.close()
    assert d.event._agent_turn is None
    assert session.active_turn is None

    assert d.event._stream is None
    if failure is not None:
        d.client.api_call.assert_awaited_once()
        d.client.files_upload_v2.assert_not_awaited()
        d.history.assert_not_awaited()
        assert (await d.wrapper.context._output_turn.result()).status == "cancelled"
        return
    calls = d.client.api_call.await_args_list
    assert [call.args[0] for call in calls] == [
        "chat.postMessage",
        "chat.update",
        *(["chat.postMessage"] if show_tool_use else []),
        "chat.postMessage",
        "chat.postMessage",
    ]
    assert messages == [
        "Opening。 buffered tail。",
        *(["🔨 调用工具: send_message_to_user"] if show_tool_use else []),
        "[image]",
        "Final。",
    ]
    assert calls[1].kwargs["json"]["ts"] == "1"
    assert calls[-2].kwargs["json"]["blocks"][0]["type"] == "image"
    assert all(
        call.kwargs["json"]["channel"] == "C123"
        and "thread_ts" not in call.kwargs["json"]
        for call in calls
    )
    d.client.files_upload_v2.assert_awaited_once()
    d.history.assert_awaited_once()
    assert d.event._stream is None


@pytest.mark.asyncio
async def test_thread_image_delivery_loss_does_not_fall_back_to_channel(dispatch):
    d = dispatch
    d.client.api_call.side_effect = TimeoutError("Delivery unknown")
    with event_context(d.event):
        result = await d.tool.call(d.wrapper, messages=d.messages)
    assert result.isError is True
    assert result.content[0].text.startswith("error: failed to send message")
    d.client.api_call.assert_awaited_once()
    assert d.client.api_call.await_args.kwargs["json"]["thread_ts"] == "0.1"
    d.history.assert_not_awaited()
    assert not d.event._execution_active
    assert current_message_event.get() is None
    assert d.event._stream is None
