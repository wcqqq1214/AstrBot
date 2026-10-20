"""Exercise image tools through the shared dispatcher and Slack reply path."""

import asyncio
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from slack_sdk.web.async_client import AsyncWebClient

from astrbot.api.event import MessageChain
from astrbot.api.message_components import Plain
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    PlatformMetadata,
)
from astrbot.core.cron.events import CronMessageEvent
from astrbot.core.platform.astr_message_event import (
    current_message_event,
    event_context,
)
from astrbot.core.platform.sources.slack.slack_adapter import SlackAdapter
from astrbot.core.platform.sources.slack.slack_event import SlackMessageEvent
from astrbot.core.star.context import Context
from astrbot.core.tools.message_tools import SendMessageToUserTool


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
        "astrbot.core.tools.message_tools.workspace_root_for_context",
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
        wrapper=SimpleNamespace(context=SimpleNamespace(event=event, context=context)),
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
async def test_image_tool_keeps_proactive_routing(dispatch, mode):
    d = dispatch
    target = "slack:GroupMessage:C456" if mode == "other-session" else ""
    if mode == "cron":
        d.event = CronMessageEvent(
            context=d.context, session=d.event.session, message="Scheduled"
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
    d.history.assert_awaited_once()


@pytest.mark.asyncio
async def test_thread_image_delivery_loss_does_not_fall_back_to_channel(dispatch):
    d = dispatch
    d.client.api_call.side_effect = TimeoutError("Delivery unknown")
    with event_context(d.event):
        result = await d.tool.call(d.wrapper, messages=d.messages)
    assert result.startswith("error: failed to send message")
    d.client.api_call.assert_awaited_once()
    assert d.client.api_call.await_args.kwargs["json"]["thread_ts"] == "0.1"
    d.history.assert_not_awaited()
    assert not d.event._execution_active
    assert current_message_event.get() is None
    assert d.event._stream is None
