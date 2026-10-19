import asyncio
import hashlib
import hmac
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from slack_sdk.socket_mode.request import SocketModeRequest

from astrbot.api.event import AstrMessageEvent
from astrbot.api.message_components import Plain
from astrbot.core.message.message_event_result import MessageEventResult
from astrbot.core.pipeline.respond.stage import RespondStage
from astrbot.core.platform.sources.slack.client import (
    SlackSocketClient,
    SlackWebhookClient,
)
from astrbot.core.platform.sources.slack.slack_adapter import SlackAdapter


@pytest.fixture(params=["webhook", "socket"])
def ingress(request, monkeypatch):
    queue = asyncio.Queue()
    adapter = SlackAdapter(
        {"id": "slack-test", "bot_token": "test", "app_token": "test"}, {}, queue
    )
    adapter.web_client.users_info = AsyncMock(return_value={"user": {"name": "Tester"}})
    adapter.web_client.conversations_info = AsyncMock(
        return_value={"channel": {"is_im": False}}
    )
    adapter.web_client.api_call = AsyncMock(return_value={"ok": True, "ts": "2"})
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(
        "astrbot.core.platform.sources.slack.slack_adapter.time",
        SimpleNamespace(monotonic=lambda: clock.now, time=lambda: 1.0),
    )
    monkeypatch.setattr(AstrMessageEvent, "send", AsyncMock())
    monkeypatch.setattr(
        "astrbot.core.pipeline.respond.stage.call_event_hook",
        AsyncMock(return_value=False),
    )
    webhook = SlackWebhookClient(
        adapter.web_client, "secret", event_handler=adapter._handle_webhook_event
    )
    socket = SlackSocketClient(
        adapter.web_client, "token", adapter._handle_socket_event
    )
    socket.socket_client = SimpleNamespace(send_socket_mode_response=AsyncMock())

    async def deliver(event_id="Ev1", valid=True):
        payload = {
            "type": "event_callback",
            "event_id": event_id,
            "event": {
                "type": "message",
                "user": "U1",
                "channel": "C1",
                "ts": "1.0",
                "text": "hello",
            },
        }
        if request.param == "socket":
            await socket._handle_events(
                None,
                SocketModeRequest(
                    type="events_api", envelope_id="envelope", payload=payload
                ),
            )
            return 200
        body = json.dumps(payload).encode()
        timestamp = "1700000000"
        signature = (
            "v0="
            + hmac.new(
                b"secret", b"v0:" + timestamp.encode() + b":" + body, hashlib.sha256
            ).hexdigest()
        )
        response = await webhook.handle_callback(
            SimpleNamespace(
                get_data=AsyncMock(return_value=body),
                headers={
                    "X-Slack-Request-Timestamp": timestamp,
                    "X-Slack-Signature": signature if valid else "invalid",
                },
            )
        )
        return response.status_code

    return SimpleNamespace(
        adapter=adapter,
        queue=queue,
        clock=clock,
        deliver=deliver,
        socket=socket,
        mode=request.param,
    )


@pytest.mark.asyncio
async def test_concurrent_and_completed_retries_send_only_one_reply(ingress):
    entered, release = asyncio.Event(), asyncio.Event()

    async def users_info(**kwargs):
        entered.set()
        await release.wait()
        return {"user": {"name": "Tester"}}

    ingress.adapter.web_client.users_info.side_effect = users_info
    if ingress.mode == "webhook":
        assert await ingress.deliver(valid=False) == 400
    first = asyncio.create_task(ingress.deliver())
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        # Pending work must not expire while message conversion is still running.
        ingress.clock.now = 601
        assert await ingress.deliver() == 200
    finally:
        release.set()
        await first
    assert await ingress.deliver() == 200
    assert ingress.queue.qsize() == 1
    event = ingress.queue.get_nowait()
    event.set_result(MessageEventResult(chain=[Plain("Only reply")]))
    stage = RespondStage()
    stage.platform_settings, stage.enable_seg = {}, False
    await stage.process(event)
    ingress.adapter.web_client.api_call.assert_awaited_once()
    assert (
        ingress.adapter.web_client.api_call.call_args.kwargs["json"]["markdown_text"]
        == "Only reply"
    )
    if ingress.mode == "socket":
        assert ingress.socket.socket_client.send_socket_mode_response.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["conversion", "queue", "cancel"])
async def test_failed_ingress_remains_retryable(ingress, monkeypatch, failure):
    if failure == "queue":
        original = ingress.queue.put_nowait

        def reject_enqueue(event):
            raise asyncio.QueueFull()

        monkeypatch.setattr(ingress.queue, "put_nowait", reject_enqueue)
    else:
        original = ingress.adapter.convert_message
        ingress.adapter.convert_message = AsyncMock(
            side_effect=asyncio.CancelledError()
            if failure == "cancel"
            else RuntimeError("conversion failed")
        )
    if failure == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await ingress.deliver()
    else:
        status = await ingress.deliver()
        if ingress.mode == "webhook":
            assert status == 500
    assert ingress.queue.empty()
    if failure == "queue":
        monkeypatch.setattr(ingress.queue, "put_nowait", original)
    else:
        ingress.adapter.convert_message = original
    assert await ingress.deliver() == 200
    assert await ingress.deliver() == 200
    assert ingress.queue.qsize() == 1


@pytest.mark.asyncio
async def test_distinct_missing_and_expired_ids_are_not_dropped(ingress):
    for event_id in ("Ev1", "Ev2", None, None):
        assert await ingress.deliver(event_id) == 200
    assert ingress.queue.qsize() == 4
    ingress.clock.now = 600
    assert await ingress.deliver("Ev1") == 200
    assert ingress.queue.qsize() == 5
    for number in range(9_999):
        ingress.adapter._seen_event_ids[f"cached-{number}"] = 600
    assert await ingress.deliver("latest") == 200
    assert len(ingress.adapter._seen_event_ids) == 10_000
