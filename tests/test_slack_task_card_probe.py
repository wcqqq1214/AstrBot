import asyncio
from unittest.mock import AsyncMock

import pytest

from scripts import verify_slack_task_cards as probe


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_probe_cleanup_preserves_failure_or_cancellation(monkeypatch, cancel):
    monkeypatch.setenv("SLACK_BOT_TOKEN", "fake")
    monkeypatch.setattr(
        "sys.argv", ["probe", "--channel", "C123", "--recipient", "U123"]
    )
    monkeypatch.setattr(
        probe.AsyncWebClient, "auth_test", AsyncMock(return_value={"team_id": "T123"})
    )
    failure = asyncio.CancelledError() if cancel else RuntimeError("original failure")
    monkeypatch.setattr(probe.asyncio, "sleep", AsyncMock(side_effect=failure))
    calls = []

    async def api(client, method, **kwargs):
        calls.append(method)
        if method == "chat_stopStream":
            raise RuntimeError("secondary cleanup failure")
        return {"ts": "1"}

    monkeypatch.setattr(probe, "slack_api_request", api)
    with pytest.raises(type(failure)) as caught:
        await probe.main()
    assert caught.value is failure
    assert calls == ["chat_postMessage", "chat_startStream", "chat_stopStream"]
