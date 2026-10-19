"""Failed Slack delivery must release the active agent before another message."""

import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

from astrbot.core.agent.events import MessageCompleted, TextDelta
from astrbot.core.agent.hooks import BaseAgentRunHooks
from astrbot.core.agent.output import PlainOutput
from astrbot.core.agent.run_context import ContextWrapper
from astrbot.core.agent.runners.base import AgentState
from astrbot.core.agent.runners.tool_loop_agent_runner import ToolLoopAgentRunner
from astrbot.core.agent.runtime import AgentInput, AgentPermissions
from astrbot.core.astr_agent_runtime import session as native_session
from astrbot.core.astr_agent_runtime.main_agent import MainAgentBuildResult
from astrbot.core.astr_agent_runtime.session import NativeAgentSession
from astrbot.core.config.default import DEFAULT_CONFIG
from astrbot.core.message.components import Plain
from astrbot.core.pipeline.process_stage import follow_up
from astrbot.core.pipeline.process_stage.method.agent_sub_stages import internal
from astrbot.core.pipeline.process_stage.stage import ProcessStage
from astrbot.core.pipeline.respond.stage import RespondStage
from astrbot.core.pipeline.scheduler import PipelineScheduler
from astrbot.core.platform.astrbot_message import AstrBotMessage, MessageMember
from astrbot.core.platform.message_type import MessageType
from astrbot.core.platform.platform_metadata import PlatformMetadata
from astrbot.core.platform.sources.slack.slack_event import SlackMessageEvent
from astrbot.core.provider.entities import ProviderRequest
from astrbot.core.utils.session_lock import session_lock_manager


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("streaming", "failure"),
    [
        (True, "rejected"),
        (True, "cancelled"),
        (False, "cancelled"),
        (True, "agent_error"),
    ],
)
async def test_failed_delivery_releases_agent_before_next_message(
    monkeypatch, streaming, failure
):
    config = copy.deepcopy(DEFAULT_CONFIG)
    config["provider_settings"]["streaming_response"] = streaming
    plugin_context = MagicMock()
    plugin_context.get_config.return_value = config
    context = SimpleNamespace(
        astrbot_config=config, plugin_manager=SimpleNamespace(context=plugin_context)
    )
    message = AstrBotMessage()
    message.type = MessageType.GROUP_MESSAGE
    message.group_id = "C123"
    message.sender = MessageMember(user_id="U123", nickname="Tester")
    message.message = [Plain("hello")]
    message.raw_message = {"channel": "C123", "team": "T123"}
    client = AsyncWebClient(token="xoxb-test")
    event = SlackMessageEvent(
        "hello",
        message,
        PlatformMetadata(name="slack", id="slack", description="Slack"),
        f"pipeline-cleanup-{failure}",
        client,
    )
    event.is_at_or_wake_command = True
    event.send_typing = AsyncMock()
    event.stop_typing = AsyncMock()
    runner = ToolLoopAgentRunner()
    provider = SimpleNamespace(
        provider_config={},
        get_model=lambda: "test",
        meta=lambda: SimpleNamespace(type="test"),
    )
    await runner.reset(
        provider=provider,
        request=ProviderRequest(prompt="hello"),
        run_context=ContextWrapper(
            context=SimpleNamespace(event=event, _output_turn=None)
        ),
        tool_executor=MagicMock(),
        agent_hooks=BaseAgentRunHooks(),
        streaming=streaming,
    )
    runner._state = AgentState.RUNNING
    runtime = SimpleNamespace(context=plugin_context, _active_main={})
    session = NativeAgentSession(runtime, "conversation", event.unified_msg_origin)
    runtime.active_turn = lambda umo: runtime._active_main.get(umo)
    runtime.open_session = AsyncMock(return_value=session)
    context.agent_runtimes = {"astrbot": runtime}
    session._prepare_turn = AsyncMock(
        return_value=MainAgentBuildResult(runner, runner.req, provider)
    )
    step_closed = asyncio.Event()

    async def step():
        nonlocal capture
        try:
            if failure == "agent_error":
                capture = follow_up.try_capture_follow_up(event, session.active_turn)
                assert capture is not None
                raise RuntimeError("Agent step failed before the error reply")
            if streaming:
                yield TextDelta(message_id="reply", part_index=0, text="Before")
            else:
                yield MessageCompleted(
                    message_id="reply", content=(PlainOutput(text="Before"),)
                )
            await asyncio.Event().wait()
        finally:
            step_closed.set()

    runner.step = step
    monkeypatch.setattr(
        internal,
        "prepare_agent_input",
        AsyncMock(return_value=AgentInput(event, AgentPermissions())),
    )
    monkeypatch.setattr(internal, "admit_agent_input", AsyncMock(return_value=True))
    monkeypatch.setattr(native_session, "record_agent_stats", AsyncMock())
    monkeypatch.setattr(native_session.Metric, "upload", AsyncMock())
    process = ProcessStage()
    respond = RespondStage()
    await process.initialize(context)
    await respond.initialize(context)
    scheduler = object.__new__(PipelineScheduler)
    scheduler.stages = [process, respond]
    posting = asyncio.Event()
    capture = None

    async def fail_post(*args, **kwargs):
        nonlocal capture
        if failure != "agent_error":
            capture = follow_up.try_capture_follow_up(event, session.active_turn)
        assert capture is not None
        posting.set()
        if failure == "cancelled":
            await asyncio.Event().wait()
        raise SlackApiError("Rejected", {"ok": False, "error": "invalid_auth"})

    client.api_call = AsyncMock(side_effect=fail_post)
    existing_tasks = asyncio.all_tasks()
    task = asyncio.create_task(scheduler._process_stages(event))
    try:
        await asyncio.wait_for(posting.wait(), 2)
        if failure == "cancelled":
            task.cancel()
        with pytest.raises(
            asyncio.CancelledError if failure == "cancelled" else SlackApiError
        ):
            await asyncio.wait_for(task, 2)

        assert runtime.active_turn(event.unified_msg_origin) is None
        assert session.active_turn is None
        assert session.pending_turn_count == 0
        next_event = copy.copy(event)
        next_event.message_str = "next"
        assert (
            follow_up.try_capture_follow_up(
                next_event, runtime.active_turn(next_event.unified_msg_origin)
            )
            is None
        )
        assert step_closed.is_set()
        assert event._stream is None
        assert event._agent_turn is None
        event.stop_typing.assert_awaited_once()
        assert capture.receipt.done() and not await capture.receipt
        assert (
            event.unified_msg_origin
            not in session_lock_manager._get_loop_manager()._locks
        )
        assert not [
            pending
            for pending in asyncio.all_tasks() - existing_tasks
            if pending is not capture.monitor_task and not pending.done()
        ]
    finally:
        await session.close()
        if capture is not None:
            await follow_up.finalize_follow_up_capture(
                capture, activated=False, consumed_marked=False
            )
