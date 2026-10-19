"""Optional task progress, independent of Slack answer delivery."""

from typing import TYPE_CHECKING

from slack_sdk.errors import SlackApiError

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import Json

from .slack_stream import slack_api_request

if TYPE_CHECKING:
    from .slack_event import SlackMessageEvent


class SlackTaskCards:
    """Maintain one native plan stream for the current reply.

    Args:
        event: Slack event with an existing thread destination.
    """

    def __init__(self, event: "SlackMessageEvent") -> None:
        self.event = event
        self.ts: str | None = None
        self.tasks: dict[str, dict] = {}
        self.disabled = False
        self.fallback = False
        self.fallback_ts: str | None = None

    async def update(self, chain: MessageChain) -> None:
        """Apply a structured tool start or result without exposing tool arguments.

        Args:
            chain: Tool metadata emitted by the agent runner.
        """
        if self.disabled or not chain.chain or not isinstance(chain.chain[0], Json):
            return
        data = chain.chain[0].data
        if not isinstance(data, dict) or not data.get("id"):
            return
        task_id = str(data["id"])
        if chain.type == "tool_call":
            task = {
                "type": "task_update",
                "id": task_id,
                "title": str(data.get("name") or "Tool")[:256],
                "status": "in_progress",
            }
        elif task_id in self.tasks:
            task = {
                **self.tasks[task_id],
                "status": "error" if data.get("is_error") else "complete",
            }
        else:
            return
        # Keep an unconfirmed result pending until Slack acknowledges the update.
        self.tasks[task_id] = {**task, "status": "in_progress"}
        raw = self.event.message_obj.raw_message
        client = self.event.web_client
        if not self.fallback:
            try:
                if self.ts is not None:
                    try:
                        await slack_api_request(
                            client,
                            "chat_appendStream",
                            channel=raw["channel"],
                            ts=self.ts,
                            chunks=[task],
                        )
                    except SlackApiError as exc:
                        if (
                            exc.response.get("error")
                            != "message_not_in_streaming_state"
                        ):
                            raise
                        # Slack confirmed the old card is sealed. Reopen once with
                        # the full state; uncertain writes must never reach here.
                        self.ts = None
                if self.ts is None:
                    team = raw.get("user_team") or raw.get("team")
                    if not team:
                        team = (await slack_api_request(client, "auth_test"))["team_id"]
                    response = await slack_api_request(
                        client,
                        "chat_startStream",
                        channel=raw["channel"],
                        thread_ts=raw["thread_ts"],
                        recipient_user_id=self.event.get_sender_id(),
                        recipient_team_id=team,
                        task_display_mode="plan",
                        chunks=[
                            {"type": "plan_update", "title": "AstrBot 工具进度"},
                            *(
                                task if key == task_id else value
                                for key, value in self.tasks.items()
                            ),
                        ],
                    )
                    self.ts = response["ts"]
                self.tasks[task_id] = task
                return
            except Exception as exc:
                # Only explicit capability/format rejections allow a text fallback.
                # Unknown write outcomes must never create another progress stream.
                self.fallback = isinstance(exc, SlackApiError) and exc.response.get(
                    "error"
                ) in {
                    "missing_scope",
                    "feature_not_enabled",
                    "invalid_method",
                    "unknown_method",
                    "not_allowed",
                    "streaming_not_allowed",
                    "invalid_arguments",
                    "invalid_blocks",
                    "message_not_in_streaming_state",
                }
                self.disabled = not self.fallback
                logger.warning(
                    "Slack task cards unavailable; answer delivery continues."
                )
                await self.close()
        if self.fallback:
            statuses = {
                "in_progress": "进行中",
                "complete": "已完成",
                "error": "失败",
            }
            tasks = {**self.tasks, task_id: task}
            text = "AstrBot 工具进度\n" + "\n".join(
                f"• {item['title']}：{statuses[item['status']]}"
                for item in tasks.values()
            )
            try:
                response = await slack_api_request(
                    client,
                    "chat_update" if self.fallback_ts else "chat_postMessage",
                    channel=raw["channel"],
                    **(
                        {"ts": self.fallback_ts}
                        if self.fallback_ts
                        else {"thread_ts": raw["thread_ts"]}
                    ),
                    text=text,
                    parse="full",
                )
                self.fallback_ts = response["ts"]
                self.tasks[task_id] = task
            except Exception:
                self.disabled = True
                logger.warning("Slack task progress fallback failed.", exc_info=True)

    async def close(self) -> None:
        """Seal progress without allowing cleanup failures to suppress the answer."""
        if (
            self.fallback_ts
            and not self.disabled
            and any(task["status"] == "in_progress" for task in self.tasks.values())
        ):
            # Attempt cleanup once; never resume progress after an uncertain write.
            self.disabled = True
            statuses = {
                "in_progress": "状态未确认",
                "complete": "已完成",
                "error": "失败",
            }
            try:
                await slack_api_request(
                    self.event.web_client,
                    "chat_update",
                    channel=self.event.message_obj.raw_message["channel"],
                    ts=self.fallback_ts,
                    text="AstrBot 工具进度\n"
                    + "\n".join(
                        f"• {task['title']}：{statuses[task['status']]}"
                        for task in self.tasks.values()
                    ),
                    parse="full",
                )
            except Exception:
                logger.warning("Slack text progress cleanup failed.", exc_info=True)
        if not self.ts:
            return
        client = self.event.web_client
        channel = self.event.message_obj.raw_message["channel"]
        try:
            pending = [
                {
                    **task,
                    "status": "error",
                    "title": task["title"][:240] + "（状态未确认）",
                }
                for task in self.tasks.values()
                if task["status"] == "in_progress"
            ]
            if pending:
                await slack_api_request(
                    client,
                    "chat_appendStream",
                    channel=channel,
                    ts=self.ts,
                    chunks=pending,
                )
        except Exception:
            logger.warning("Slack task-card final status update failed.", exc_info=True)
        finally:
            try:
                await slack_api_request(
                    client, "chat_stopStream", channel=channel, ts=self.ts
                )
            except Exception:
                logger.warning("Slack task-card cleanup failed.", exc_info=True)
            self.ts = None
