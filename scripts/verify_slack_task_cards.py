"""Manually verify Slack task cards without enabling them in the adapter.

Set SLACK_BOT_TOKEN and run with --channel and --recipient. Each stage pauses
for --pause-seconds (default 10), allowing screenshots of intermediate states.
The error task is simulated; this script does not execute model tools.
"""

import argparse
import asyncio
import logging
import os

from slack_sdk.web.async_client import AsyncWebClient

from astrbot.api.event import MessageChain
from astrbot.api.message_components import Plain
from astrbot.core.platform.sources.slack.slack_event import SlackMessageEvent
from astrbot.core.platform.sources.slack.slack_stream import slack_api_request


async def main() -> None:
    """Send a controlled plan-card lifecycle and a separate AstrBot reply."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--channel", required=True, help="Authorized test channel ID")
    parser.add_argument("--recipient", required=True, help="Test user's Slack ID")
    parser.add_argument("--pause-seconds", type=float, default=10)
    args = parser.parse_args()
    client = AsyncWebClient(token=os.environ["SLACK_BOT_TOKEN"])
    team = (await client.auth_test())["team_id"]
    root = await slack_api_request(
        client,
        "chat_postMessage",
        channel=args.channel,
        text="AstrBot 任务卡片验证：独立进度卡片 + 独立正文（受控模拟，非真实工具调用）",
    )
    thread = root["ts"]
    print(f"Thread: {thread}", flush=True)
    ts = None
    stopped = False
    try:
        response = await slack_api_request(
            client,
            "chat_startStream",
            channel=args.channel,
            thread_ts=thread,
            recipient_user_id=args.recipient,
            recipient_team_id=team,
            task_display_mode="plan",
            chunks=[
                {"type": "plan_update", "title": "AstrBot 正在验证任务卡片"},
                {
                    "type": "task_update",
                    "id": "probe-read",
                    "title": "读取资料（模拟）",
                    "status": "in_progress",
                },
            ],
        )
        ts = response["ts"]
        print(f"Card: {ts}; phase: running", flush=True)
        await asyncio.sleep(max(0, args.pause_seconds))
        await slack_api_request(
            client,
            "chat_appendStream",
            channel=args.channel,
            ts=ts,
            chunks=[
                {
                    "type": "task_update",
                    "id": "probe-read",
                    "title": "读取资料（模拟）",
                    "status": "complete",
                },
                {
                    "type": "task_update",
                    "id": "probe-check",
                    "title": "检查结果（模拟）",
                    "status": "in_progress",
                },
            ],
        )
        print("Phase: first task complete, second running", flush=True)
        await asyncio.sleep(max(0, args.pause_seconds))
        # Send terminal chunks once; cleanup stops without replaying these chunks.
        await slack_api_request(
            client,
            "chat_appendStream",
            channel=args.channel,
            ts=ts,
            chunks=[
                {"type": "plan_update", "title": "任务卡片验证结束（包含模拟失败）"},
                {
                    "type": "task_update",
                    "id": "probe-check",
                    "title": "检查结果（模拟失败）",
                    "status": "error",
                },
            ],
        )
        await slack_api_request(client, "chat_stopStream", channel=args.channel, ts=ts)
        stopped = True
        # Exercise the existing AstrBot reply path independently of task progress.
        await SlackMessageEvent._send_message(
            MessageChain(
                [
                    Plain(
                        "**任务卡片验证正文**\n\n"
                        "- 读取资料：模拟完成\n"
                        "- 检查结果：模拟失败\n\n"
                        "这条正文通过 AstrBot 发送链路独立发送。"
                    )
                ]
            ),
            client,
            args.channel,
            thread_ts=thread,
        )
        print("Phase: stopped; separate AstrBot reply sent", flush=True)
    finally:
        if ts and not stopped:
            try:
                await slack_api_request(
                    client, "chat_stopStream", channel=args.channel, ts=ts
                )
            except Exception:
                logging.exception("Slack task-card probe cleanup failed.")


if __name__ == "__main__":
    asyncio.run(main())
