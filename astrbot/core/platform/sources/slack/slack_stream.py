"""Progressive Slack replies with native streams and in-place Markdown edits."""

import asyncio
import time
from collections.abc import AsyncGenerator
from contextlib import aclosing
from copy import copy
from typing import TYPE_CHECKING
from weakref import WeakSet

from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import At, Plain, Reply

from .slack_markdown import split_slack_markdown

if TYPE_CHECKING:
    from .slack_event import SlackMessageEvent

_UNSUPPORTED_STREAM_CLIENTS: WeakSet[AsyncWebClient] = WeakSet()
_STREAM_CAPABILITY_ERRORS = {
    "missing_scope",
    "feature_not_enabled",
    "invalid_method",
    "unknown_method",
}


async def slack_api_request(client: AsyncWebClient, method: str, **kwargs):
    """Call Slack, retrying once only after an explicit rate-limit rejection.

    Args:
        client: Shared Slack client; its retry settings are left unchanged.
        method: SDK method name.
        **kwargs: Request payload.

    Returns:
        Slack's response, including confirmation that a stream is already closed.

    Raises:
        SlackApiError: Slack rejects the request or the retry.
    """
    if method in {"chat_postMessage", "chat_startStream", "chat_appendStream"}:
        # A lost response does not mean the write failed. Isolate retry
        # settings so concurrent requests using the shared client are unaffected.
        client = copy(client)
        client.retry_handlers = []
    try:
        return await getattr(client, method)(**kwargs)
    except SlackApiError as exc:
        # A previous stop may have succeeded despite a lost response.
        if (
            method == "chat_stopStream"
            and exc.response.get("error") == "message_not_in_streaming_state"
        ):
            return exc.response
        if exc.response.get("error") not in {"ratelimited", "rate_limited"}:
            raise
        headers = getattr(exc.response, "headers", {}) or {}
        await asyncio.sleep(
            max(
                1.0,
                float(headers.get("Retry-After", headers.get("retry-after", "1"))),
            )
        )
        return await getattr(client, method)(**kwargs)


class SlackMessageStream:
    """Keep each contiguous text segment in one Slack message.

    Args:
        event: Incoming event providing the client and reply destination.
    """

    def __init__(self, event: "SlackMessageEvent") -> None:
        self.event = event
        self.client = event.web_client
        raw = getattr(event.message_obj, "raw_message", None)
        raw = raw if isinstance(raw, dict) else {}
        self.channel = (
            raw.get("channel") or event.get_group_id() or event.get_sender_id()
        )
        self.thread = raw.get("thread_ts")
        self.team = raw.get("user_team") or raw.get("team")
        self.native = (
            bool(self.thread)
            and self.client not in _UNSUPPORTED_STREAM_CLIENTS
            and callable(getattr(self.client, "chat_startStream", None))
        )
        self.ts: str | None = None
        self.text = ""
        self.sent = ""
        self.empty_text: str | None = None
        self.open = False
        self.last_update = 0.0
        self.lock = asyncio.Lock()
        self.task = asyncio.current_task()
        self.error: Exception | asyncio.CancelledError | None = None

    async def _flush(self, final: bool = False) -> None:
        """Publish accumulated text, sealing it at a content boundary.

        Args:
            final: Close this segment even if its last update was recent.
        """
        if self.error is not None:
            raise self.error
        if not self.text.strip():
            if final:
                self.text = ""
            return
        if not final and (
            self.text == self.sent
            or self.text == self.empty_text
            or (self.ts is not None and time.monotonic() - self.last_update < 1.5)
        ):
            return

        # Preserve a completed literal marker without retrying empty Markdown.
        literal = final and self.text == self.empty_text
        body = (
            {"text": self.text, "mrkdwn": False}
            if literal
            else {"markdown_text": self.text}
        )
        # A safe split may move already-visible text to the next message. Replace
        # the existing head in place instead of appending overlapping characters.
        if self.native and self.sent and not self.text.startswith(self.sent):
            if self.open:
                await slack_api_request(
                    self.client, "chat_stopStream", channel=self.channel, ts=self.ts
                )
                self.open = False
            self.native = False

        if self.native and not literal:
            start_attempted = False
            try:
                if self.ts is None:
                    if not self.team:
                        self.team = (await slack_api_request(self.client, "auth_test"))[
                            "team_id"
                        ]
                    start_attempted = True
                    response = await slack_api_request(
                        self.client,
                        "chat_startStream",
                        channel=self.channel,
                        thread_ts=self.thread,
                        recipient_user_id=self.event.get_sender_id(),
                        recipient_team_id=self.team,
                        markdown_text=self.text,
                    )
                    self.ts = response["ts"]
                    self.open = True
                elif self.text != self.sent:
                    await slack_api_request(
                        self.client,
                        "chat_appendStream",
                        channel=self.channel,
                        ts=self.ts,
                        markdown_text=self.text[len(self.sent) :],
                    )
                self.sent = self.text
                if final:
                    await slack_api_request(
                        self.client, "chat_stopStream", channel=self.channel, ts=self.ts
                    )
                    self.open = False
            except Exception as exc:
                if self.ts is None:
                    # An ambiguous start may already have created a message.
                    # Only explicit capability rejections permit a new post.
                    error = (
                        exc.response.get("error")
                        if isinstance(exc, SlackApiError)
                        else None
                    )
                    if error == "no_text" and not final:
                        self.empty_text = self.text
                        return
                    if error not in _STREAM_CAPABILITY_ERRORS and error not in {
                        "not_allowed",
                        "streaming_not_allowed",
                        "invalid_thread_ts",
                        "channel_type_not_supported",
                    }:
                        raise
                    if start_attempted and error in _STREAM_CAPABILITY_ERRORS:
                        # Cache only app-level capability failures, scoped to this
                        # adapter's client lifetime. Thread errors remain local.
                        _UNSUPPORTED_STREAM_CLIENTS.add(self.client)
                else:
                    # Close without appending anything, then replace the same
                    # message with the full buffer. A timeout may have delivered
                    # the delta already; retrying that delta would duplicate it.
                    try:
                        await slack_api_request(
                            self.client,
                            "chat_stopStream",
                            channel=self.channel,
                            ts=self.ts,
                        )
                        self.open = False
                    except Exception:
                        logger.warning(
                            "Slack stream could not be sealed before recovery.",
                            exc_info=True,
                        )
                self.native = False
                logger.info("Slack native streaming unavailable; using message edits.")

        if not self.native or literal:
            if self.ts is None:
                destination = {"channel": self.channel}
                if self.thread:
                    destination["thread_ts"] = self.thread
                try:
                    response = await slack_api_request(
                        self.client, "chat_postMessage", **destination, **body
                    )
                except SlackApiError as exc:
                    if exc.response.get("error") != "no_text" or final:
                        raise
                    # Slack can reject an unfinished heading before content arrives.
                    self.empty_text = self.text
                    return
                self.ts = response["ts"]
            elif self.sent != self.text or self.open:
                await slack_api_request(
                    self.client, "chat_update", channel=self.channel, ts=self.ts, **body
                )
            self.sent = self.text
        if final and self.open:
            await slack_api_request(
                self.client, "chat_stopStream", channel=self.channel, ts=self.ts
            )
            self.open = False
        self.last_update = time.monotonic()
        if final:
            self.ts = None
            self.text = self.sent = ""
            self.empty_text = None
            self.last_update = 0.0

    async def run(self, generator: AsyncGenerator[MessageChain, None]) -> None:
        """Consume deltas, closing text before media and tool boundaries.

        Args:
            generator: Ordered text deltas and message components.
        """
        queue: asyncio.Queue[MessageChain] = asyncio.Queue()
        consumed = asyncio.Event()

        async def produce() -> None:
            """Yield one chunk at a time, closing generation on cancellation."""
            # Keep generator context in one task and do not cross a tool boundary
            # until its preceding message has been fully processed.
            async with aclosing(generator):
                async for chain in generator:
                    await queue.put(chain)
                    await consumed.wait()
                    consumed.clear()

        producer = asyncio.create_task(produce())
        pending = None
        reasoning = False
        try:
            while True:
                if pending is None:
                    pending = asyncio.create_task(queue.get())
                timeout = None
                if (
                    self.text.strip()
                    and self.text != self.sent
                    and self.text != self.empty_text
                ):
                    timeout = max(0.0, self.last_update + 1.5 - time.monotonic())
                done, _ = await asyncio.wait(
                    {pending, producer},
                    timeout=timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    async with self.lock:
                        await self._flush()
                    continue
                if producer in done:
                    producer.result()
                    break
                chain = pending.result()
                pending = None
                try:
                    async with self.lock:
                        if chain.type in {"break", "aborted"}:
                            await self._flush(final=True)
                            if chain.type == "aborted":
                                break
                            continue
                        if chain.type in {"audio_chunk", "tool_call", "agent_stats"}:
                            continue
                        # Upstream only emits reasoning when the user enables it. Keep
                        # it separate from the answer without altering either Markdown.
                        if chain.chain and (chain.type == "reasoning") != reasoning:
                            await self._flush(final=True)
                            reasoning = chain.type == "reasoning"
                        for component in chain.chain:
                            if isinstance(component, Reply):
                                continue
                            if isinstance(component, (Plain, At)):
                                text = (
                                    component.text
                                    if isinstance(component, Plain)
                                    else f"<@{component.qq}>"
                                )
                                parts = split_slack_markdown(self.text + text)
                                for part in parts[:-1]:
                                    self.text = part
                                    await self._flush(final=True)
                                self.text = parts[-1]
                                await self._flush()
                            else:
                                await self._flush(final=True)
                                await self.event.send(chain.derive([component]))
                finally:
                    if chain.type != "aborted":
                        consumed.set()
            async with self.lock:
                await self._flush(final=True)
        finally:
            producer.cancel()
            if pending is not None:
                pending.cancel()
            await asyncio.gather(
                producer,
                *([pending] if pending is not None else []),
                return_exceptions=True,
            )
            # Cancellation or generator failure must not leave a typing stream
            # open. Do not repost on errors with an unknown delivery outcome.
            if self.open and self.ts:
                try:
                    await slack_api_request(
                        self.client, "chat_stopStream", channel=self.channel, ts=self.ts
                    )
                except Exception:
                    logger.warning("Slack stream cleanup failed.", exc_info=True)
