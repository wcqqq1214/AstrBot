"""Progressive Slack replies with native streams and in-place Markdown edits."""

import asyncio
import time
from collections.abc import AsyncGenerator
from copy import copy
from typing import TYPE_CHECKING

from slack_sdk.errors import SlackApiError

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import At, Plain, Reply

if TYPE_CHECKING:
    from .slack_event import SlackMessageEvent


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
        self.native = bool(self.thread) and callable(
            getattr(self.client, "chat_startStream", None)
        )
        self.ts: str | None = None
        self.text = ""
        self.sent = ""
        self.open = False
        self.last_update = 0.0

    async def _request(self, method: str, **kwargs):
        """Call Slack, retrying once only after an explicit rate-limit rejection.

        Args:
            method: SDK method name.
            **kwargs: Request payload.

        Returns:
            Slack's response, including confirmation that a stream is already closed.

        Raises:
            SlackApiError: Slack rejects the request or the retry.
        """
        client = self.client
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

    async def _flush(self, final: bool = False) -> None:
        """Publish accumulated text, sealing it at a content boundary.

        Args:
            final: Close this segment even if its last update was recent.
        """
        if not self.text.strip():
            if final:
                self.text = ""
            return
        if not final and (
            self.text == self.sent
            or (self.ts is not None and time.monotonic() - self.last_update < 1.5)
        ):
            return

        # Oversized text uses the plain-text field. Never submit more than 12k
        # characters to Markdown or replay a native append.
        body = (
            {"markdown_text": self.text}
            if len(self.text) <= 12_000
            else {"text": self.text, "blocks": []}
        )
        if self.native and len(self.text) > 12_000:
            if self.open:
                await self._request("chat_stopStream", channel=self.channel, ts=self.ts)
                self.open = False
            self.native = False

        if self.native:
            try:
                if self.ts is None:
                    if not self.team:
                        self.team = (await self._request("auth_test"))["team_id"]
                    response = await self._request(
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
                    await self._request(
                        "chat_appendStream",
                        channel=self.channel,
                        ts=self.ts,
                        markdown_text=self.text[len(self.sent) :],
                    )
                self.sent = self.text
                if final:
                    await self._request(
                        "chat_stopStream", channel=self.channel, ts=self.ts
                    )
                    self.open = False
            except Exception as exc:
                if self.ts is None:
                    # An ambiguous start may already have created a message.
                    # Only explicit capability rejections permit a new post.
                    if not isinstance(exc, SlackApiError) or exc.response.get(
                        "error"
                    ) not in {
                        "missing_scope",
                        "not_allowed",
                        "feature_not_enabled",
                        "invalid_method",
                        "unknown_method",
                        "streaming_not_allowed",
                        "invalid_thread_ts",
                        "channel_type_not_supported",
                    }:
                        raise
                else:
                    # Close without appending anything, then replace the same
                    # message with the full buffer. A timeout may have delivered
                    # the delta already; retrying that delta would duplicate it.
                    try:
                        await self._request(
                            "chat_stopStream", channel=self.channel, ts=self.ts
                        )
                        self.open = False
                    except Exception:
                        logger.warning(
                            "Slack stream could not be sealed before recovery.",
                            exc_info=True,
                        )
                self.native = False
                logger.info("Slack native streaming unavailable; using message edits.")

        if not self.native:
            if self.ts is None:
                destination = {"channel": self.channel}
                if self.thread:
                    destination["thread_ts"] = self.thread
                response = await self._request(
                    "chat_postMessage", **destination, **body
                )
                self.ts = response["ts"]
            elif self.sent != self.text or self.open:
                await self._request(
                    "chat_update", channel=self.channel, ts=self.ts, **body
                )
            self.sent = self.text
        if final and self.open:
            await self._request("chat_stopStream", channel=self.channel, ts=self.ts)
            self.open = False
        self.last_update = time.monotonic()
        if final:
            self.ts = None
            self.text = self.sent = ""
            self.last_update = 0.0

    async def run(self, generator: AsyncGenerator[MessageChain, None]) -> None:
        """Consume deltas, closing text before media and tool boundaries.

        Args:
            generator: Ordered text deltas and message components.
        """
        reasoning = False
        try:
            async for chain in generator:
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
                        # Slack truncates text above 40k. Split oversized text
                        # while preserving every source character.
                        while text:
                            remaining = 39_000 - len(self.text)
                            self.text += text[:remaining]
                            text = text[remaining:]
                            if text:
                                await self._flush(final=True)
                        await self._flush()
                    else:
                        await self._flush(final=True)
                        await self.event.send(chain.derive([component]))
            await self._flush(final=True)
        finally:
            # Cancellation or generator failure must not leave a typing stream
            # open. Do not repost on errors with an unknown delivery outcome.
            if self.open and self.ts:
                try:
                    await self._request(
                        "chat_stopStream", channel=self.channel, ts=self.ts
                    )
                except Exception:
                    logger.warning("Slack stream cleanup failed.", exc_info=True)
