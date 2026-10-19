from functools import partial
from unittest.mock import AsyncMock

import pytest
from slack_sdk.web.async_client import AsyncWebClient

from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.message_components import At, Image, Plain, Reply
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    Platform,
    PlatformMetadata,
)
from astrbot.core.message.message_event_result import MessageEventResult
from astrbot.core.pipeline.respond.stage import RespondStage
from astrbot.core.platform.astr_message_event import MessageSesion
from astrbot.core.platform.sources.slack.slack_adapter import SlackAdapter
from astrbot.core.platform.sources.slack.slack_event import SlackMessageEvent


@pytest.fixture
def slack_event(monkeypatch):
    monkeypatch.setattr(AstrMessageEvent, "send", AsyncMock())
    monkeypatch.setattr(AstrMessageEvent, "send_streaming", AsyncMock())
    monkeypatch.setattr(Platform, "send_by_session", AsyncMock())
    message = AstrBotMessage()
    message.type = MessageType.GROUP_MESSAGE
    message.group_id = "C123"
    message.sender = MessageMember(user_id="U123", nickname="Tester")
    message.message = []
    client = AsyncWebClient(token="xoxb-test")
    # Exercise SDK serialization without sending messages to a real workspace.
    client.api_call = AsyncMock(return_value={"ok": True, "ts": "1.0"})
    return SlackMessageEvent(
        "test",
        message,
        PlatformMetadata(name="slack", id="slack", description="Slack"),
        "C123",
        client,
    )


@pytest.fixture(params=["event", "respond", "session", "buffered_stream"])
def slack_sender(request, slack_event, monkeypatch):
    if request.param == "event":
        return slack_event.send
    if request.param == "session":
        adapter = object.__new__(SlackAdapter)
        adapter.web_client = slack_event.web_client
        session = MessageSesion(
            platform_name="slack",
            message_type=MessageType.GROUP_MESSAGE,
            session_id="C123",
        )
        return partial(adapter.send_by_session, session)
    if request.param == "respond":
        stage = RespondStage()
        stage.platform_settings = {}
        stage.enable_seg = False
        monkeypatch.setattr(
            "astrbot.core.pipeline.respond.stage.call_event_hook",
            AsyncMock(return_value=False),
        )

        async def respond(chain):
            slack_event.set_result(MessageEventResult(chain=chain.chain))
            await stage.process(slack_event)

        return respond

    async def stream(chain):
        async def fragments():
            for component in chain.chain:
                yield MessageChain([component])

        await slack_event.send_streaming(fragments(), use_fallback=False)

    return stream


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted", [False, True], ids=["plain", "quoted"])
@pytest.mark.parametrize(
    "content",
    [
        "- **First**\n- [Second](https://example.com)\n\nFollowing paragraph.",
        "- Parent\n  - Child\n    - `Grandchild`\n- Next\n\nFollowing paragraph.",
        "1. First\n2. Second\n   1. Child\n3. Third\n\nFollowing paragraph.",
    ],
    ids=["unordered", "nested", "ordered"],
)
async def test_slack_reply_preserves_markdown(
    slack_event, slack_sender, content, quoted
):
    # Split inside the list to verify that message fragments are reassembled.
    split = content.index("\n") + 1
    chain = [Plain(content[:split]), Plain(content[split:])]
    if quoted:
        chain.insert(0, Reply(id="1.0"))
    await slack_sender(MessageChain(chain))

    slack_event.web_client.api_call.assert_awaited_once_with(
        "chat.postMessage", json={"channel": "C123", "markdown_text": content}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [12_000, 12_001])
async def test_slack_markdown_length_limit(slack_event, length):
    content = "a" * length
    await slack_event.send(MessageChain([Plain(content)]))

    payload = slack_event.web_client.api_call.await_args.kwargs["json"]
    if length == 12_000:
        assert payload == {"channel": "C123", "markdown_text": content}
    else:
        assert payload == {
            "channel": "C123",
            "text": "",
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": content}}
            ],
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted", [False, True])
@pytest.mark.parametrize("content", [None, "", " \n\t"])
async def test_slack_empty_text_is_not_sent(slack_event, content, quoted):
    chain = [] if content is None else [Plain(content)]
    if quoted:
        chain.insert(0, Reply(id="1.0"))
    await slack_event.send(MessageChain(chain))
    slack_event.web_client.api_call.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted", [False, True])
@pytest.mark.parametrize("slack_sender", ["event", "respond", "session"], indirect=True)
async def test_slack_mixed_image_reply_keeps_blocks(slack_event, slack_sender, quoted):
    chain = [
        Plain("- Before\n  - Nested"),
        Image.fromURL("https://example.com/image.png"),
        Plain("After."),
    ]
    if quoted:
        chain.insert(0, Reply(id="1.0"))
    await slack_sender(MessageChain(chain))

    slack_event.web_client.api_call.assert_awaited_once_with(
        "chat.postMessage",
        json={
            "channel": "C123",
            "text": "",
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "- Before\n  - Nested"},
                },
                {
                    "type": "image",
                    "image_url": "https://example.com/image.png",
                    "alt_text": "图片",
                },
                {"type": "section", "text": {"type": "mrkdwn", "text": "After."}},
            ],
        },
    )


@pytest.mark.asyncio
async def test_slack_mention_keeps_existing_format(slack_event):
    await slack_event.send(MessageChain([At(qq="U456"), Plain(" hello")]))

    payload = slack_event.web_client.api_call.await_args.kwargs["json"]
    assert "markdown_text" not in payload
    assert payload["blocks"] == [
        {"type": "section", "text": {"type": "mrkdwn", "text": "<@U456> hello"}}
    ]


@pytest.mark.asyncio
async def test_slack_markdown_timeout_does_not_resend(slack_event):
    slack_event.web_client.api_call.side_effect = TimeoutError("Response timed out")

    with pytest.raises(TimeoutError):
        await slack_event.send(MessageChain([Plain("- First\n- Second")]))

    slack_event.web_client.api_call.assert_awaited_once()
