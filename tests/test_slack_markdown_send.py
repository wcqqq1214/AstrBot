from functools import partial
from unittest.mock import AsyncMock

import pytest
from slack_sdk.errors import SlackApiError
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
async def test_slack_mixed_image_reply_preserves_markdown_order(
    slack_event, slack_sender, quoted
):
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
            "text": "- Before\n  - Nested\n\n[图片]\n\nAfter.",
            "blocks": [
                {
                    "type": "markdown",
                    "text": "- Before\n  - Nested",
                },
                {
                    "type": "image",
                    "image_url": "https://example.com/image.png",
                    "alt_text": "图片",
                },
                {"type": "markdown", "text": "After."},
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    [
        "invalid_blocks",
        "invalid_blocks_format",
        "msg_blocks_too_long",
        "msg_too_long",
        "too_many_blocks",
    ],
)
async def test_slack_mixed_markdown_rejection_reuses_uploaded_image(
    slack_event, tmp_path, error_code
):
    path = tmp_path / "image.png"
    path.write_bytes(b"test image")
    client = slack_event.web_client
    client.files_upload_v2 = AsyncMock(
        return_value={
            "ok": True,
            "files": [{"url_private": "https://slack.test/image.png"}],
        }
    )
    client.api_call.side_effect = [
        SlackApiError("Rejected blocks", {"ok": False, "error": error_code}),
        {"ok": True},
    ]

    await slack_event.send(
        MessageChain(
            [
                Plain("- Before\n  - Child"),
                Image.fromFileSystem(str(path)),
                Plain("After."),
            ]
        )
    )

    client.files_upload_v2.assert_awaited_once()
    assert client.api_call.await_count == 2
    native, fallback = [call.kwargs["json"] for call in client.api_call.await_args_list]
    assert [block["type"] for block in native["blocks"]] == [
        "markdown",
        "image",
        "markdown",
    ]
    assert fallback == {
        "channel": "C123",
        "text": "",
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "- Before\n  - Child"},
            },
            native["blocks"][1],
            {"type": "section", "text": {"type": "mrkdwn", "text": "After."}},
        ],
    }
    assert native["blocks"][1]["slack_file"]["url"] == "https://slack.test/image.png"
    assert path.read_bytes() == b"test image"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code", [None, "ratelimited", "invalid_auth", "service_unavailable"]
)
async def test_slack_mixed_markdown_does_not_retry_other_errors(
    slack_event, error_code
):
    error = (
        SlackApiError("Send failed", {"ok": False, "error": error_code})
        if error_code
        else TimeoutError("Response timed out")
    )
    slack_event.web_client.api_call.side_effect = error

    with pytest.raises(type(error)):
        await slack_event.send(
            MessageChain(
                [Plain("- First"), Image.fromURL("https://example.com/image.png")]
            )
        )

    slack_event.web_client.api_call.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [12_000, 12_001])
async def test_slack_mixed_markdown_limit_is_cumulative(slack_event, length):
    before = "a" * 6_000
    after = "b" * (length - len(before))
    await slack_event.send(
        MessageChain(
            [
                Plain(before),
                Image.fromURL("https://example.com/image.png"),
                Plain(after),
            ]
        )
    )

    slack_event.web_client.api_call.assert_awaited_once()
    blocks = slack_event.web_client.api_call.await_args.kwargs["json"]["blocks"]
    text_type = "markdown" if length == 12_000 else "section"
    assert [block["type"] for block in blocks] == [text_type, "image", text_type]
    if length == 12_000:
        assert [blocks[0]["text"], blocks[2]["text"]] == [before, after]
    else:
        assert [blocks[0]["text"]["text"], blocks[2]["text"]["text"]] == [before, after]


@pytest.mark.asyncio
@pytest.mark.parametrize("with_mention", [False, True])
async def test_slack_image_only_and_mention_keep_legacy_blocks(
    slack_event, with_mention
):
    chain = [Image.fromURL("https://example.com/image.png")]
    if with_mention:
        chain[:0] = [At(qq="U456"), Plain("- First")]
    await slack_event.send(MessageChain(chain))

    slack_event.web_client.api_call.assert_awaited_once()
    payload = slack_event.web_client.api_call.await_args.kwargs["json"]
    assert "markdown_text" not in payload
    assert [block["type"] for block in payload["blocks"]] == (
        ["section", "image"] if with_mention else ["image"]
    )
