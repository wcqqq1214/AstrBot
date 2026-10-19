from copy import deepcopy
from functools import partial
from types import SimpleNamespace
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
from astrbot.core.config.default import DEFAULT_CONFIG
from astrbot.core.message.message_event_result import (
    MessageEventResult,
)
from astrbot.core.pipeline.respond.stage import RespondStage
from astrbot.core.pipeline.result_decorate.stage import ResultDecorateStage
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


@pytest.fixture(params=["event", "respond", "session"])
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("quoted", "mentioned"),
    [(False, False), (True, False), (True, True)],
    ids=["plain", "quoted", "quoted-mention"],
)
async def test_slack_reply_preserves_markdown(
    slack_event, slack_sender, quoted, mentioned
):
    content = (
        "- **First**\n  - Child\n    - `Grandchild`\n"
        "- [Second](https://example.com)\n\nFollowing paragraph.\n\n"
        "1. First\n2. Second\n   1. Child\n3. Third\n\nFinal paragraph."
    )
    # Split inside the list to verify that message fragments are reassembled.
    split = content.index("\n") + 1
    chain = [Plain(content[:split]), Plain(content[split:])]
    if mentioned:
        chain[0].text = "\n" + chain[0].text
        chain.insert(0, At(qq="U456"))
        content = "<@U456>\n" + content
    if quoted:
        chain.insert(0, Reply(id="1.0"))
    await slack_sender(MessageChain(chain))

    slack_event.web_client.api_call.assert_awaited_once_with(
        "chat.postMessage", json={"channel": "C123", "markdown_text": content}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mentioned", [False, True])
@pytest.mark.parametrize("length", [12_000, 12_001])
async def test_slack_markdown_length_limit(slack_event, length, mentioned):
    prefix = "<@U456>" if mentioned else ""
    text = "a" * (length - len(prefix))
    chain = [At(qq="U456"), Plain(text)] if mentioned else [Plain(text)]
    await slack_event.send(MessageChain(chain))
    content = prefix + text

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
@pytest.mark.parametrize("decorated", [False, True])
async def test_slack_mixed_image_reply_preserves_markdown_order(
    slack_event, slack_sender, decorated
):
    chain = [
        Plain("- Before\n  - Nested"),
        Image.fromURL("https://example.com/image.png"),
        Plain("After."),
    ]
    prefix = "<@U456>\n" if decorated else ""
    if decorated:
        chain[0].text = "\n" + chain[0].text
        chain[:0] = [Reply(id="1.0"), At(qq="U456")]
    await slack_sender(MessageChain(chain))

    slack_event.web_client.api_call.assert_awaited_once_with(
        "chat.postMessage",
        json={
            "channel": "C123",
            "text": prefix + "- Before\n  - Nested\n\n[图片]\n\nAfter.",
            "blocks": [
                {
                    "type": "markdown",
                    "text": prefix + "- Before\n  - Nested",
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
async def test_slack_mentions_preserve_component_order(slack_event):
    await slack_event.send(
        MessageChain(
            [
                Plain("Hello "),
                At(qq="U456"),
                Plain(" and "),
                At(qq="U789"),
                Plain(".\n\n- First\n  - Child"),
            ]
        )
    )

    payload = slack_event.web_client.api_call.await_args.kwargs["json"]
    assert payload == {
        "channel": "C123",
        "markdown_text": "Hello <@U456> and <@U789>.\n\n- First\n  - Child",
    }


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
                At(qq="U456"),
                Plain("\n- Before\n  - Child"),
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
                "text": {"type": "mrkdwn", "text": "<@U456>\n- Before\n  - Child"},
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
async def test_slack_image_only_uses_markdown_only_with_mention(
    slack_event, with_mention
):
    chain = [Image.fromURL("https://example.com/image.png")]
    if with_mention:
        chain.insert(0, At(qq="U456"))
    await slack_event.send(MessageChain(chain))

    slack_event.web_client.api_call.assert_awaited_once()
    payload = slack_event.web_client.api_call.await_args.kwargs["json"]
    assert "markdown_text" not in payload
    assert [block["type"] for block in payload["blocks"]] == (
        ["markdown", "image"] if with_mention else ["image"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("with_image", [False, True])
async def test_slack_automatic_mention_through_response_pipeline(
    slack_event, monkeypatch, with_image
):
    config = deepcopy(DEFAULT_CONFIG)
    config["platform_settings"]["reply_with_mention"] = True
    config["platform_settings"]["reply_with_quote"] = True
    config["platform_settings"]["reply_prefix"] = ""
    config["platform_settings"]["segmented_reply"]["enable"] = False
    config["t2i"] = False
    config["provider_tts_settings"]["enable"] = False
    config["content_safety"]["also_use_in_response"] = False
    context = SimpleNamespace(
        astrbot_config=config,
        plugin_manager=SimpleNamespace(
            context=SimpleNamespace(
                get_using_tts_provider_async=AsyncMock(return_value=None),
            )
        ),
    )
    monkeypatch.setattr(
        "astrbot.core.pipeline.result_decorate.stage.star_handlers_registry.get_handlers_by_event_type",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        "astrbot.core.pipeline.respond.stage.call_event_hook",
        AsyncMock(return_value=False),
    )
    content = "- First\n  - Child\n\nFollowing paragraph."
    chain = [Plain(content)]
    if with_image:
        chain.extend([Image.fromURL("https://example.com/image.png"), Plain("After.")])
    slack_event.message_obj.message_id = "1.0"
    slack_event.set_result(MessageEventResult(chain=chain))
    decorate = ResultDecorateStage()
    await decorate.initialize(context)
    async for _ in decorate.process(slack_event):
        pass
    result = slack_event.get_result()
    assert any(isinstance(segment, At) for segment in result.chain)
    assert any(isinstance(segment, Reply) for segment in result.chain)
    respond = RespondStage()
    respond.platform_settings = {}
    respond.enable_seg = False
    await respond.process(slack_event)

    slack_event.web_client.api_call.assert_awaited_once()
    payload = slack_event.web_client.api_call.await_args.kwargs["json"]
    expected = "<@U123>\n" + content
    if with_image:
        assert [block["type"] for block in payload["blocks"]] == [
            "markdown",
            "image",
            "markdown",
        ]
        assert payload["blocks"][0]["text"] == expected
        assert payload["blocks"][2]["text"] == "After."
    else:
        assert payload == {"channel": "C123", "markdown_text": expected}
