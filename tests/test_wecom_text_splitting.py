import json
from unittest.mock import AsyncMock

import pytest
from requests import Response
from wechatpy.enterprise import WeChatClient

from astrbot.api.event import MessageChain
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    PlatformMetadata,
)
from astrbot.core.platform.sources.wecom.wecom_event import WecomPlatformEvent
from astrbot.core.platform.sources.wecom.wecom_kf_message import WeChatKFMessage


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("", [""]),
        ("a" * 2048, ["a" * 2048]),
        ("a" * 4096, ["a" * 2048, "a" * 2048]),
        ("中" * 682, ["中" * 682]),
        ("中" * 683, ["中" * 682, "中"]),
        ("中" * 2048, ["中" * 682] * 3 + ["中" * 2]),
        ("中" * 4096, ["中" * 682] * 6 + ["中" * 4]),
        ("é" * 1025, ["é" * 1024, "é"]),
        ("😀" * 513, ["😀" * 512, "😀"]),
        ("a" * 2 + "中" * 682, ["a" * 2 + "中" * 682]),
        ("a" * 3 + "中" * 682, ["a" * 3 + "中" * 681, "中"]),
        ("中" * 680 + "。" + "文" * 10, ["中" * 680 + "。", "文" * 10]),
        ("a" * 2047 + "\nend", ["a" * 2047 + "\n", "end"]),
        ("a" * 2047 + "😀。", ["a" * 2047, "😀。"]),
    ],
    ids=[
        "empty",
        "ascii-boundary",
        "ascii-long",
        "han-below-limit",
        "han-over-limit",
        "han-2048-chars",
        "han-long",
        "two-byte",
        "emoji",
        "mixed-boundary",
        "mixed-over-limit",
        "punctuation",
        "newline-boundary",
        "split-before-emoji",
    ],
)
async def test_split_plain_respects_utf8_limit(text, expected):
    event = object.__new__(WecomPlatformEvent)

    chunks = await event.split_plain(text)

    assert chunks == expected
    assert "".join(chunks) == text
    assert all(len(chunk.encode("utf-8")) <= 2048 for chunk in chunks)


@pytest.mark.asyncio
@pytest.mark.parametrize("customer_service", [False, True], ids=["application", "kf"])
@pytest.mark.parametrize(
    "text",
    ["中" * 683, "中" * 3200, "Hello 中文😀。\n" * 300, ("中" * 399 + "。") * 8],
    ids=["unsplit-han", "long-han", "mixed", "punctuation-quota"],
)
async def test_send_preserves_text_in_byte_limited_requests(
    monkeypatch, customer_service, text
):
    client = WeChatClient("test-corp", "test-secret", access_token="test-token")
    if customer_service:
        client.kf_message = WeChatKFMessage(client)
    message = AstrBotMessage()
    message.type = MessageType.FRIEND_MESSAGE
    message.self_id = "100001"
    message.session_id = "alice"
    message.sender = MessageMember("alice", "Alice")
    event = WecomPlatformEvent(
        "", message, PlatformMetadata("wecom", "WeCom", "test-wecom"), "alice", client
    )
    sent = []

    def capture_request(request, **kwargs):
        """Validate the serialized SDK payload without contacting WeCom.

        Args:
            request: Prepared HTTP request from the real SDK.
            **kwargs: Requests transport options.

        Returns:
            A synthetic successful API response.
        """
        endpoint = "kf/send_msg" if customer_service else "message/send"
        assert request.url.startswith(
            f"https://qyapi.weixin.qq.com/cgi-bin/{endpoint}?"
        )
        payload = json.loads(request.body)
        content = payload["text"]["content"]
        assert payload["msgtype"] == "text"
        assert payload["touser"] == "alice"
        assert len(content.encode("utf-8")) <= 2048
        if customer_service:
            assert len(sent) < 5, "WeChat customer service allows five replies"
        sent.append(content)
        response = Response()
        response.status_code = 200
        response.request = request
        response._content = b'{"errcode": 0, "errmsg": "ok"}'
        return response

    monkeypatch.setattr(client._http, "send", capture_request)
    monkeypatch.setattr("asyncio.sleep", AsyncMock())
    monkeypatch.setattr(
        "astrbot.core.platform.astr_message_event.Metric.upload", AsyncMock()
    )

    await event.send(MessageChain().message(text))

    assert len(sent) > 1
    assert "".join(sent) == text
