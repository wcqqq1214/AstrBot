"""Split Slack Markdown without discarding source whitespace."""

import re


def split_slack_markdown(text: str) -> list[str]:
    """Keep chunks within Slack's 12k limit, reopening split code fences.

    Prefer paragraph and line boundaries so ordinary lists stay together.
    A single paragraph or list exceeding the limit still has to be split.

    Args:
        text: Complete Markdown or the currently buffered stream segment.

    Returns:
        Ordered chunks containing every source character, plus any synthetic
        closing and reopening fences needed at message boundaries.
    """
    limit = 12_000
    if len(text) <= limit:
        return [text]

    # Record source positions before adding synthetic fences. Only a matching,
    # sufficiently long fence with no info string closes an existing block.
    fences = []
    opened = None
    for match in re.finditer(r"(?m)^ {0,3}(`{3,}|~{3,})([^\r\n]*)(?:\r?\n|$)", text):
        marker, info = match.groups()
        if opened is None:
            if marker[0] == "`" and "`" in info:
                continue
            opened = (
                match.start(),
                match.end(),
                marker,
                match.group().rstrip("\r\n") + "\n",
            )
        elif (
            marker[0] == opened[2][0]
            and len(marker) >= len(opened[2])
            and not info.strip()
        ):
            fences.append((*opened, match.start(), match.end()))
            opened = None
    if opened is not None:
        fences.append((*opened, len(text), len(text)))

    chunks = []
    start = 0
    prefix = ""
    while start < len(text):
        budget = limit - len(prefix)
        while True:
            end = min(start + budget, len(text))
            if end < len(text):
                # Keep the separator in the preceding message; never lstrip
                # the continuation, which may start with meaningful indentation.
                for separator in ("\r\n\r\n", "\n\n", "\n", " "):
                    lower = start if len(separator) > 1 else start + budget // 2
                    boundary = text.rfind(separator, lower, end)
                    if boundary >= 0 and (prefix or text[start:boundary].strip()):
                        end = boundary + len(separator)
                        break
            suffix = continuation = ""
            for opening, body_start, marker, opener, closing, block_end in fences:
                # Leave complete opening/closing fence lines together when possible.
                if opening < end < body_start and opening > start:
                    end = opening
                elif closing < end < block_end and closing > start:
                    end = closing
                # Only seal actual message boundaries. The last chunk may still
                # receive streaming deltas, so its source fence must stay open.
                if body_start <= end <= closing and opening < end < len(text):
                    # An exceptionally long fence cannot be reopened within the
                    # payload limit; keep its source rather than stall splitting.
                    if len(opener) + len(marker) + 2 < limit // 2:
                        suffix = ("" if text[end - 1] == "\n" else "\n") + marker
                        continuation = opener
                    break
            overflow = len(prefix) + end - start + len(suffix) - limit
            if overflow <= 0:
                break
            budget = end - start - overflow
        chunks.append(prefix + text[start:end] + suffix)
        start, prefix = end, continuation
    return chunks
