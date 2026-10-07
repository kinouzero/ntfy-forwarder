import math

import aiohttp

from core.http import get_http_session


class DeliveryError(RuntimeError):
    def __init__(
        self,
        channel,
        description,
        *,
        status_code=None,
        retry_after=None,
        retryable=True,
    ):
        super().__init__(description)
        self.channel = channel
        self.status_code = status_code
        self.retry_after = retry_after
        self.retryable = retryable


def join_message(message, attachment_url=None):
    parts = [str(message or "").strip()]
    if attachment_url:
        parts.append(f"Attachment: {attachment_url}")
    return "\n".join(p for p in parts if p)


async def post_json(channel, url, payload, headers=None):
    session = get_http_session()
    if session is None:
        raise DeliveryError(channel, "HTTP session not ready", retryable=True)
    req_headers = dict(headers or {})
    try:
        async with session.post(url, json=payload, headers=req_headers, timeout=60) as resp:
            body = await resp.text()
            if resp.status >= 400:
                retryable = resp.status == 429 or resp.status >= 500
                try:
                    retry_after = max(1, math.ceil(float(resp.headers.get("Retry-After", "0"))))
                except (ValueError, OverflowError):
                    retry_after = None
                raise DeliveryError(
                    channel,
                    f"{channel} HTTP {resp.status}: {body[:300]}",
                    status_code=resp.status,
                    retryable=retryable,
                    retry_after=retry_after if resp.headers.get("Retry-After") else None,
                )
    except aiohttp.ClientError as exc:
        raise DeliveryError(channel, str(exc), retryable=True) from exc
