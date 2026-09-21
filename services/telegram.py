import aiohttp

from core.http import get_http_session


class TelegramAPIError(RuntimeError):
    def __init__(
        self,
        description,
        *,
        status_code=None,
        error_code=None,
        retry_after=None,
        retryable=True,
    ):
        super().__init__(description)
        self.status_code = status_code
        self.error_code = error_code
        self.retry_after = retry_after
        self.retryable = retryable


async def tg_call(method, payload, token):
    token = str(token or "").strip()
    if not token:
        raise RuntimeError("telegram bot token is missing")
    tg_api = f"https://api.telegram.org/bot{token}"

    session = get_http_session()
    if session is None:
        raise RuntimeError("HTTP session not ready")

    try:
        async with session.post(
            f"{tg_api}/{method}",
            json=payload,
            timeout=60,
        ) as resp:
            data = await resp.json()
            if resp.status >= 500:
                raise TelegramAPIError(
                    data.get("description", "telegram server error"),
                    status_code=resp.status,
                    error_code=data.get("error_code"),
                    retryable=True,
                )
            if not data.get("ok", False):
                params = data.get("parameters", {}) or {}
                retry_after = params.get("retry_after")
                status_code = resp.status
                description = data.get("description", "telegram api error")
                retryable = False
                if status_code == 429:
                    retryable = True
                elif status_code >= 500:
                    retryable = True
                raise TelegramAPIError(
                    description,
                    status_code=status_code,
                    error_code=data.get("error_code"),
                    retry_after=retry_after,
                    retryable=retryable,
                )
            return data
    except aiohttp.ClientError as exc:
        raise TelegramAPIError(
            str(exc),
            retryable=True,
        ) from exc
