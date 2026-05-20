from core.http import get_http_session
from core.config import TG_TOKEN

TG_API = (
    f"https://api.telegram.org/bot{TG_TOKEN}"
    if TG_TOKEN
    else None
)


async def tg_call(method, payload):

    if TG_API is None:
        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is not set"
        )

    session = get_http_session()
    if session is None:
        raise RuntimeError("HTTP session not ready")

    async with session.post(
        f"{TG_API}/{method}",
        json=payload,
        timeout=60,
    ) as resp:

        resp.raise_for_status()

        return await resp.json()
