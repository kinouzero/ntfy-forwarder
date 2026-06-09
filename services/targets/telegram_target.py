import asyncio

from core.config import TG_ADMIN, TELEGRAM_MAX_MESSAGE_LENGTH
from core.metrics import telegram_send_seconds
from services.telegram import tg_call
from utils.telegram import split_message


def telegram_disable_notification(priority):
    # Telegram only supports "silent" vs "normal".
    # Map ntfy priorities 1-2 to silent, 3-5 to normal notifications.
    return int(priority or 3) <= 2


async def send_telegram_message(message, attachment_url=None, priority=3):
    disable_notification = telegram_disable_notification(priority)
    for chunk in split_message(message, TELEGRAM_MAX_MESSAGE_LENGTH):
        payload = {
            "chat_id": TG_ADMIN,
            "text": chunk,
            "parse_mode": "MarkdownV2",
            "disable_notification": disable_notification,
        }
        start = asyncio.get_event_loop().time()
        await tg_call("sendMessage", payload)
        telegram_send_seconds.observe(asyncio.get_event_loop().time() - start)

    if attachment_url:
        await tg_call(
            "sendDocument",
            {
                "chat_id": TG_ADMIN,
                "document": attachment_url,
                "disable_notification": disable_notification,
            },
        )
