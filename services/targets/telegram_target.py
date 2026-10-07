import asyncio

from core.metrics import telegram_send_seconds
from services.telegram import TelegramAPIError, tg_call
from utils.telegram import split_message


def telegram_disable_notification(priority):
    # Telegram only supports "silent" vs "normal".
    # Map ntfy priorities 1-2 to silent, 3-5 to normal notifications.
    return int(priority or 3) <= 2


async def send_telegram_message(
    message,
    attachment_url=None,
    priority=3,
    chat_id=None,
    bot_token=None,
    max_message_length=4096,
):
    chat_id = str(chat_id or "").strip()
    if not chat_id:
        raise TelegramAPIError("missing telegram chat_id", status_code=400, retryable=False)
    bot_token = str(bot_token or "").strip()
    if not bot_token:
        raise TelegramAPIError(
            "missing telegram bot_token",
            status_code=400,
            retryable=False,
        )
    disable_notification = telegram_disable_notification(priority)
    for chunk in split_message(message, int(max_message_length or 4096)):
        payload = {
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": "MarkdownV2",
            "disable_notification": disable_notification,
        }
        start = asyncio.get_event_loop().time()
        try:
            await tg_call("sendMessage", payload, token=bot_token)
        except TelegramAPIError as exc:
            # Fallback for malformed MarkdownV2 payloads: send plain text.
            if exc.status_code == 400 and "can't parse entities" in str(exc).lower():
                plain_payload = {
                    "chat_id": chat_id,
                    "text": chunk,
                    "disable_notification": disable_notification,
                }
                await tg_call("sendMessage", plain_payload, token=bot_token)
            else:
                raise
        telegram_send_seconds.observe(asyncio.get_event_loop().time() - start)

    if attachment_url:
        await tg_call(
            "sendDocument",
            {
                "chat_id": chat_id,
                "document": attachment_url,
                "disable_notification": disable_notification,
            },
            token=bot_token,
        )
