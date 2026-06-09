from core.config import GENERIC_WEBHOOK_URL, GENERIC_WEBHOOK_AUTH_HEADER

from services.targets.common import post_json


async def send_generic_webhook_message(message, attachment_url=None, priority=3):
    await post_json(
        "webhook",
        GENERIC_WEBHOOK_URL,
        {
            "message": message,
            "attachment_url": attachment_url,
            "priority": int(priority or 3),
        },
        headers={
            "Authorization": GENERIC_WEBHOOK_AUTH_HEADER,
        } if GENERIC_WEBHOOK_AUTH_HEADER else None,
    )
