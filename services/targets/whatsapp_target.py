from core.config import (
    WHATSAPP_API_BASE,
    WHATSAPP_API_VERSION,
    WHATSAPP_PHONE_NUMBER_ID,
    WHATSAPP_ACCESS_TOKEN,
    WHATSAPP_TO,
)

from services.targets.common import post_json, join_message


async def send_whatsapp_message(message, attachment_url=None, priority=3):
    body = join_message(message, attachment_url)[:3900]
    url = (
        f"{WHATSAPP_API_BASE}/{WHATSAPP_API_VERSION}/"
        f"{WHATSAPP_PHONE_NUMBER_ID}/messages"
    )
    await post_json(
        "whatsapp",
        url,
        {
            "messaging_product": "whatsapp",
            "to": WHATSAPP_TO,
            "type": "text",
            "text": {
                "preview_url": False,
                "body": body,
            },
        },
        headers={
            "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        },
    )
