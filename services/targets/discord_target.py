from core.config import DISCORD_WEBHOOK_URL

from services.targets.common import post_json, join_message


async def send_discord_message(message, attachment_url=None, priority=3):
    await post_json(
        "discord",
        DISCORD_WEBHOOK_URL,
        {"content": join_message(message, attachment_url)},
    )
