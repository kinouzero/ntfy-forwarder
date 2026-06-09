from core.config import SLACK_WEBHOOK_URL

from services.targets.common import post_json, join_message


async def send_slack_message(message, attachment_url=None, priority=3):
    await post_json(
        "slack",
        SLACK_WEBHOOK_URL,
        {"text": join_message(message, attachment_url)},
    )
