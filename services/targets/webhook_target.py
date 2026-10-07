from services.targets.common import post_json, join_message


def webhook_payload(payload_type, message, attachment_url=None, priority=3):
    if payload_type == "discord":
        return {"content": join_message(message, attachment_url)}
    if payload_type == "slack":
        return {"text": join_message(message, attachment_url)}
    return {
        "message": message,
        "attachment_url": attachment_url,
        "priority": int(priority or 3),
    }


async def send_webhook_message(
    url,
    payload_type,
    message,
    attachment_url=None,
    priority=3,
    auth_header=None,
):
    headers = None
    if auth_header:
        headers = {"Authorization": auth_header}
    await post_json(
        f"webhook_{payload_type}",
        url,
        webhook_payload(payload_type, message, attachment_url, priority),
        headers=headers,
    )
