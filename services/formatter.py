from utils.markdown import escape_md

def priority_icon(priority):

    mapping = {
        1: "⚪",
        2: "🔵",
        3: "🟢",
        4: "⚠️",
        5: "🚨",
    }

    return mapping.get(priority, "🟢")

def build_message(event):

    icon = priority_icon(
        event.priority
    )

    parts = [
        f"{icon} *{escape_md(event.topic)}*"
    ]

    if getattr(event, "title", None):

        parts.append(
            f"*{escape_md(event.title)}*"
        )

    parts.append(
        escape_md(event.message)
    )

    attachment = getattr(
        event,
        "attachment",
        None,
    )

    if attachment:

        url = attachment.get("url")

        if url:

            parts.append(
                f"📎 {escape_md(url)}"
            )

    if getattr(event, "tags", None):

        parts.append(
            "Tags: "
            + ", ".join(
                escape_md(tag)
                for tag in event.tags
            )
        )

    return "\n\n".join(parts)
