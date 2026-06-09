import asyncio
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from core.config import (
    DAILY_SUMMARY_ENABLED,
    DAILY_SUMMARY_HOUR,
    DAILY_SUMMARY_MINUTE,
    TZ,
)
from core.state import shutdown_event
from core.logging import log
from db.topics import list_topics, list_topic_status_counts
from db.messages import count_messages_by_topic_since
from db.errors import count_errors_since
from db.telegram_queue import count_telegram_queue
from db.dead_letter import count_dead_letters
from services.queue import enqueue_telegram
from utils.markdown import escape_md


def _summary_tz():
    try:
        return ZoneInfo(TZ)
    except Exception:
        return ZoneInfo("UTC")


def _is_summary_time(now):
    return (
        now.hour == DAILY_SUMMARY_HOUR
        and now.minute == DAILY_SUMMARY_MINUTE
    )


async def _build_daily_summary():
    now_ts = int(time.time())
    since = now_ts - 86400

    topics = await list_topics()
    status_counts = await list_topic_status_counts()
    counts_24h = await count_messages_by_topic_since(since)
    queue_count = await count_telegram_queue()
    dead_count = await count_dead_letters()
    error_count = await count_errors_since(since)

    top = sorted(
        counts_24h.items(),
        key=lambda x: x[1],
        reverse=True,
    )[:5]
    top_lines = []
    for name, count in top:
        top_lines.append(f"- {escape_md(name)}: {count}")
    if not top_lines:
        top_lines = ["- no events"]

    total_24h = sum(counts_24h.values())
    enabled = sum(1 for t in topics if bool(t["enabled"]))
    disabled = len(topics) - enabled
    filtered = sum(v.get("filtered", 0) for v in status_counts.values())
    disabled_dropped = sum(v.get("disabled", 0) for v in status_counts.values())
    rate_limited = sum(v.get("rate_limited", 0) for v in status_counts.values())

    return (
        "📊 *Daily Summary*\\n\\n"
        f"Total 24h: *{total_24h}*\\n"
        f"Topics: *{len(topics)}* \\(enabled: {enabled}, disabled: {disabled}\\)\\n"
        f"Filtered: *{filtered}*\\n"
        f"Dropped \\(disabled\\): *{disabled_dropped}*\\n"
        f"Rate limited: *{rate_limited}*\\n"
        f"Errors 24h: *{error_count}*\\n"
        f"Queue: *{queue_count}*\\n"
        f"Dead letters: *{dead_count}*\\n\\n"
        "*Top topics \\(24h\\)*\\n"
        + "\\n".join(top_lines)
    )


async def daily_summary_loop():
    if not DAILY_SUMMARY_ENABLED:
        log("INFO", "daily summary disabled")
        return

    tz = _summary_tz()
    last_sent_date = None

    while not shutdown_event.is_set():
        now = datetime.now(tz)
        day_key = now.date().isoformat()
        if _is_summary_time(now) and day_key != last_sent_date:
            try:
                summary = await _build_daily_summary()
                await enqueue_telegram(
                    {
                        "topic": "__system__",
                        "message": summary,
                        "priority": 2,
                    }
                )
                last_sent_date = day_key
                log("INFO", "daily summary sent", day=day_key)
            except Exception as exc:
                log("WARN", "daily summary failed", error=str(exc))

        await asyncio.sleep(30)
