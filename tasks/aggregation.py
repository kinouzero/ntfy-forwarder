import asyncio

from core.state import aggregation_buffer, shutdown_event
from services.formatter import build_message
from services.queue import enqueue_telegram
from utils.markdown import escape_md
from db.topics import is_topic_enabled
from db.settings import get_settings_snapshot
from core.logging import log


async def flush_aggregation():
    settings = await get_settings_snapshot()
    min_count = int(settings["aggregation_min_count"])
    for topic in list(aggregation_buffer):
        # Detach the batch before any await: receivers may append concurrently.
        events = aggregation_buffer[topic]
        aggregation_buffer[topic] = []
        if not events:
            continue
        try:
            if not await is_topic_enabled(topic):
                log("INFO", "aggregation dropped for disabled topic", topic=topic,
                    count=len(events))
                continue
            if len(events) >= min_count:
                await enqueue_telegram({
                    "topic": topic,
                    "message": f"⚠️ {escape_md(topic)}: {len(events)} events",
                    "priority": 4,
                })
                events.clear()
            else:
                while events:
                    event = events[0]
                    await enqueue_telegram({
                        "topic": topic,
                        "message": build_message(event),
                        "priority": int(event.priority or 3),
                        "attachment_url": (event.attachment or {}).get("url"),
                    })
                    events.pop(0)
        except BaseException:
            aggregation_buffer[topic] = events + aggregation_buffer.get(topic, [])
            raise


async def aggregation_loop():
    while not shutdown_event.is_set():
        try:
            settings = await get_settings_snapshot()
            await asyncio.sleep(int(settings["aggregation_interval"]))
            await flush_aggregation()
        except Exception as exc:
            log("ERROR", "aggregation failed; pending messages retained", error=str(exc))
            await asyncio.sleep(1)
