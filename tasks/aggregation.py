import asyncio

from core.state import (
    aggregation_buffer,
    shutdown_event,
)

from services.formatter import build_message
from services.queue import enqueue_telegram
from utils.markdown import escape_md
from db.topics import is_topic_enabled
from db.settings import get_settings_snapshot
from core.logging import log

async def aggregation_loop():

    while not shutdown_event.is_set():
        settings = await get_settings_snapshot()
        interval = int(settings["aggregation_interval"])
        min_count = int(settings["aggregation_min_count"])

        await asyncio.sleep(
            interval
        )

        for topic, events in list(
            aggregation_buffer.items()
        ):

            if not events:
                continue

            if not await is_topic_enabled(topic):
                log(
                    "INFO",
                    "aggregation dropped for disabled topic",
                    topic=topic,
                    count=len(events),
                )
                aggregation_buffer[topic].clear()
                continue

            if len(events) < min_count:

                for event in events:

                    await enqueue_telegram(
                        {
                            "topic": topic,
                            "message": build_message(event),
                            "priority": int(getattr(event, "priority", 3) or 3),
                            "attachment_url": (
                                (event.attachment or {}).get("url")
                                if getattr(event, "attachment", None)
                                else None
                            ),
                        }
                    )

            else:

                await enqueue_telegram(
                    {
                        "topic": topic,
                        "message": (
                            f"⚠️ {escape_md(topic)}: "
                            f"{len(events)} events"
                        ),
                        "priority": 4,
                    }
                )

            aggregation_buffer[
                topic
            ].clear()
