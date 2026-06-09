import asyncio

from core.config import (
    AGGREGATION_INTERVAL,
    AGGREGATION_MIN_COUNT,
)

from core.state import (
    aggregation_buffer,
    shutdown_event,
)

from services.formatter import build_message
from services.queue import enqueue_telegram
from utils.markdown import escape_md
from db.topics import is_topic_enabled
from core.logging import log

async def aggregation_loop():

    while not shutdown_event.is_set():

        await asyncio.sleep(
            AGGREGATION_INTERVAL
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

            if len(events) < AGGREGATION_MIN_COUNT:

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
