import asyncio

from core.state import (
    digest_buffer,
    shutdown_event,
)
from services.queue import enqueue_telegram
from utils.markdown import escape_md
from db.topics import is_topic_enabled
from core.logging import log

async def digest_loop():

    while not shutdown_event.is_set():

        await asyncio.sleep(600)

        for key, count in list(digest_buffer.items()):

            if not await is_topic_enabled(key):
                log(
                    "INFO",
                    "digest dropped for disabled topic",
                    topic=key,
                    count=count,
                )
                continue

            if count > 10:

                await enqueue_telegram(
                    {
                        "topic": key,
                        "message": (
                            f"⚠️ {escape_md(key)}: "
                            f"{count} events in 10m"
                        ),
                        "priority": 4,
                    }
                )

        digest_buffer.clear()
