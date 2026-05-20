import asyncio

from core.config import (
    AGGREGATION_INTERVAL,
    AGGREGATION_MIN_COUNT,
)

from core.state import (
    aggregation_buffer,
    telegram_queue,
)

from services.formatter import build_message
from utils.markdown import escape_md

async def aggregation_loop():

    while True:

        await asyncio.sleep(
            AGGREGATION_INTERVAL
        )

        for topic, events in list(
            aggregation_buffer.items()
        ):

            if not events:
                continue

            if len(events) < AGGREGATION_MIN_COUNT:

                for event in events:

                    await telegram_queue.put(
                        build_message(event)
                    )

            else:

                await telegram_queue.put(
                    f"⚠️ {escape_md(topic)}: "
                    f"{len(events)} events"
                )

            aggregation_buffer[
                topic
            ].clear()
