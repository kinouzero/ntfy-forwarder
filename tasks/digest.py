import asyncio

from core.state import (
    telegram_queue,
    digest_buffer,
)
from utils.markdown import escape_md

async def digest_loop():

    while True:

        await asyncio.sleep(600)

        for key, count in list(digest_buffer.items()):

            if count > 10:

                await telegram_queue.put(
                    f"⚠️ {escape_md(key)}: "
                    f"{count} events in 10m"
                )

        digest_buffer.clear()
