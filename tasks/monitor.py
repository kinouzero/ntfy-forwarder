import asyncio
import time

from core.state import (
    workers,
    worker_last_seen,
)

from services.ntfy import ntfy_worker

async def worker_monitor_loop():

    while True:

        now = int(time.time())

        for topic, ts in list(
            worker_last_seen.items()
        ):

            if now - ts > 120:

                try:
                    workers[topic].cancel()
                except Exception:
                    pass

                workers[topic] = asyncio.create_task(
                    ntfy_worker(topic)
                )

        await asyncio.sleep(30)
