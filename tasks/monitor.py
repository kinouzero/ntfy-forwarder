import asyncio
import time

from core.state import (
    workers,
    worker_last_seen,
    shutdown_event,
)

from services.ntfy import ntfy_worker
from core.logging import log

async def worker_monitor_loop():

    while not shutdown_event.is_set():

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
                log("WARN", "monitor restarted stale worker", topic=topic)

        await asyncio.sleep(30)
