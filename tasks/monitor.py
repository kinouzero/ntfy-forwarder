import asyncio
import time

from core.state import workers, worker_last_seen, shutdown_event
from services.ntfy import ntfy_worker
from core.logging import log


async def worker_monitor_loop():
    while not shutdown_event.is_set():
        now = int(time.time())
        for topic, task in list(workers.items()):
            last_seen = worker_last_seen.setdefault(topic, now)
            if task.done() or now - last_seen > 120:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                worker_last_seen[topic] = now
                workers[topic] = asyncio.create_task(ntfy_worker(topic))
                log("WARN", "monitor restarted worker", topic=topic)
        await asyncio.sleep(30)
