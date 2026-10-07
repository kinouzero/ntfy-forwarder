import asyncio
import signal
from urllib.parse import urlsplit

from aiohttp import web

from core.logging import log
from core.state import shutdown_event, workers
from core.http import create_http_session, close_http_session
from core.config import NTFY_BASE_URL, LOG_LEVEL, TZ
from db.schema import init_db
from db.topics import list_topics
from services.ntfy import ntfy_worker
from services.plugins import load_plugins
from tasks.aggregation import aggregation_loop, flush_aggregation
from tasks.delivery_sender import delivery_sender_loop
from tasks.daily_summary import daily_summary_loop
from tasks.digest import digest_loop
from tasks.retention import retention_loop
from tasks.backup import backup_loop
from tasks.monitor import worker_monitor_loop
from tasks.db_maintenance import db_maintenance_loop
from api.web import create_web_app

running_tasks = []


def validate_config():
    url = urlsplit(NTFY_BASE_URL)
    if url.scheme not in {"http", "https"} or not url.hostname:
        raise RuntimeError("NTFY_BASE_URL must be an HTTP(S) URL")


async def bootstrap_topics():
    for row in await list_topics():
        topic = str(row["name"]).strip()
        if topic and (topic not in workers or workers[topic].done()):
            workers[topic] = asyncio.create_task(ntfy_worker(topic))


async def shutdown():
    shutdown_event.set()
    # Include workers created by the UI or replaced by the monitor.
    tasks = set(running_tasks) | set(workers.values())
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    running_tasks.clear()
    workers.clear()
    await close_http_session()


async def main():
    log("INFO", "starting forwarder")
    log("INFO", "config", log_level=LOG_LEVEL, tz=TZ)
    validate_config()
    shutdown_event.clear()
    runner = None
    database_ready = False
    try:
        await init_db()
        database_ready = True
        await create_http_session()
        await load_plugins()
        await bootstrap_topics()
        for worker in (
            delivery_sender_loop, daily_summary_loop, aggregation_loop,
            digest_loop, retention_loop, backup_loop, worker_monitor_loop,
            db_maintenance_loop,
        ):
            running_tasks.append(asyncio.create_task(worker(), name=worker.__name__))
        runner = web.AppRunner(await create_web_app())
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", 8081).start()
        waiter = asyncio.create_task(shutdown_event.wait(), name="shutdown_waiter")
        running_tasks.append(waiter)
        done, _ = await asyncio.wait(running_tasks, return_when=asyncio.FIRST_COMPLETED)
        if not shutdown_event.is_set():
            task = next(iter(done))
            raise RuntimeError(f"background task stopped: {task.get_name()}") from task.exception()
    finally:
        try:
            if runner is not None:
                await runner.cleanup()
        finally:
            await shutdown()
            if database_ready:
                await flush_aggregation()


def stop():
    shutdown_event.set()


def run():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(loop.shutdown_default_executor())
        loop.close()
        asyncio.set_event_loop(None)


if __name__ == "__main__":
    run()
