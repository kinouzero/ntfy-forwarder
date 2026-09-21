import asyncio
import signal

from aiohttp import web

from core.logging import log
from core.state import (
    shutdown_event,
    workers,
)

from core.http import (
    create_http_session,
    close_http_session,
)

from core.config import (
    NTFY_BASE_URL,
    LOG_LEVEL,
    TZ,
    ACTIVE_TARGETS,
)

from db.schema import init_db
from db.topics import list_topics

from services.ntfy import ntfy_worker
from services.plugins import load_plugins

from tasks.aggregation import aggregation_loop
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
    if not NTFY_BASE_URL:
        raise RuntimeError("NTFY_BASE_URL is not set")


async def bootstrap_topics():
    rows = await list_topics()
    topics = [str(r["name"]).strip() for r in rows if str(r["name"]).strip()]
    for topic in topics:

        task = asyncio.create_task(
            ntfy_worker(topic)
        )

        workers[topic] = task

        running_tasks.append(task)

async def shutdown():

    shutdown_event.set()

    for task in running_tasks:
        task.cancel()

    await asyncio.gather(
        *running_tasks,
        return_exceptions=True,
    )

    await close_http_session()


async def main():

    log(
        "INFO",
        "starting forwarder",
    )
    log("INFO", "config", log_level=LOG_LEVEL, tz=TZ)

    validate_config()

    await init_db()

    await create_http_session()

    await load_plugins()

    await bootstrap_topics()

    background_tasks = []

    background_tasks.append(
        asyncio.create_task(
            delivery_sender_loop()
        )
    )
    background_tasks.append(
        asyncio.create_task(
            daily_summary_loop()
        )
    )

    background_tasks.extend(
        [
            asyncio.create_task(
                aggregation_loop()
            ),
            asyncio.create_task(
                digest_loop()
            ),
            asyncio.create_task(
                retention_loop()
            ),
            asyncio.create_task(
                backup_loop()
            ),
            asyncio.create_task(
                worker_monitor_loop()
            ),
            asyncio.create_task(
                db_maintenance_loop()
            ),
        ]
    )

    app = await create_web_app()

    runner = web.AppRunner(app)

    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        8081,
    )

    await site.start()

    await shutdown_event.wait()

    await shutdown()

    for task in background_tasks:
        task.cancel()

    await asyncio.gather(
        *background_tasks,
        return_exceptions=True,
    )

    await runner.cleanup()

def stop():
    shutdown_event.set()

def run():
    loop = asyncio.new_event_loop()

    asyncio.set_event_loop(loop)

    for sig in (
        signal.SIGTERM,
        signal.SIGINT,
    ):
        loop.add_signal_handler(
            sig,
            stop,
        )

    try:
        loop.run_until_complete(main())

    finally:
        loop.close()


if __name__ == "__main__":
    run()
