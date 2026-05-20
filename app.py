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
    BOOTSTRAP_TOPICS,
    TG_TOKEN,
    TG_ADMIN,
    TELEGRAM_ENABLED,
    NTFY_BASE_URL,
    TOPIC_ALLOWLIST,
    TOPIC_DENYLIST,
    ADMIN_WEB_URL,
    SEND_ADMIN_LINK_ON_START,
    LOG_LEVEL,
    TZ,
)

from db.schema import init_db
from db.topics import add_topic, get_topic

from services.ntfy import ntfy_worker
from services.plugins import load_plugins

from tasks.aggregation import aggregation_loop
from tasks.telegram_sender import telegram_sender_loop
from tasks.digest import digest_loop
from tasks.retention import retention_loop
from tasks.backup import backup_loop
from tasks.monitor import worker_monitor_loop

from api.web import create_web_app
from services.telegram import tg_call

running_tasks = []

def validate_config():
    if not NTFY_BASE_URL:
        raise RuntimeError("NTFY_BASE_URL is not set")

    if TELEGRAM_ENABLED:
        if not TG_TOKEN or not TG_ADMIN:
            raise RuntimeError(
                "TELEGRAM_BOT_TOKEN or TELEGRAM_ADMIN_CHAT_ID not set"
            )


def topic_allowed(topic):
    if TOPIC_ALLOWLIST and topic not in TOPIC_ALLOWLIST:
        return False
    if TOPIC_DENYLIST and topic in TOPIC_DENYLIST:
        return False
    return True

async def bootstrap_topics():

    topics = [
        t.strip()
        for t in BOOTSTRAP_TOPICS.split(",")
        if t.strip()
    ]

    for topic in topics:

        if not topic_allowed(topic):
            log("WARN", "topic skipped", topic=topic)
            continue

        existing = await get_topic(topic)
        if existing and not bool(existing["enabled"]):
            log("WARN", "topic disabled", topic=topic)
            continue

        await add_topic(topic)

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

    if not TELEGRAM_ENABLED:
        log(
            "WARN",
            "telegram disabled",
        )

    await load_plugins()

    await bootstrap_topics()

    background_tasks = []

    if TELEGRAM_ENABLED:
        background_tasks.append(
            asyncio.create_task(
                telegram_sender_loop()
            )
        )
        if ADMIN_WEB_URL and SEND_ADMIN_LINK_ON_START:
            payload = {
                "chat_id": TG_ADMIN,
                "text": "Open admin panel",
                "reply_markup": {
                    "inline_keyboard": [
                        [
                            {
                                "text": "Open Admin",
                                "web_app": {
                                    "url": ADMIN_WEB_URL,
                                },
                            }
                        ]
                    ]
                },
            }
            asyncio.create_task(
                tg_call("sendMessage", payload)
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
