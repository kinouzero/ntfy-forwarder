import asyncio
import traceback

from core.state import (
    telegram_queue,
    shutdown_event,
)

from core.config import TG_ADMIN

from services.telegram import tg_call

from db.errors import log_error

from core.metrics import (
    telegram_messages_sent_total,
    telegram_messages_failed_total,
    telegram_queue_size,
    worker_errors_total,
    telegram_send_seconds,
)
from core.config import TELEGRAM_MAX_MESSAGE_LENGTH
from utils.telegram import split_message

RATE_LIMIT_DELAY = 0.05
MAX_RETRIES = 5


async def send_telegram_message(message):

    for chunk in split_message(
        message,
        TELEGRAM_MAX_MESSAGE_LENGTH,
    ):
        payload = {
            "chat_id": TG_ADMIN,
            "text": chunk,
            "parse_mode": "MarkdownV2",
        }

        start = asyncio.get_event_loop().time()
        await tg_call(
            "sendMessage",
            payload,
        )
        telegram_send_seconds.observe(
            asyncio.get_event_loop().time() - start
        )


async def process_message(message):

    for retry in range(MAX_RETRIES):
        try:

            await send_telegram_message(
                message
            )

            await asyncio.sleep(
                RATE_LIMIT_DELAY
            )

            telegram_messages_sent_total.inc()
            return True

        except Exception:

            if retry == MAX_RETRIES - 1:

                await log_error(
                    "telegram_sender",
                    None,
                    traceback.format_exc(),
                )

                telegram_messages_failed_total.inc()
                worker_errors_total.labels(
                    component="telegram_sender"
                ).inc()
                return False

            await asyncio.sleep(
                2 ** retry
            )


async def telegram_sender_loop():

    while not shutdown_event.is_set():

        try:

            message = await telegram_queue.get()
            telegram_queue_size.set(
                telegram_queue.qsize()
            )

            if message is None:
                continue

            await process_message(
                message
            )

        except asyncio.CancelledError:
            raise

        except Exception:

            await log_error(
                "telegram_sender_loop",
                None,
                traceback.format_exc(),
            )

            worker_errors_total.labels(
                component="telegram_sender_loop"
            ).inc()
            await asyncio.sleep(5)
