import asyncio
import traceback

from core.state import (
    telegram_queue,
    shutdown_event,
)

from core.config import TG_ADMIN

from services.telegram import tg_call

from db.errors import log_error
from db.topics import is_topic_enabled
from db.telegram_queue import (
    get_next_telegram_item,
    ack_telegram_item,
    retry_telegram_item,
    count_telegram_queue,
)
from db.dead_letter import (
    move_to_dead_letter,
    count_dead_letters,
)
from core.logging import log

from core.metrics import (
    telegram_messages_sent_total,
    telegram_messages_failed_total,
    telegram_queue_size,
    telegram_dead_letter_size,
    telegram_queue_retries_total,
    telegram_dead_letter_total,
    worker_errors_total,
    telegram_send_seconds,
)
from core.config import (
    TELEGRAM_MAX_MESSAGE_LENGTH,
    TELEGRAM_QUEUE_MAX_ATTEMPTS,
    TELEGRAM_QUEUE_BASE_RETRY_SECONDS,
    TELEGRAM_QUEUE_MAX_RETRY_SECONDS,
)
from utils.telegram import split_message
from services.telegram import TelegramAPIError

RATE_LIMIT_DELAY = 0.05
QUEUE_POLL_IDLE_SECONDS = 0.5


def _telegram_disable_notification(priority):
    # Telegram only supports "silent" vs "normal".
    # Map ntfy priorities 1-2 to silent, 3-5 to normal notifications.
    return int(priority or 3) <= 2


async def send_telegram_message(message, attachment_url=None, priority=3):

    disable_notification = _telegram_disable_notification(priority)

    for chunk in split_message(
        message,
        TELEGRAM_MAX_MESSAGE_LENGTH,
    ):
        payload = {
            "chat_id": TG_ADMIN,
            "text": chunk,
            "parse_mode": "MarkdownV2",
            "disable_notification": disable_notification,
        }

        start = asyncio.get_event_loop().time()
        await tg_call(
            "sendMessage",
            payload,
        )
        telegram_send_seconds.observe(
            asyncio.get_event_loop().time() - start
        )

    if attachment_url:
        await tg_call(
            "sendDocument",
            {
                "chat_id": TG_ADMIN,
                "document": attachment_url,
                "disable_notification": disable_notification,
            },
        )


def _unpack_queue_item(item):
    if isinstance(item, dict) and "payload" in item:
        payload = item.get("payload", {})
        return (
            item.get("id"),
            item.get("attempts", 0),
            payload.get("topic"),
            payload.get("message"),
            payload.get("attachment_url"),
            payload.get("priority", 3),
        )
    if isinstance(item, dict):
        return (
            item.get("id"),
            item.get("attempts", 0),
            item.get("topic"),
            item.get("message"),
            item.get("attachment_url"),
            item.get("priority", 3),
        )
    if isinstance(item, tuple) and len(item) == 2:
        topic, message = item
        return None, 0, topic, message, None, 3
    return None, 0, None, item, None, 3


def _compute_retry_delay_seconds(attempts, exc):
    if isinstance(exc, TelegramAPIError) and exc.retry_after:
        return max(1, min(int(exc.retry_after), TELEGRAM_QUEUE_MAX_RETRY_SECONDS))
    delay = TELEGRAM_QUEUE_BASE_RETRY_SECONDS * (2 ** max(0, int(attempts)))
    return max(1, min(int(delay), TELEGRAM_QUEUE_MAX_RETRY_SECONDS))


def _is_retryable(exc):
    if isinstance(exc, TelegramAPIError):
        return bool(exc.retryable)
    return True


def _reason_from_exception(exc):
    if isinstance(exc, TelegramAPIError):
        if exc.status_code == 429:
            return "telegram_429"
        if exc.status_code is not None and exc.status_code >= 500:
            return "telegram_5xx"
        if exc.status_code is not None and exc.status_code >= 400:
            return "telegram_4xx"
        return "telegram_api"
    return "network_or_unknown"


async def process_queue_item(item):
    item_id, attempts, topic, message, attachment_url, priority = _unpack_queue_item(item)
    if message is None:
        return ("drop", None, None)
    if topic and not await is_topic_enabled(topic):
        log("INFO", "telegram message skipped for disabled topic", topic=topic)
        return ("drop", None, None)
    try:
        await process_message(
            message,
            attachment_url=attachment_url,
            priority=priority,
        )
        return ("sent", None, None)
    except Exception as exc:
        retryable = _is_retryable(exc)
        delay = _compute_retry_delay_seconds(attempts, exc)
        reason = _reason_from_exception(exc)
        await log_error("telegram_sender", topic, str(exc))
        return ("retry" if retryable else "dead", delay, reason)


async def process_message(message, attachment_url=None, priority=3):
    await send_telegram_message(
        message,
        attachment_url=attachment_url,
        priority=priority,
    )
    await asyncio.sleep(
        RATE_LIMIT_DELAY
    )
    telegram_messages_sent_total.inc()
    return True


async def telegram_sender_loop():

    while not shutdown_event.is_set():

        try:
            if telegram_queue.empty():
                row = await get_next_telegram_item()
                if row is not None:
                    await telegram_queue.put(row)
                else:
                    telegram_queue_size.set(await count_telegram_queue())
                    await asyncio.sleep(QUEUE_POLL_IDLE_SECONDS)
                    continue

            message = await telegram_queue.get()

            if message is None:
                continue

            status, delay, reason = await process_queue_item(
                message
            )
            (
                item_id,
                queue_attempts,
                _topic,
                _msg,
                _attachment,
                _priority,
            ) = _unpack_queue_item(message)
            if item_id is not None:
                attempts = int(queue_attempts) + 1
                if status in {"sent", "drop"}:
                    await ack_telegram_item(item_id)
                elif status == "retry" and attempts < TELEGRAM_QUEUE_MAX_ATTEMPTS:
                    telegram_queue_retries_total.labels(
                        reason=reason or "retryable"
                    ).inc()
                    await retry_telegram_item(item_id, delay or TELEGRAM_QUEUE_BASE_RETRY_SECONDS)
                else:
                    await move_to_dead_letter(
                        payload={
                            "topic": _topic,
                            "message": _msg,
                            "attachment_url": _attachment,
                            "priority": _priority,
                        },
                        attempts=attempts,
                        last_error=reason or "max_attempts_reached",
                        topic=_topic,
                    )
                    telegram_dead_letter_total.labels(
                        reason=reason or "max_attempts_reached"
                    ).inc()
                    telegram_messages_failed_total.inc()
                    worker_errors_total.labels(
                        component="telegram_sender"
                    ).inc()
                    await ack_telegram_item(item_id)
            telegram_queue_size.set(await count_telegram_queue())
            telegram_dead_letter_size.set(await count_dead_letters())

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
