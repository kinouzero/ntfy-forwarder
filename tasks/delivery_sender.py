import asyncio
import traceback

from core.state import telegram_queue, shutdown_event
from core.config import (
    DELIVERY_TARGETS,
    TELEGRAM_MAX_MESSAGE_LENGTH,
    TELEGRAM_QUEUE_MAX_ATTEMPTS,
    TELEGRAM_QUEUE_BASE_RETRY_SECONDS,
    TELEGRAM_QUEUE_MAX_RETRY_SECONDS,
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
)
from services.telegram import TelegramAPIError, tg_call
from services.targets.common import DeliveryError
from services.targets import telegram_target as telegram_target_impl
from services.targets.webhook_target import (
    send_generic_webhook_message as _send_generic_webhook_message,
)
from services.targets.discord_target import send_discord_message as _send_discord_message
from services.targets.slack_target import send_slack_message as _send_slack_message
from services.targets.whatsapp_target import send_whatsapp_message as _send_whatsapp_message
from db.errors import log_error
from db.topics import is_topic_enabled
from db.telegram_queue import (
    get_next_telegram_item,
    ack_telegram_item,
    retry_telegram_item,
    count_telegram_queue,
)
from db.dead_letter import move_to_dead_letter, count_dead_letters

RATE_LIMIT_DELAY = 0.05
QUEUE_POLL_IDLE_SECONDS = 0.5


TARGET_SENDER_NAMES = {
    "telegram": "send_telegram_message",
    "webhook": "send_generic_webhook_message",
    "discord": "send_discord_message",
    "slack": "send_slack_message",
    "whatsapp": "send_whatsapp_message",
}


async def send_telegram_message(message, attachment_url=None, priority=3):
    telegram_target_impl.tg_call = tg_call
    telegram_target_impl.TELEGRAM_MAX_MESSAGE_LENGTH = TELEGRAM_MAX_MESSAGE_LENGTH
    await telegram_target_impl.send_telegram_message(
        message,
        attachment_url=attachment_url,
        priority=priority,
    )


async def send_generic_webhook_message(message, attachment_url=None, priority=3):
    await _send_generic_webhook_message(
        message,
        attachment_url=attachment_url,
        priority=priority,
    )


async def send_discord_message(message, attachment_url=None, priority=3):
    await _send_discord_message(
        message,
        attachment_url=attachment_url,
        priority=priority,
    )


async def send_slack_message(message, attachment_url=None, priority=3):
    await _send_slack_message(
        message,
        attachment_url=attachment_url,
        priority=priority,
    )


async def send_whatsapp_message(message, attachment_url=None, priority=3):
    await _send_whatsapp_message(
        message,
        attachment_url=attachment_url,
        priority=priority,
    )


def _targets():
    return [t for t in DELIVERY_TARGETS if t]


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
    retry_after = getattr(exc, "retry_after", None)
    if retry_after:
        return max(1, min(int(retry_after), TELEGRAM_QUEUE_MAX_RETRY_SECONDS))
    delay = TELEGRAM_QUEUE_BASE_RETRY_SECONDS * (2 ** max(0, int(attempts)))
    return max(1, min(int(delay), TELEGRAM_QUEUE_MAX_RETRY_SECONDS))


def _is_retryable(exc):
    if isinstance(exc, TelegramAPIError):
        return bool(exc.retryable)
    if isinstance(exc, DeliveryError):
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
    if isinstance(exc, DeliveryError):
        ch = exc.channel
        if exc.status_code == 429:
            return f"{ch}_429"
        if exc.status_code is not None and exc.status_code >= 500:
            return f"{ch}_5xx"
        if exc.status_code is not None and exc.status_code >= 400:
            return f"{ch}_4xx"
        return f"{ch}_error"
    return "network_or_unknown"


async def process_queue_item(item):
    item_id, attempts, topic, message, attachment_url, priority = _unpack_queue_item(item)
    if message is None:
        return ("drop", None, None)
    if topic and not await is_topic_enabled(topic):
        log("INFO", "telegram message skipped for disabled topic", topic=topic)
        return ("drop", None, None)
    try:
        await process_message(message, attachment_url=attachment_url, priority=priority)
        return ("sent", None, None)
    except Exception as exc:
        retryable = _is_retryable(exc)
        delay = _compute_retry_delay_seconds(attempts, exc)
        reason = _reason_from_exception(exc)
        await log_error("delivery_sender", topic, str(exc))
        return ("retry" if retryable else "dead", delay, reason)


async def process_message(message, attachment_url=None, priority=3):
    sent_any = False
    for target in _targets():
        sender_name = TARGET_SENDER_NAMES.get(target)
        if sender_name is None:
            log("WARN", "unknown delivery target", target=target)
            continue
        sender = globals().get(sender_name)
        if sender is None:
            log("WARN", "missing sender implementation", target=target)
            continue
        await sender(message, attachment_url=attachment_url, priority=priority)
        sent_any = True
    if not sent_any:
        log("WARN", "no delivery target available")
    await asyncio.sleep(RATE_LIMIT_DELAY)
    telegram_messages_sent_total.inc()
    return True


async def delivery_sender_loop():
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

            status, delay, reason = await process_queue_item(message)
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
                    telegram_queue_retries_total.labels(reason=reason or "retryable").inc()
                    await retry_telegram_item(
                        item_id,
                        delay or TELEGRAM_QUEUE_BASE_RETRY_SECONDS,
                    )
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
                    worker_errors_total.labels(component="delivery_sender").inc()
                    await ack_telegram_item(item_id)
            telegram_queue_size.set(await count_telegram_queue())
            telegram_dead_letter_size.set(await count_dead_letters())
        except asyncio.CancelledError:
            raise
        except Exception:
            await log_error("delivery_sender_loop", None, traceback.format_exc())
            worker_errors_total.labels(component="delivery_sender_loop").inc()
            await asyncio.sleep(5)
