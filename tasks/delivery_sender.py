import asyncio
import traceback

from core.state import telegram_queue, shutdown_event
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
from services.telegram import TelegramAPIError
from services.targets.common import DeliveryError
from services.targets import telegram_target as telegram_target_impl
from services.targets.webhook_target import send_webhook_message
from db.errors import log_error
from db.settings import get_settings_snapshot
from db.topics import is_topic_enabled
from db.targets import resolve_delivery_target_for_topic
from db.telegram_queue import (
    get_next_telegram_item,
    ack_telegram_item,
    retry_telegram_item,
    count_telegram_queue,
)
from db.dead_letter import move_to_dead_letter, count_dead_letters

RATE_LIMIT_DELAY = 0.05
QUEUE_POLL_IDLE_SECONDS = 0.5

SUPPORTED_TARGET_KINDS = {
    "telegram",
    "webhook_generic",
    "webhook_discord",
    "webhook_slack",
}


async def send_telegram_message(target, message, attachment_url=None, priority=3):
    cfg = target.get("config") or {}
    chat_id = str(cfg.get("chat_id") or "").strip()
    bot_token = str(cfg.get("bot_token") or "").strip()
    max_message_length = int(cfg.get("max_message_length") or 4096)
    if not chat_id:
        raise DeliveryError(
            "telegram",
            "missing telegram chat_id in target config",
            status_code=400,
            retryable=False,
        )
    if not bot_token:
        raise DeliveryError(
            "telegram",
            "missing telegram bot_token in target config",
            status_code=400,
            retryable=False,
        )
    await telegram_target_impl.send_telegram_message(
        message,
        attachment_url=attachment_url,
        priority=priority,
        chat_id=chat_id,
        bot_token=bot_token,
        max_message_length=max_message_length,
    )


async def send_webhook_kind(target, message, attachment_url=None, priority=3):
    cfg = target.get("config") or {}
    url = str(cfg.get("url") or "").strip()
    if not url:
        raise DeliveryError(
            "webhook",
            "missing webhook url in target config",
            status_code=400,
            retryable=False,
        )
    kind = str(target.get("kind") or "").strip()
    payload_type = {
        "webhook_generic": "generic",
        "webhook_discord": "discord",
        "webhook_slack": "slack",
    }.get(kind)
    if payload_type is None:
        raise DeliveryError(
            "webhook",
            f"unsupported webhook kind: {kind}",
            status_code=400,
            retryable=False,
        )
    await send_webhook_message(
        url,
        payload_type,
        message,
        attachment_url=attachment_url,
        priority=priority,
        auth_header=cfg.get("auth_header"),
    )


def _unpack_queue_item(item):
    if isinstance(item, dict) and "payload" in item:
        payload = item.get("payload", {})
        if not isinstance(payload, dict):
            payload = {"message": payload}
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


def _compute_retry_delay_seconds(attempts, exc, settings):
    max_retry = int(settings["delivery_queue_max_retry_seconds"])
    base_retry = int(settings["delivery_queue_base_retry_seconds"])
    retry_after = getattr(exc, "retry_after", None)
    if retry_after:
        # A provider delay is a lower bound, not the local backoff cap.
        return max(1, int(retry_after))
    delay = base_retry * (2 ** max(0, int(attempts)))
    return max(1, min(int(delay), max_retry))


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
        log("INFO", "message skipped for disabled topic", topic=topic)
        return ("drop", None, None)
    try:
        await process_message(
            message,
            attachment_url=attachment_url,
            priority=priority,
            topic=topic,
        )
        return ("sent", None, None)
    except Exception as exc:
        settings = await get_settings_snapshot()
        retryable = _is_retryable(exc)
        delay = _compute_retry_delay_seconds(attempts, exc, settings)
        reason = _reason_from_exception(exc)
        await log_error("delivery_sender", topic, str(exc))
        return ("retry" if retryable else "dead", delay, reason)


async def process_message(message, attachment_url=None, priority=3, topic=None):
    target = await resolve_delivery_target_for_topic(topic)
    if target is None:
        raise DeliveryError("target", "no delivery target available", retryable=True)

    kind = str(target.get("kind") or "").strip()
    if kind not in SUPPORTED_TARGET_KINDS:
        raise DeliveryError(
            "target",
            f"unsupported target kind: {kind}",
            status_code=400,
            retryable=False,
        )

    if kind == "telegram":
        await send_telegram_message(
            target,
            message,
            attachment_url=attachment_url,
            priority=priority,
        )
    else:
        await send_webhook_kind(
            target,
            message,
            attachment_url=attachment_url,
            priority=priority,
        )

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
                settings = await get_settings_snapshot()
                max_attempts = int(settings["delivery_queue_max_attempts"])
                base_retry = int(settings["delivery_queue_base_retry_seconds"])
                attempts = int(queue_attempts) + 1
                if status in {"sent", "drop"}:
                    await ack_telegram_item(item_id)
                elif status == "retry" and attempts < max_attempts:
                    telegram_queue_retries_total.labels(reason=reason or "retryable").inc()
                    await retry_telegram_item(
                        item_id,
                        delay or base_retry,
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
                        queue_item_id=item_id,
                    )
                    telegram_dead_letter_total.labels(
                        reason=reason or "max_attempts_reached"
                    ).inc()
                    telegram_messages_failed_total.inc()
                    worker_errors_total.labels(component="delivery_sender").inc()
            telegram_queue_size.set(await count_telegram_queue())
            telegram_dead_letter_size.set(await count_dead_letters())
        except asyncio.CancelledError:
            raise
        except Exception:
            await log_error("delivery_sender_loop", None, traceback.format_exc())
            worker_errors_total.labels(component="delivery_sender_loop").inc()
            await asyncio.sleep(5)
