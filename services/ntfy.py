import json
import asyncio
import time
import random
from collections import deque

from core.http import get_http_session
from core.logging import log
from core.state import (
    aggregation_buffer,
    shutdown_event,
    worker_last_seen,
    digest_buffer,
    recent_events,
    topic_stats,
    topic_rates,
)

from core.config import (
    NTFY_BASE_URL,
    NTFY_TOKEN,
    MAX_AGGREGATION_BUFFER,
    MAX_DIGEST_BUFFER,
    DB_BATCH_SIZE,
    DB_BATCH_FLUSH_SECONDS,
    ADMIN_RECENT_EVENTS,
)

from utils.quiet_hours import in_quiet_hours

from models.event import NtfyEvent

from services.plugins import run_plugins

from db.messages import insert_messages
from db.topics import (
    increment_topic_count,
    is_topic_enabled,
    increment_topic_status_count,
)
from db.dead_letter import move_to_dead_letter
from db.errors import log_error

from core.metrics import (
    ntfy_messages_total,
    ntfy_messages_filtered_total,
    ntfy_messages_inserted_total,
    ntfy_messages_duplicate_total,
    worker_errors_total,
    aggregation_dropped_total,
    digest_dropped_total,
    db_insert_seconds,
    event_process_seconds,
)

async def ntfy_worker(topic):
    headers = {}

    if NTFY_TOKEN:
        headers["Authorization"] = (
            f"Bearer {NTFY_TOKEN}"
        )

    url = f"{NTFY_BASE_URL}/{topic}/json"

    backoff = 1
    buffer = []
    last_flush = time.monotonic()
    topic_stats.setdefault(
        topic,
        {
            "received": 0,
            "filtered": 0,
            "rate_limited": 0,
            "disabled": 0,
            "inserted": 0,
            "errors": 0,
        },
    )
    recent_events.setdefault(
        topic,
        deque(maxlen=ADMIN_RECENT_EVENTS),
    )
    topic_rates.setdefault(
        topic,
        deque(maxlen=60),
    )

    async def flush_buffer():
        nonlocal buffer, last_flush
        if not buffer:
            return

        if not await is_topic_enabled(topic):
            for raw, evt in buffer:
                log("DEBUG", "filtered", topic=topic, reason="disabled")
                ntfy_messages_filtered_total.labels(
                    topic=topic,
                    reason="disabled",
                ).inc()
                topic_stats[topic]["filtered"] += 1
                topic_stats[topic]["disabled"] += 1
                await increment_topic_status_count(topic, "filtered")
                await increment_topic_status_count(topic, "disabled")
                await move_to_dead_letter(
                    payload=raw,
                    attempts=0,
                    last_error="topic_disabled",
                    topic=topic,
                )
                recent_events[topic].append(
                    {
                        "ts": int(time.time()),
                        "message": evt.message,
                        "priority": evt.priority,
                        "event_id": evt.event_id,
                        "title": evt.title,
                        "tags": evt.tags,
                    }
                )
            buffer = []
            last_flush = time.monotonic()
            return

        raws = [raw for raw, _evt in buffer]
        events = [_evt for _raw, _evt in buffer]

        start = time.monotonic()
        inserted = await insert_messages(
            topic,
            raws,
        )
        db_insert_seconds.labels(
            operation="batch" if len(raws) > 1 else "single"
        ).observe(time.monotonic() - start)
        inserted_count = sum(1 for ok in inserted if ok)
        log(
            "DEBUG",
            "db flush",
            topic=topic,
            buffered=len(buffer),
            inserted=inserted_count,
        )

        for ok, evt in zip(inserted, events):
            if not ok:
                ntfy_messages_duplicate_total.labels(
                    topic=topic
                ).inc()
                continue

            ntfy_messages_inserted_total.labels(
                topic=topic
            ).inc()
            topic_stats[topic]["inserted"] += 1

            await increment_topic_count(
                topic
            )

            evt = await run_plugins(
                evt
            )

            aggregation_buffer.setdefault(
                topic,
                []
            ).append(evt)
            if (
                len(aggregation_buffer[topic])
                > MAX_AGGREGATION_BUFFER
            ):
                aggregation_buffer[topic].pop(0)
                aggregation_dropped_total.labels(
                    topic=topic
                ).inc()

            if topic in digest_buffer or (
                len(digest_buffer) < MAX_DIGEST_BUFFER
            ):
                digest_buffer[topic] = (
                    digest_buffer.get(topic, 0) + 1
                )
            else:
                digest_dropped_total.labels(
                    topic=topic
                ).inc()

            recent_events[topic].append(
                {
                    "ts": int(time.time()),
                    "message": evt.message,
                    "priority": evt.priority,
                    "event_id": evt.event_id,
                    "title": evt.title,
                    "tags": evt.tags,
                }
            )

        buffer = []
        last_flush = time.monotonic()

    log("INFO", "ntfy worker started", topic=topic)

    while not shutdown_event.is_set():

        try:

            session = get_http_session()
            if session is None:
                await asyncio.sleep(1)
                continue

            log("DEBUG", "ntfy connecting", topic=topic, url=url)
            async with session.get(
                url,
                headers=headers,
                timeout=None,
            ) as resp:

                async for line in resp.content:

                    if not line:
                        continue

                    raw = json.loads(
                        line.decode().strip()
                    )

                    if raw.get("event") != "message":
                        continue

                    worker_last_seen[topic] = int(time.time())
                    ntfy_messages_total.labels(topic=topic).inc()
                    topic_stats[topic]["received"] += 1
                    await increment_topic_status_count(topic, "received")
                    start_time = time.monotonic()

                    event = NtfyEvent.from_json(
                        topic,
                        raw,
                    )

                    if not await is_topic_enabled(topic):
                        log("DEBUG", "filtered", topic=topic, reason="disabled")
                        ntfy_messages_filtered_total.labels(
                            topic=topic,
                            reason="disabled",
                        ).inc()
                        topic_stats[topic]["filtered"] += 1
                        topic_stats[topic]["disabled"] += 1
                        await increment_topic_status_count(topic, "filtered")
                        await increment_topic_status_count(topic, "disabled")
                        await move_to_dead_letter(
                            payload=raw,
                            attempts=0,
                            last_error="topic_disabled",
                            topic=topic,
                        )
                        recent_events[topic].append(
                            {
                                "ts": int(time.time()),
                                "message": event.message,
                                "priority": event.priority,
                                "event_id": event.event_id,
                                "title": event.title,
                                "tags": event.tags,
                            }
                        )
                        continue

                    if (
                        in_quiet_hours()
                        and event.priority < 4
                    ):
                        log("DEBUG", "filtered", topic=topic, reason="quiet_hours")
                        ntfy_messages_filtered_total.labels(
                            topic=topic,
                            reason="quiet_hours",
                        ).inc()
                        topic_stats[topic]["filtered"] += 1
                        await increment_topic_status_count(topic, "filtered")
                        continue

                    buffer.append((raw, event))

                    if (
                        len(buffer) >= DB_BATCH_SIZE
                        or time.monotonic() - last_flush
                        >= DB_BATCH_FLUSH_SECONDS
                    ):
                        await flush_buffer()

                    event_process_seconds.labels(
                        topic=topic
                    ).observe(time.monotonic() - start_time)

                    topic_rates[topic].append(
                        int(time.time())
                    )

                backoff = 1
                if buffer:
                    await flush_buffer()

        except Exception as e:

            await log_error(
                "ntfy_worker",
                topic,
                e,
            )
            worker_errors_total.labels(
                component="ntfy_worker"
            ).inc()
            topic_stats[topic]["errors"] += 1

            log("WARN", "ntfy worker error", topic=topic, error=str(e))
            await asyncio.sleep(
                min(backoff, 60) + random.random()
            )
            backoff = min(backoff * 2, 60)
