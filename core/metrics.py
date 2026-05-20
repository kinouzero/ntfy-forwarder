from prometheus_client import Counter, Gauge
from prometheus_client import Histogram

ntfy_messages_total = Counter(
    "forwarder_ntfy_messages_total",
    "Total ntfy messages received",
    ["topic"],
)

ntfy_messages_filtered_total = Counter(
    "forwarder_ntfy_messages_filtered_total",
    "Total ntfy messages filtered out",
    ["topic", "reason"],
)

ntfy_messages_inserted_total = Counter(
    "forwarder_ntfy_messages_inserted_total",
    "Total ntfy messages inserted into db",
    ["topic"],
)

ntfy_messages_duplicate_total = Counter(
    "forwarder_ntfy_messages_duplicate_total",
    "Total ntfy messages skipped due to duplicates",
    ["topic"],
)

telegram_messages_sent_total = Counter(
    "forwarder_telegram_messages_sent_total",
    "Total telegram messages sent",
)

telegram_messages_failed_total = Counter(
    "forwarder_telegram_messages_failed_total",
    "Total telegram messages failed",
)

telegram_queue_size = Gauge(
    "forwarder_telegram_queue_size",
    "Current size of telegram queue",
)

worker_errors_total = Counter(
    "forwarder_worker_errors_total",
    "Total worker errors",
    ["component"],
)

rate_limited_total = Counter(
    "forwarder_rate_limited_total",
    "Total messages dropped by rate limiting",
    ["topic"],
)

aggregation_dropped_total = Counter(
    "forwarder_aggregation_dropped_total",
    "Total events dropped from aggregation buffer",
    ["topic"],
)

digest_dropped_total = Counter(
    "forwarder_digest_dropped_total",
    "Total events dropped from digest buffer",
    ["topic"],
)

db_insert_seconds = Histogram(
    "forwarder_db_insert_seconds",
    "DB insert duration seconds",
    ["operation"],
)

telegram_send_seconds = Histogram(
    "forwarder_telegram_send_seconds",
    "Telegram send duration seconds",
)

event_process_seconds = Histogram(
    "forwarder_event_process_seconds",
    "Event processing duration seconds",
    ["topic"],
)
