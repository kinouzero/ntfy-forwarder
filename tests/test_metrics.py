from prometheus_client import generate_latest

from core import metrics


def test_metrics_exposed_and_incrementable():
    metrics.ntfy_messages_total.labels(
        topic="topic-1"
    ).inc()
    metrics.telegram_messages_sent_total.inc()
    metrics.telegram_queue_size.set(3)
    metrics.telegram_dead_letter_size.set(1)
    metrics.telegram_queue_retries_total.labels(reason="telegram_429").inc()
    metrics.db_maintenance_runs_total.labels(operation="optimize").inc()

    data = generate_latest().decode()

    assert "forwarder_ntfy_messages_total" in data
    assert "forwarder_telegram_messages_sent_total" in data
    assert "forwarder_telegram_queue_size" in data
    assert "forwarder_telegram_dead_letter_size" in data
    assert "forwarder_telegram_queue_retries_total" in data
    assert "forwarder_db_maintenance_runs_total" in data
