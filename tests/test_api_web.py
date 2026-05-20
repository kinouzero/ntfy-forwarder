import json

import pytest

from api.web import health, metrics
from core.state import (
    workers,
    telegram_queue,
)
from tests.conftest import drain_queue


@pytest.mark.asyncio
async def test_health_returns_status_and_counts():
    workers.clear()
    workers["t1"] = object()
    drain_queue(telegram_queue)
    telegram_queue.put_nowait("msg-1")
    telegram_queue.put_nowait("msg-2")

    resp = await health(None)
    payload = json.loads(resp.body.decode())

    assert payload["status"] == "ok"
    assert payload["workers"] == 1
    assert payload["queue"] == 2

    drain_queue(telegram_queue)


@pytest.mark.asyncio
async def test_metrics_returns_prometheus_payload():
    resp = await metrics(None)
    body = resp.body.decode()

    assert resp.content_type == "text/plain"
    assert "forwarder_" in body
