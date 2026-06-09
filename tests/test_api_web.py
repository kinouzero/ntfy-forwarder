import json

import pytest

from api.web import health, metrics
from core.state import (
    workers,
)


@pytest.mark.asyncio
async def test_health_returns_status_and_counts(monkeypatch):
    workers.clear()
    workers["t1"] = object()
    async def _count():
        return 2
    monkeypatch.setattr("api.web.count_telegram_queue", _count)
    async def _dead():
        return 0
    monkeypatch.setattr("api.web.count_dead_letters", _dead)
    monkeypatch.setattr("api.web.HEALTH_TELEGRAM_CHECK_ENABLED", False)
    monkeypatch.setattr("api.web.TELEGRAM_ENABLED", False)

    class _Conn:
        async def execute(self, *_a, **_kw):
            return None
        async def commit(self):
            return None
        async def close(self):
            return None

    async def _db():
        return _Conn()
    monkeypatch.setattr("api.web.db", _db)

    resp = await health(None)
    payload = json.loads(resp.body.decode())

    assert payload["status"] == "ok"
    assert payload["workers"] == 1
    assert payload["queue"] == 2
    assert "checks" in payload


@pytest.mark.asyncio
async def test_metrics_returns_prometheus_payload():
    resp = await metrics(None)
    body = resp.body.decode()

    assert resp.content_type == "text/plain"
    assert "forwarder_" in body


@pytest.mark.asyncio
async def test_health_degraded_when_db_fails(monkeypatch):
    workers.clear()
    async def _count():
        return 0
    monkeypatch.setattr("api.web.count_telegram_queue", _count)
    monkeypatch.setattr("api.web.count_dead_letters", _count)
    monkeypatch.setattr("api.web.HEALTH_TELEGRAM_CHECK_ENABLED", False)
    monkeypatch.setattr("api.web.TELEGRAM_ENABLED", False)

    async def _db():
        raise RuntimeError("db down")
    monkeypatch.setattr("api.web.db", _db)

    resp = await health(None)
    payload = json.loads(resp.body.decode())
    assert payload["status"] == "degraded"
