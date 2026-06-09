import pytest
import json
from aiohttp.test_utils import make_mocked_request

from api.web import create_web_app
from core.state import workers


@pytest.mark.asyncio
async def test_health_endpoint(monkeypatch):
    workers.clear()
    workers["t1"] = object()
    async def _count():
        return 2
    monkeypatch.setattr("api.web.count_telegram_queue", _count)
    async def _dead():
        return 0
    monkeypatch.setattr("api.web.count_dead_letters", _dead)
    monkeypatch.setattr("api.web.ACTIVE_TARGETS", ())

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

    app = await create_web_app()
    req = make_mocked_request("GET", "/health", app=app)
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())

    assert payload["status"] == "ok"
    assert payload["workers"] == 1
    assert payload["queue"] == 2
    assert "checks" in payload


@pytest.mark.asyncio
async def test_metrics_endpoint():
    app = await create_web_app()
    req = make_mocked_request("GET", "/metrics", app=app)
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    body = resp.body.decode()

    assert "forwarder_" in body
