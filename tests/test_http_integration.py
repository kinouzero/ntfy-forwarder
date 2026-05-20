import pytest
import json
from aiohttp.test_utils import make_mocked_request

from api.web import create_web_app
from core.state import workers, telegram_queue
from tests.conftest import drain_queue


@pytest.mark.asyncio
async def test_health_endpoint():
    workers.clear()
    workers["t1"] = object()
    drain_queue(telegram_queue)
    telegram_queue.put_nowait("m1")
    telegram_queue.put_nowait("m2")

    app = await create_web_app()
    req = make_mocked_request("GET", "/health", app=app)
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())

    assert payload["status"] == "ok"
    assert payload["workers"] == 1
    assert payload["queue"] == 2

    drain_queue(telegram_queue)


@pytest.mark.asyncio
async def test_metrics_endpoint():
    app = await create_web_app()
    req = make_mocked_request("GET", "/metrics", app=app)
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    body = resp.body.decode()

    assert "forwarder_" in body
