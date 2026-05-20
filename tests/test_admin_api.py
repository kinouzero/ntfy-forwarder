import pytest
import json
import asyncio
from aiohttp.test_utils import make_mocked_request

from api import web as api_web
from core.state import workers
from db.schema import init_db


@pytest.mark.asyncio
async def test_admin_topics_list_and_toggle(tmp_db_paths, monkeypatch):
    await init_db()
    monkeypatch.setattr(api_web, "ADMIN_TOKEN", "tkn")
    workers.clear()

    app = await api_web.create_web_app()

    req = make_mocked_request(
        "GET",
        "/api/topics?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["items"] == []

    req = make_mocked_request(
        "POST",
        "/api/topics/topic-1/toggle?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    req._match_info = match
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["name"] == "topic-1"
    assert payload["enabled"] is True
    assert "topic-1" in workers

    req = make_mocked_request(
        "POST",
        "/api/topics/topic-1/toggle?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    req._match_info = match
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["enabled"] is False
    assert "topic-1" not in workers

    req = make_mocked_request(
        "POST",
        "/api/topics/pause_all?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    req._match_info = match
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["paused"] is True

    req = make_mocked_request(
        "POST",
        "/api/topics/resume_all?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    req._match_info = match
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["resumed"] is True

    req = make_mocked_request(
        "GET",
        "/api/topics/topic-1?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    req._match_info = match
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["name"] == "topic-1"

    req = make_mocked_request(
        "POST",
        "/api/topics/topic-1/clear?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    req._match_info = match
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["cleared"] == "topic-1"

    req = make_mocked_request(
        "POST",
        "/api/topics/clear_all?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    req._match_info = match
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["cleared"] is True

    req = make_mocked_request(
        "GET",
        "/api/stats?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    req._match_info = match
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert "workers" in payload

    for task in list(workers.values()):
        task.cancel()
    await asyncio.gather(
        *workers.values(),
        return_exceptions=True,
    )
