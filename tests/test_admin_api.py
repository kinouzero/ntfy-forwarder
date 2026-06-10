import pytest
import json
import asyncio
from aiohttp.test_utils import make_mocked_request

from api import web as api_web
from core.state import (
    workers,
    aggregation_buffer,
    digest_buffer,
    worker_last_seen,
)
from db.schema import init_db
from db.errors import log_error
from db.dead_letter import move_to_dead_letter


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
    first_task = workers["topic-1"]

    aggregation_buffer["topic-1"] = [object()]
    digest_buffer["topic-1"] = 3
    worker_last_seen["topic-1"] = 123

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
    assert "topic-1" in workers
    assert workers["topic-1"] is first_task
    assert "topic-1" in aggregation_buffer
    assert "topic-1" in digest_buffer
    assert "topic-1" in worker_last_seen

    req = make_mocked_request(
        "POST",
        "/api/topics/topic-1/toggle?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    req._match_info = match
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["enabled"] is True
    assert "topic-1" in workers
    assert workers["topic-1"] is first_task

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
    assert "count_total" in payload
    assert "count_24h" in payload
    assert "count_since_reset" in payload
    assert "status_counts" in payload

    req = make_mocked_request(
        "POST",
        "/api/topics/topic-1/reset_count?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    req._match_info = match
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["reset_count"] == "topic-1"

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
        "/api/topics/export?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["version"] == 1
    assert any(item["name"] == "topic-1" for item in payload["items"])

    class _Req:
        def __init__(self):
            self.match_info = {}

        async def json(self):
            return {
                "items": [
                    {"name": "topic-2", "enabled": True},
                    {"name": "topic-3", "enabled": False},
                ]
            }

    req = make_mocked_request(
        "POST",
        "/api/topics/import?token=tkn",
        app=app,
    )
    req.json = _Req().json  # type: ignore[attr-defined]
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["imported"] == 2
    assert payload["total"] == 2

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


@pytest.mark.asyncio
async def test_admin_errors_api(tmp_db_paths, monkeypatch):
    await init_db()
    monkeypatch.setattr(api_web, "ADMIN_TOKEN", "tkn")

    await log_error("worker", "topic-1", "boom")

    app = await api_web.create_web_app()
    req = make_mocked_request(
        "GET",
        "/api/errors?token=tkn&limit=10",
        app=app,
    )
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())

    assert len(payload["items"]) >= 1
    assert payload["items"][0]["component"] == "worker"
    assert "total" in payload

    req = make_mocked_request(
        "GET",
        "/api/errors?token=tkn&topic=topic-1&component=worker&q=boom&format=csv",
        app=app,
    )
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    body = resp.body.decode()
    assert resp.content_type == "text/csv"
    assert "component,topic,error" in body

    req = make_mocked_request(
        "POST",
        "/api/errors/clear?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["cleared"] is True
    assert payload["deleted"] >= 1

    req = make_mocked_request(
        "GET",
        "/api/errors?token=tkn&limit=10",
        app=app,
    )
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["total"] == 0


@pytest.mark.asyncio
async def test_dead_letter_api_and_requeue(tmp_db_paths, monkeypatch):
    await init_db()
    monkeypatch.setattr(api_web, "ADMIN_TOKEN", "tkn")
    await move_to_dead_letter(
        payload={"topic": "t1", "message": "x"},
        attempts=8,
        last_error="telegram_429",
        topic="t1",
    )

    app = await api_web.create_web_app()
    req = make_mocked_request(
        "GET",
        "/api/queue/dead_letters?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["dead_letters"] == 1
    item_id = payload["items"][0]["id"]

    req = make_mocked_request(
        "POST",
        f"/api/queue/dead_letters/{item_id}/requeue?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    req._match_info = match
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["requeued"] == item_id


@pytest.mark.asyncio
async def test_dead_letter_batch_requeue(tmp_db_paths, monkeypatch):
    await init_db()
    monkeypatch.setattr(api_web, "ADMIN_TOKEN", "tkn")
    await move_to_dead_letter(
        payload={"topic": "a", "message": "x"},
        attempts=8,
        last_error="telegram_429",
        topic="a",
    )
    await move_to_dead_letter(
        payload={"topic": "b", "message": "x"},
        attempts=8,
        last_error="telegram_4xx",
        topic="b",
    )
    app = await api_web.create_web_app()

    class _Req:
        async def json(self):
            return {"topic": "a", "reason": "telegram_429", "limit": 100}

    req = make_mocked_request(
        "POST",
        "/api/queue/dead_letters/requeue_batch?token=tkn",
        app=app,
    )
    req.json = _Req().json  # type: ignore[attr-defined]
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["requeued"] == 1


@pytest.mark.asyncio
async def test_dead_letter_clear_filtered(tmp_db_paths, monkeypatch):
    await init_db()
    monkeypatch.setattr(api_web, "ADMIN_TOKEN", "tkn")
    await move_to_dead_letter(
        payload={"topic": "a", "message": "x"},
        attempts=1,
        last_error="telegram_429",
        topic="a",
    )
    await move_to_dead_letter(
        payload={"topic": "b", "message": "y"},
        attempts=2,
        last_error="telegram_5xx",
        topic="b",
    )
    app = await api_web.create_web_app()

    class _Req:
        async def json(self):
            return {"topic": "a", "reason": "telegram_429"}

    req = make_mocked_request(
        "POST",
        "/api/queue/dead_letters/clear?token=tkn",
        app=app,
    )
    req.json = _Req().json  # type: ignore[attr-defined]
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["cleared"] is True
    assert payload["deleted"] == 1

    req = make_mocked_request(
        "GET",
        "/api/queue/dead_letters?token=tkn",
        app=app,
    )
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert payload["dead_letters"] == 1


@pytest.mark.asyncio
async def test_oidc_admin_redirects_html_when_not_logged(tmp_db_paths, monkeypatch):
    await init_db()
    monkeypatch.setattr(api_web, "ADMIN_TOKEN", "")
    monkeypatch.setattr(api_web, "OIDC_ENABLED", True)
    monkeypatch.setattr(api_web, "OIDC_ISSUER_URL", "https://issuer.example")
    monkeypatch.setattr(api_web, "OIDC_CLIENT_ID", "client")
    monkeypatch.setattr(api_web, "OIDC_CLIENT_SECRET", "secret")
    monkeypatch.setattr(api_web, "OIDC_REDIRECT_URI", "https://app/auth/callback")
    monkeypatch.setattr(api_web, "OIDC_SESSION_SECRET", "session-secret")

    app = await api_web.create_web_app()
    req = make_mocked_request("GET", "/admin", app=app)
    match = await app.router.resolve(req)

    with pytest.raises(api_web.web.HTTPFound) as exc:
        await match.handler(req)
    assert "/auth/login?next=%2Fadmin" in str(exc.value.location)


@pytest.mark.asyncio
async def test_oidc_admin_rejects_api_when_not_logged(tmp_db_paths, monkeypatch):
    await init_db()
    monkeypatch.setattr(api_web, "ADMIN_TOKEN", "")
    monkeypatch.setattr(api_web, "OIDC_ENABLED", True)
    monkeypatch.setattr(api_web, "OIDC_ISSUER_URL", "https://issuer.example")
    monkeypatch.setattr(api_web, "OIDC_CLIENT_ID", "client")
    monkeypatch.setattr(api_web, "OIDC_CLIENT_SECRET", "secret")
    monkeypatch.setattr(api_web, "OIDC_REDIRECT_URI", "https://app/auth/callback")
    monkeypatch.setattr(api_web, "OIDC_SESSION_SECRET", "session-secret")

    app = await api_web.create_web_app()
    req = make_mocked_request("GET", "/api/topics", app=app)
    match = await app.router.resolve(req)

    with pytest.raises(api_web.web.HTTPUnauthorized):
        await match.handler(req)


@pytest.mark.asyncio
async def test_token_and_oidc_can_coexist(tmp_db_paths, monkeypatch):
    await init_db()
    monkeypatch.setattr(api_web, "ADMIN_TOKEN", "tkn")
    monkeypatch.setattr(api_web, "OIDC_ENABLED", True)
    monkeypatch.setattr(api_web, "OIDC_ISSUER_URL", "https://issuer.example")
    monkeypatch.setattr(api_web, "OIDC_CLIENT_ID", "client")
    monkeypatch.setattr(api_web, "OIDC_CLIENT_SECRET", "secret")
    monkeypatch.setattr(api_web, "OIDC_REDIRECT_URI", "https://app/auth/callback")
    monkeypatch.setattr(api_web, "OIDC_SESSION_SECRET", "session-secret")

    app = await api_web.create_web_app()

    req = make_mocked_request("GET", "/api/topics?token=tkn", app=app)
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert "items" in payload

    req = make_mocked_request("GET", "/admin", app=app)
    match = await app.router.resolve(req)
    with pytest.raises(api_web.web.HTTPFound) as exc:
        await match.handler(req)
    assert "/auth/login?next=%2Fadmin" in str(exc.value.location)


@pytest.mark.asyncio
async def test_disable_query_token_requires_header_or_cookie(tmp_db_paths, monkeypatch):
    await init_db()
    monkeypatch.setattr(api_web, "ADMIN_TOKEN", "tkn")
    monkeypatch.setattr(api_web, "ADMIN_ALLOW_QUERY_TOKEN", False)
    monkeypatch.setattr(api_web, "OIDC_ENABLED", False)

    app = await api_web.create_web_app()

    req = make_mocked_request("GET", "/api/topics?token=tkn", app=app)
    match = await app.router.resolve(req)
    with pytest.raises(api_web.web.HTTPUnauthorized):
        await match.handler(req)

    req = make_mocked_request(
        "GET",
        "/api/topics",
        app=app,
        headers={"X-Admin-Token": "tkn"},
    )
    match = await app.router.resolve(req)
    resp = await match.handler(req)
    payload = json.loads(resp.body.decode())
    assert "items" in payload


@pytest.mark.asyncio
async def test_security_headers_middleware_applies(tmp_db_paths):
    await init_db()
    app = await api_web.create_web_app()

    req = make_mocked_request("GET", "/health", app=app)
    match = await app.router.resolve(req)
    resp = await api_web.security_headers_middleware(req, match.handler)

    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert resp.headers["X-Frame-Options"] == "DENY"
    assert resp.headers["Referrer-Policy"] == "no-referrer"
    assert "default-src 'self'" in resp.headers["Content-Security-Policy"]


def test_sanitize_next_path_blocks_open_redirect():
    assert api_web._sanitize_next_path("/admin/stats") == "/admin/stats"
    assert api_web._sanitize_next_path("https://evil.example") == "/admin"
    assert api_web._sanitize_next_path("//evil.example/path") == "/admin"


def test_oidc_user_allowed_verified_email(monkeypatch):
    monkeypatch.setattr(api_web, "OIDC_REQUIRE_VERIFIED_EMAIL", True)
    monkeypatch.setattr(api_web, "OIDC_ALLOWED_EMAILS", tuple())
    monkeypatch.setattr(api_web, "OIDC_ALLOWED_DOMAINS", tuple())
    assert api_web._oidc_user_allowed({"email": "a@b.c", "email_verified": True})
    assert not api_web._oidc_user_allowed({"email": "a@b.c", "email_verified": False})
