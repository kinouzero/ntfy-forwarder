import asyncio
from unittest.mock import AsyncMock

import pytest

from api import web as api
from db.topics import add_topic
from db.targets import get_delivery_target


@pytest.mark.asyncio
async def test_export_route_is_not_shadowed(web_client):
    await add_topic("alpha")
    response = await web_client.get("/api/topics/export")
    assert response.status == 200
    assert (await response.json())["items"] == [{"name": "alpha", "enabled": True}]


@pytest.mark.asyncio
async def test_target_api_and_topic_binding(web_client):
    response = await web_client.post("/api/targets", json={
        "name": "alerts", "kind": "telegram", "is_default": True,
        "config": {"chat_id": "123", "bot_token": "fake", "max_message_length": 20000},
    })
    assert response.status == 200
    target_id = (await response.json())["id"]
    assert (await get_delivery_target(target_id))["config"]["max_message_length"] == 4096
    response = await web_client.get("/api/targets")
    assert len((await response.json())["items"]) == 1
    response = await web_client.post("/api/targets", json={
        "name": "alerts", "kind": "webhook_generic", "config": {"url": "https://example.test"},
    })
    assert response.status == 409
    response = await web_client.put(f"/api/targets/{target_id}", json={
        "name": "updated", "kind": "webhook_slack", "config": {"url": "https://example.test"},
    })
    assert response.status == 200
    assert (await get_delivery_target(target_id))["name"] == "updated"
    assert (await web_client.post(f"/api/targets/{target_id}/default")).status == 200
    await add_topic("news")
    for value in (target_id, None):
        response = await web_client.post("/api/topics/news/target", json={"target_id": value})
        assert response.status == 200
        assert (await response.json())["target_id"] == value
    assert (await web_client.delete(f"/api/targets/{target_id}")).status == 200
    assert (await web_client.get("/api/targets")).status == 200


@pytest.mark.parametrize("payload", [[], {}, {"name": "a", "kind": "wrong"},
                                     {"name": "a", "kind": "telegram", "config": []},
                                     {"name": "a", "kind": "telegram", "config": {
                                         "max_message_length": "oops"}},
                                     {"name": "a", "kind": "webhook_generic",
                                         "config": {"url": "file:///tmp/a"}},
                                     {"name": "a", "kind": "webhook_generic", "config": {}}])
@pytest.mark.asyncio
async def test_invalid_targets_return_400(web_client, payload):
    assert (await web_client.post("/api/targets", json=payload)).status == 400


@pytest.mark.parametrize("method,path,payload,status", [
    ("post", "/api/targets", "broken", 400),
    ("put", "/api/targets/999", {}, 404),
    ("delete", "/api/targets/999", None, 404),
    ("post", "/api/targets/999/default", None, 404),
    ("delete", "/api/targets/not-an-id", None, 400),
    ("post", "/api/topics/missing/target", {}, 404),
    ("post", "/api/topics/news/target", {"target_id": 999}, 400),
    ("post", "/api/topics/news/target", {"target_id": "x"}, 400),
    ("post", "/api/topics/news/target", [], 400),
    ("post", "/api/topics/news/target", "broken", 400),
    ("post", "/api/topics/import", {"items": None}, 400),
    ("post", "/api/topics/import", [], 400),
    ("post", "/api/topics/import", 3, 400),
    ("post", "/api/topics/import", "broken", 400),
    ("post", "/api/settings", [], 400),
    ("post", "/api/settings", "broken", 400),
    ("get", "/api/errors?limit=bad", None, 400),
    ("get", "/api/queue/dead_letters?offset=bad", None, 400),
    ("post", "/api/queue/dead_letters/999/requeue", None, 404),
    ("post", "/api/topics/missing/reset_count", None, 404),
    ("get", "/api/topics/missing", None, 404),
])
@pytest.mark.asyncio
async def test_api_client_errors(web_client, method, path, payload, status):
    await add_topic("news")
    kwargs = {"data": payload} if isinstance(payload, str) else {"json": payload}
    response = await web_client.request(method, path, **kwargs)
    assert response.status == status, await response.text()
    assert response.headers["X-Content-Type-Options"] == "nosniff"


@pytest.mark.parametrize("path", ["/", "/targets", "/settings", "/stats", "/errors", "/queue",
                                  "/topic/news"])
@pytest.mark.asyncio
async def test_html_pages_render(web_client, path):
    await add_topic("news")
    response = await web_client.get(path)
    assert response.status == 200
    assert "text/html" in response.headers["Content-Type"]
    assert "<!doctype html>" in (await response.text()).lower()


@pytest.mark.asyncio
async def test_settings_api_persists(web_client):
    response = await web_client.post("/api/settings", json={"quiet_hours_start": 0,
                                                            "quiet_hours_end": 0})
    assert response.status == 200
    assert (await response.json())["saved"] is True
    response = await web_client.get("/api/settings")
    assert (await response.json())["values"]["quiet_hours_end"] == 0


@pytest.mark.asyncio
async def test_import_mixed_entries_and_worker_cleanup(web_client):
    response = await web_client.post("/api/topics/import", json=["alpha", "", 4,
                                     {"name": "beta", "enabled": False}])
    assert (await response.json())["imported"] == 2
    await api._stop_worker("alpha")
    assert "alpha" not in api.workers
    await api._stop_worker("missing")
    done = asyncio.create_task(asyncio.sleep(0))
    await done
    api.workers["done"] = done
    await api._stop_worker("done")


@pytest.mark.asyncio
async def test_health_database_failure_returns_503(web_client, monkeypatch):
    monkeypatch.setattr(api, "count_telegram_queue", AsyncMock(side_effect=OSError("db offline")))
    response = await web_client.get("/health")
    assert response.status == 503
    payload = await response.json()
    assert payload["status"] == "degraded"
    assert payload["checks"]["db_writable"]["ok"] is False


@pytest.mark.asyncio
async def test_local_login_bad_credentials_and_redirect_sanitization(web_client, monkeypatch):
    monkeypatch.setattr(api, "ACCESS_LOCAL_ENABLED", True)
    monkeypatch.setattr(api, "ACCESS_LOCAL_USERNAME", "admin")
    monkeypatch.setattr(api, "ACCESS_LOCAL_PASSWORD", "secret")
    monkeypatch.setattr(api, "ACCESS_SESSION_SECRET", "session-secret")
    response = await web_client.post("/login", data={"username": "é", "password": "wrong"})
    assert response.status == 401 and "Invalid username" in await response.text()
    response = await web_client.get('/login', params={"next": '/\"><script>alert(1)</script>'})
    assert '<script>alert(1)</script>' not in await response.text()
    for path in ("//evil.test", "/\\evil.test", "/\nmalicious", "https://evil.test"):
        assert api._sanitize_next_path(path) == "/"
    response = await web_client.post("/login", data={"username": "admin", "password": "secret",
                                                     "next": "/\\evil.test"}, allow_redirects=False)
    assert response.status == 302 and response.headers["Location"] == "/"


@pytest.mark.asyncio
async def test_unconfigured_auth_endpoints(web_client):
    for method, path in (("GET", "/login"), ("POST", "/login"), ("GET", "/auth/login"),
                         ("GET", "/auth/callback")):
        response = await web_client.request(method, path, allow_redirects=False)
        assert response.status == 503


@pytest.mark.asyncio
async def test_api_unavailable_without_any_authentication(web_client, monkeypatch):
    monkeypatch.setattr(api, "ACCESS_TOKEN", "")
    monkeypatch.setattr(api, "ACCESS_SESSION_SECRET", "")
    response = await web_client.get("/api/topics")
    assert response.status == 503
    assert await response.text() == "access disabled"
    assert response.headers["X-Content-Type-Options"] == "nosniff"


@pytest.mark.asyncio
async def test_html_uses_local_login_when_token_is_missing(web_client, monkeypatch):
    from urllib.parse import parse_qs, urlsplit

    monkeypatch.setattr(api, "ACCESS_LOCAL_ENABLED", True)
    monkeypatch.setattr(api, "ACCESS_LOCAL_USERNAME", "admin")
    monkeypatch.setattr(api, "ACCESS_LOCAL_PASSWORD", "secret")
    monkeypatch.setattr(api, "ACCESS_SESSION_SECRET", "session-secret")
    response = await web_client.get(
        "/stats", headers={"X-Access-Token": "", "Accept": "text/html"},
        allow_redirects=False,
    )
    assert response.status == 302
    redirect = urlsplit(response.headers["Location"])
    assert redirect.path == "/login"
    assert parse_qs(redirect.query) == {"next": ["/stats"]}


@pytest.mark.asyncio
async def test_health_distinguishes_stale_active_and_stopped_workers(web_client, monkeypatch):
    import time
    from types import SimpleNamespace

    from core import state

    now = int(time.time())
    monkeypatch.setattr(api, "time", SimpleNamespace(time=lambda: now, monotonic=time.monotonic))
    for topic in ("active", "stale"):
        state.workers[topic] = asyncio.create_task(asyncio.Event().wait())
    stopped = asyncio.create_task(asyncio.sleep(0))
    await stopped
    state.workers["stopped"] = stopped
    state.worker_last_seen.update({
        "active": now - 30, "stale": now - 121,
        "stopped": now - 900, "removed": now - 1000,
    })
    response = await web_client.get("/health")
    assert response.status == 200
    payload = await response.json()
    assert payload["status"] == "degraded"
    assert payload["workers"] == 2
    assert payload["checks"]["workers"] == {"stale": 1, "max_last_seen_age_seconds": 121}


@pytest.mark.asyncio
async def test_health_webhook_without_auth_and_successful_options(web_client):
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from core.http import create_http_session
    from db.targets import create_delivery_target

    received = []

    async def options(request):
        received.append(request)
        return web.Response(status=204)

    upstream = web.Application()
    upstream.router.add_options("/hook", options)
    async with TestServer(upstream) as server:
        await create_http_session()
        await create_delivery_target(
            "webhook", "webhook_generic", {"url": str(server.make_url("/hook"))},
        )
        response = await web_client.get("/health")
        payload = await response.json()
        assert payload["status"] == "ok"
        check = payload["checks"]["targets"]["webhook"]
        assert check["ok"] is True and check["error"] is None
    assert len(received) == 1
    assert "Authorization" not in received[0].headers


@pytest.mark.asyncio
async def test_topic_detail_selects_linked_target_among_multiple(web_client):
    from db.targets import create_delivery_target, set_topic_delivery_target

    await add_topic("news")
    await create_delivery_target("A-default", "telegram", is_default=True)
    chosen = await create_delivery_target("B-topic", "telegram")
    await set_topic_delivery_target("news", chosen)
    response = await web_client.get("/api/topics/news")
    payload = await response.json()
    assert payload["target_id"] == chosen and payload["target_name"] == "B-topic"
    await set_topic_delivery_target("news", None)
    response = await web_client.get("/api/topics/news")
    payload = await response.json()
    assert payload["target_id"] is None and payload["target_name"] is None


@pytest.mark.asyncio
async def test_dlq_batch_rejects_malformed_json_without_mutation(web_client):
    from db.dead_letter import count_dead_letters, move_to_dead_letter
    from db.telegram_queue import count_telegram_queue

    await move_to_dead_letter({"message": "pending"}, 1, "failed")
    response = await web_client.post(
        "/api/queue/dead_letters/requeue_batch", data="{",
        headers={"Content-Type": "application/json"},
    )
    assert response.status == 400
    assert await response.text() == "invalid json"
    assert await count_dead_letters() == 1
    assert await count_telegram_queue() == 0


@pytest.mark.asyncio
async def test_dlq_batch_obeys_limit_and_leaves_remainder(web_client):
    from db.dead_letter import list_dead_letters, move_to_dead_letter
    from db.telegram_queue import count_telegram_queue

    for index in range(3):
        await move_to_dead_letter({"message": str(index)}, 1, "failed")
    letters = await list_dead_letters()
    response = await web_client.post("/api/queue/dead_letters/requeue_batch", json={"limit": 2})
    assert response.status == 200
    assert await response.json() == {"requeued": 2, "failed": 0}
    assert [item["id"] for item in await list_dead_letters()] == [letters[-1]["id"]]
    assert await count_telegram_queue() == 2


@pytest.mark.asyncio
async def test_dlq_batch_handles_item_already_requeued_by_another_request(web_client, monkeypatch):
    from db.dead_letter import count_dead_letters, move_to_dead_letter, requeue_dead_letter
    from db.telegram_queue import count_telegram_queue

    for message in ("first", "second"):
        await move_to_dead_letter({"message": message}, 1, "failed")
    results = []

    async def concurrently_requeued(item_id):
        if not results:
            # Another request moves the first item after this batch has listed it.
            assert await requeue_dead_letter(item_id)
        result = await requeue_dead_letter(item_id)
        results.append(result)
        return result

    monkeypatch.setattr(api, "requeue_dead_letter", concurrently_requeued)
    response = await web_client.post("/api/queue/dead_letters/requeue_batch", json={})
    assert response.status == 200
    assert await response.json() == {"requeued": 1, "failed": 0}
    assert results == [False, True]
    assert await count_dead_letters() == 0
    assert await count_telegram_queue() == 2
