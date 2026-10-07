import asyncio
import json
import ssl
import time
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import make_mocked_request

from api import web as api
from core import state
from db import dead_letter, settings, targets, telegram_queue, topics
from models.event import NtfyEvent
from services import ntfy
from services.formatter import build_message
from tasks import aggregation, daily_summary, delivery_sender, digest, retention
from utils.telegram import split_message


class StreamSession:
    def __init__(self, entries):
        self.entries = entries

    def get(self, *args, **kwargs):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def raise_for_status(self):
        pass

    @property
    def content(self):
        async def entries():
            for entry in self.entries:
                if callable(entry):
                    await entry()
                else:
                    yield json.dumps(entry).encode() + b"\n"
            state.shutdown_event.set()
        return entries()


@pytest.mark.asyncio
async def test_ntfy_quiet_hours_and_buffer_limits(initialized_db, monkeypatch):
    await topics.add_topic("news")
    await settings.update_settings({"max_aggregation_buffer": 10, "max_digest_buffer": 10})
    monkeypatch.setattr(ntfy, "in_quiet_hours_window", lambda *args: True)
    state.digest_buffer.update({str(i): 1 for i in range(10)})
    entries = [{"event": "message", "id": str(i), "priority": 4, "message": str(i)}
               for i in range(11)]
    entries.append({"event": "message", "id": "quiet", "priority": 3})
    monkeypatch.setattr(ntfy, "get_http_session", lambda: StreamSession(entries))
    await ntfy.ntfy_worker("news")
    assert len(state.aggregation_buffer["news"]) == 10
    assert state.aggregation_buffer["news"][0].event_id == "1"
    assert "news" not in state.digest_buffer
    assert state.topic_stats["news"]["inserted"] == 11
    assert state.topic_stats["news"]["filtered"] == 1


@pytest.mark.asyncio
async def test_ntfy_batch_paused_before_flush_moves_to_dlq(initialized_db, monkeypatch):
    await topics.add_topic("news")
    await settings.update_settings({"db_batch_size": 10, "db_batch_flush_seconds": 100,
                                   "quiet_hours_start": 0, "quiet_hours_end": 0})

    async def pause():
        await topics.set_topic_enabled("news", False)

    entries = [{"event": "message", "id": "first", "message": "hello"}, pause]
    monkeypatch.setattr(ntfy, "get_http_session", lambda: StreamSession(entries))
    await ntfy.ntfy_worker("news")
    letters = await dead_letter.list_dead_letters()
    assert len(letters) == 1 and letters[0]["last_error"] == "topic_disabled"
    assert state.topic_stats["news"]["disabled"] == 1


@pytest.mark.asyncio
async def test_ntfy_eof_flushes_partial_batch(initialized_db, monkeypatch):
    await settings.update_settings({"db_batch_size": 10, "db_batch_flush_seconds": 100,
                                   "quiet_hours_start": 0, "quiet_hours_end": 0})
    monkeypatch.setattr(ntfy, "get_http_session", lambda: StreamSession([
        {"event": "message", "id": "first", "message": "hello"},
    ]))
    await ntfy.ntfy_worker("news")
    assert state.aggregation_buffer["news"][0].message == "hello"


@pytest.mark.asyncio
async def test_ntfy_waits_when_session_missing(monkeypatch):
    monkeypatch.setattr(ntfy, "get_http_session", lambda: None)
    sleep = AsyncMock(side_effect=lambda _: state.shutdown_event.set())
    monkeypatch.setattr(ntfy.asyncio, "sleep", sleep)
    await ntfy.ntfy_worker("news")
    sleep.assert_awaited_once_with(1)


@pytest.mark.asyncio
async def test_concurrent_dead_letter_requeue_is_atomic(initialized_db):
    await dead_letter.move_to_dead_letter(
        {"event": "message", "message": "raw", "title": "title",
         "attachment": {"url": "https://example.test/file"}},
        0, "topic_disabled", topic="news",
    )
    letter = (await dead_letter.list_dead_letters())[0]
    results = await asyncio.gather(
        *(dead_letter.requeue_dead_letter(letter["id"]) for _ in range(2))
    )
    assert sorted(results) == [False, True]
    assert await dead_letter.count_dead_letters() == 0
    assert await telegram_queue.count_telegram_queue() == 1
    queued = (await telegram_queue.get_next_telegram_item())["payload"]
    assert queued["topic"] == "news"
    assert "title" in queued["message"]
    assert queued["attachment_url"] == "https://example.test/file"


@pytest.mark.asyncio
async def test_dlq_batch_failure_retains_original(web_client, monkeypatch):
    await dead_letter.move_to_dead_letter({"message": "hello"}, 2, "retry", topic="news")
    monkeypatch.setattr(api, "requeue_dead_letter", AsyncMock(side_effect=OSError("db locked")))
    response = await web_client.post("/api/queue/dead_letters/requeue_batch", json={"limit": -1})
    assert (await response.json()) == {"requeued": 0, "failed": 1}
    assert await dead_letter.count_dead_letters() == 1
    response = await web_client.post("/api/queue/dead_letters/clear", data="invalid")
    assert response.status == 200
    for payload in ([], "broken"):
        response = await web_client.post("/api/queue/dead_letters/requeue_batch", json=payload)
        assert response.status == 400


@pytest.mark.asyncio
async def test_topic_target_and_runtime_stats_are_visible(web_client):
    await topics.add_topic("news")
    target = await targets.create_delivery_target("alerts", "telegram", {"chat_id": "1"})
    await targets.set_topic_delivery_target("news", target)
    state.worker_last_seen["news"] = int(time.time()) - 200
    state.worker_last_seen["missing"] = 1
    state.workers["news"] = asyncio.create_task(asyncio.Event().wait())
    for path in ("/api/topics", "/api/topics/news", "/api/stats"):
        response = await web_client.get(path)
        assert response.status == 200
        payload = await response.json()
        if path == "/api/stats":
            assert payload["stale_workers"] == 1
    state.topic_stats["news"] = {"received": 1}
    state.recent_events["news"] = [1]
    state.topic_rates["news"] = [1]
    response = await web_client.post("/api/topics/hard_clear_all")
    assert response.status == 200
    assert state.topic_stats["news"]["received"] == 0
    assert not state.recent_events["news"] and not state.topic_rates["news"]
    response = await web_client.put(f"/api/targets/{target}", data="broken")
    assert response.status == 400


@pytest.mark.asyncio
async def test_health_target_checks_cache_and_failures(web_client, monkeypatch):
    calls = []

    class Response:
        status = 503

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    class Session:
        def options(self, url, **kwargs):
            calls.append((url, kwargs))
            return Response()

    await targets.create_delivery_target("good", "telegram", {"bot_token": "fake"})
    await targets.create_delivery_target("missing-token", "telegram")
    await targets.create_delivery_target("webhook", "webhook_generic",
                                         {"url": "http://example.test", "auth_header": "Bearer x"})
    await targets.create_delivery_target("missing-url", "webhook_generic")
    monkeypatch.setattr(api, "tg_call", AsyncMock(return_value={"ok": True}))
    monkeypatch.setattr(api, "get_http_session", Session)
    for _ in range(2):
        response = await web_client.get("/health")
        body = await response.json()
        assert body["status"] == "degraded"
        checks = body["checks"]["targets"]
        assert checks["good"]["ok"] is True
        assert checks["webhook"]["error"] == "http_503"
    assert len(calls) == 1
    monkeypatch.setattr(api, "_HEALTH_TARGET_CACHE", {})
    monkeypatch.setattr(api, "get_http_session", lambda: None)
    response = await web_client.get("/health")
    checks = (await response.json())["checks"]["targets"]
    assert checks["webhook"]["error"] == "HTTP session not ready"
    monkeypatch.setattr(api, "list_delivery_targets", AsyncMock(side_effect=OSError("offline")))
    assert (await web_client.get("/health")).status == 200


@pytest.mark.asyncio
async def test_daily_summary_disabled_and_failed(initialized_db, monkeypatch):
    assert "no events" in await daily_summary._build_daily_summary()
    monkeypatch.setattr(daily_summary.asyncio, "sleep",
                        AsyncMock(side_effect=lambda _: state.shutdown_event.set()))
    await settings.update_settings({"daily_summary_enabled": False})
    await daily_summary.daily_summary_loop()
    state.shutdown_event.clear()
    await settings.update_settings({"daily_summary_enabled": True})
    monkeypatch.setattr(daily_summary, "_should_send_today", lambda *a, **kw: True)
    enqueue = AsyncMock(side_effect=OSError("offline"))
    monkeypatch.setattr(daily_summary, "enqueue_telegram", enqueue)
    await daily_summary.daily_summary_loop()
    enqueue.assert_awaited_once()


@pytest.mark.asyncio
async def test_digest_preserves_arrivals_during_send(monkeypatch):
    state.digest_buffer["news"] = 11

    async def enqueue(payload):
        state.digest_buffer["news"] += 3
        state.digest_buffer["new-topic"] = 1
        state.shutdown_event.set()

    monkeypatch.setattr(digest, "enqueue_telegram", enqueue)
    monkeypatch.setattr(digest, "is_topic_enabled", AsyncMock(return_value=True))
    monkeypatch.setattr(digest.asyncio, "sleep", AsyncMock())
    await digest.digest_loop()
    assert state.digest_buffer == {"news": 3, "new-topic": 1}


@pytest.mark.asyncio
async def test_stopped_loops_and_empty_aggregation(initialized_db):
    state.aggregation_buffer["empty"] = []
    await aggregation.flush_aggregation()
    state.shutdown_event.set()
    await digest.digest_loop()
    await retention.retention_loop()
    await daily_summary.daily_summary_loop()
    await delivery_sender.delivery_sender_loop()


def test_formatting_edge_cases():
    assert build_message(NtfyEvent("topic", "hello", attachment={"name": "file"}))
    for text in ("abcdefgh\nijklmnop", "abcdefgh ijklmnop", "abcdefgh\n\nijklmnop"):
        chunks = split_message(text, 10)
        assert len(chunks) == 2 and all(len(chunk) <= 10 for chunk in chunks)
    assert split_message("a" * 8 + "\n\n", 9) == ["a" * 8]


@pytest.mark.parametrize("tls,forwarded_proto,expected_secure", [
    (False, None, False),
    (True, None, True),
    (False, "http", False),
    (False, "https", True),
    (False, "HTTPS", True),
    (True, "http", True),
])
def test_request_security_uses_tls_transport_or_forwarded_header(
    tls, forwarded_proto, expected_secure,
):
    headers = {"Cookie": "access_token=x"}
    if forwarded_proto is not None:
        headers["X-Forwarded-Proto"] = forwarded_proto
    # An HTTPS URL alone does not establish TLS on a mocked server request.
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER) if tls else None
    request = make_mocked_request("GET", "/", headers=headers, sslcontext=context)
    assert request.scheme == ("https" if tls else "http")
    assert api._get_access_token(request) == "x"
    assert api._request_is_secure(request) is expected_secure
    assert api._cookie_kwargs(request, 60)["secure"] is expected_secure


@pytest.mark.asyncio
async def test_ntfy_idle_ticks_do_not_flush_batch_before_deadline(initialized_db, monkeypatch):
    from types import SimpleNamespace

    from db.messages import count_messages_by_topic_since

    await topics.add_topic("news")
    await settings.update_settings({
        "db_batch_size": 10, "db_batch_flush_seconds": 10,
        "quiet_hours_start": 0, "quiet_hours_end": 0,
    })
    clock = {"now": 100.0}
    monkeypatch.setattr(ntfy, "time", SimpleNamespace(
        time=time.time, monotonic=lambda: clock["now"],
    ))
    monkeypatch.setattr(ntfy, "get_http_session", lambda: StreamSession([]))

    async def timed_stream(content):
        yield None  # Idle with no message must not create an empty batch.
        assert await count_messages_by_topic_since(0) == {}
        yield b'{"event":"message","id":"first","message":"hello"}\n'
        clock["now"] = 109.0
        yield None
        assert await count_messages_by_topic_since(0) == {}
        assert not state.aggregation_buffer.get("news")
        clock["now"] = 110.0
        yield None
        assert await count_messages_by_topic_since(0) == {"news": 1}
        state.shutdown_event.set()

    monkeypatch.setattr(ntfy, "_stream_lines", timed_stream)
    await ntfy.ntfy_worker("news")
    assert [event.message for event in state.aggregation_buffer["news"]] == ["hello"]
