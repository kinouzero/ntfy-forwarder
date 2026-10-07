import asyncio
from unittest.mock import AsyncMock

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer
import pytest
import pytest_asyncio

from core import http, state
from db import dead_letter, settings, targets, telegram_queue, topics
from db.client import db
from services import ntfy, telegram
from services.queue import enqueue_telegram
from services.targets import common, telegram_target, webhook_target
from tasks import aggregation, delivery_sender as sender


@pytest_asyncio.fixture
async def receiver():
    calls = []
    response = {"status": 200, "body": {"ok": True}, "headers": {}}

    async def receive(request):
        calls.append({"path": request.path, "headers": request.headers,
                      "payload": await request.json()})
        return web.json_response(response["body"], status=response["status"],
                                 headers=response["headers"])

    app = web.Application()
    app.router.add_post("/{path:.*}", receive)
    async with TestServer(app) as server:
        await http.create_http_session()
        yield str(server.make_url("/hook")), calls, response


@pytest.mark.parametrize("kind,key", [("webhook_generic", "message"),
                                      ("webhook_discord", "content"),
                                      ("webhook_slack", "text")])
@pytest.mark.asyncio
async def test_webhook_delivery_over_http(initialized_db, receiver, kind, key):
    url, calls, _ = receiver
    await targets.create_delivery_target("webhook", kind,
                                         {"url": url, "auth_header": "Bearer secret"},
                                         is_default=True)
    assert await sender.process_message("hello", attachment_url="https://example.test/file",
                                        topic="news", priority=4)
    assert len(calls) == 1
    assert "hello" in calls[0]["payload"][key]
    assert calls[0]["headers"]["Authorization"] == "Bearer secret"
    if kind == "webhook_generic":
        assert calls[0]["payload"]["priority"] == 4
    else:
        assert "Attachment:" in calls[0]["payload"][key]


@pytest.mark.parametrize("http_status,attempts,status,reason", [
    (200, 8, "sent", None), (429, 8, "retry", "webhook_generic_429"),
    (503, 8, "retry", "webhook_generic_5xx"),
    (403, 8, "dead", "webhook_generic_4xx"), (503, 1, "dead", "webhook_generic_5xx"),
])
@pytest.mark.asyncio
async def test_durable_sender_success_retry_and_dlq(initialized_db, receiver, monkeypatch,
                                                    http_status, attempts, status, reason):
    url, calls, response = receiver
    response["status"] = http_status
    await settings.update_settings({"delivery_queue_max_attempts": attempts})
    await targets.create_delivery_target("webhook", "webhook_generic", {"url": url},
                                         is_default=True)
    await enqueue_telegram({"topic": "news", "message": "hello", "priority": 3})

    async def stop_after_delivery():
        state.shutdown_event.set()
        return await dead_letter.count_dead_letters()

    monkeypatch.setattr(sender, "count_dead_letters", stop_after_delivery)
    await asyncio.wait_for(sender.delivery_sender_loop(), 3)
    assert len(calls) == 1
    conn = await db()
    try:
        rows = await (await conn.execute("SELECT * FROM telegram_queue")).fetchall()
    finally:
        await conn.close()
    letters = await dead_letter.list_dead_letters()
    if status == "retry":
        assert len(rows) == 1 and rows[0]["attempts"] == 1
        assert rows[0]["next_attempt_at"] > 0
    else:
        assert rows == []
    if status == "dead":
        assert len(letters) == 1 and letters[0]["last_error"] == reason
        assert letters[0]["payload"]["message"] == "hello"
    else:
        assert letters == []


@pytest.mark.asyncio
async def test_missing_target_never_acknowledges_message(initialized_db):
    status, delay, reason = await sender.process_queue_item({"topic": "news", "message": "hello"})
    assert status == "retry" and delay > 0 and reason == "target_error"


@pytest.mark.asyncio
async def test_invalid_target_configs_are_permanent_errors(monkeypatch):
    for config in ({}, {"chat_id": "1"}):
        with pytest.raises(common.DeliveryError) as exc:
            await sender.send_telegram_message({"config": config}, "hello")
        assert not exc.value.retryable
    for target in ({"config": {}}, {"config": {"url": "http://x"}, "kind": "bad"}):
        with pytest.raises(common.DeliveryError) as exc:
            await sender.send_webhook_kind(target, "hello")
        assert not exc.value.retryable
    monkeypatch.setattr(sender, "resolve_delivery_target_for_topic",
                        AsyncMock(return_value={"kind": "unsupported"}))
    with pytest.raises(common.DeliveryError):
        await sender.process_message("hello")


@pytest.mark.parametrize("item,expected", [
    (("topic", "hello"), (None, 0, "topic", "hello", None, 3)),
    ({"id": 1, "payload": {"message": "hi"}, "attempts": 2}, (1, 2, None, "hi", None, 3)),
    ("legacy", (None, 0, None, "legacy", None, 3)),
])
def test_unpack_legacy_items(item, expected):
    assert sender._unpack_queue_item(item) == expected


@pytest.mark.parametrize("error_type,prefix", [(telegram.TelegramAPIError, "telegram"),
                                               (common.DeliveryError, "test")])
@pytest.mark.parametrize("code,suffix", [(None, None), (400, "4xx"), (429, "429"), (500, "5xx")])
def test_failure_classification(error_type, prefix, code, suffix):
    args = ("test", "failed") if error_type is common.DeliveryError else ("failed",)
    exc = error_type(*args, status_code=code, retryable=False)
    expected = suffix or ("error" if prefix == "test" else "api")
    assert sender._reason_from_exception(exc) == f"{prefix}_{expected}"
    assert not sender._is_retryable(exc)
    assert sender._is_retryable(OSError())
    assert sender._reason_from_exception(OSError()) == "network_or_unknown"


def test_retry_after_respects_provider_minimum():
    exc = common.DeliveryError("test", "rate limited", retry_after=600)
    assert sender._compute_retry_delay_seconds(1, exc, settings.SETTINGS_DEFAULTS) == 600
    assert sender._compute_retry_delay_seconds(10, OSError(), settings.SETTINGS_DEFAULTS) == 300


@pytest.mark.asyncio
async def test_sender_empty_drop_and_outer_failure(initialized_db, monkeypatch):
    assert await sender.process_queue_item({"payload": {}}) == ("drop", None, None)
    sleep = AsyncMock(side_effect=lambda _: state.shutdown_event.set())
    monkeypatch.setattr(sender.asyncio, "sleep", sleep)
    await sender.delivery_sender_loop()
    assert sleep.call_args.args == (sender.QUEUE_POLL_IDLE_SECONDS,)
    state.shutdown_event.clear()
    monkeypatch.setattr(sender, "get_next_telegram_item", AsyncMock(side_effect=OSError("offline")))
    await sender.delivery_sender_loop()
    assert sleep.call_args.args == (5,)


@pytest.mark.asyncio
async def test_transport_session_missing_and_network_errors(monkeypatch):
    monkeypatch.setattr(common, "get_http_session", lambda: None)
    with pytest.raises(common.DeliveryError, match="not ready"):
        await common.post_json("test", "http://invalid", {})
    monkeypatch.setattr(telegram, "get_http_session", lambda: None)
    with pytest.raises(RuntimeError, match="not ready"):
        await telegram.tg_call("sendMessage", {}, "token")

    class OfflineSession:
        def post(self, *args, **kwargs):
            raise aiohttp.ClientConnectionError("offline")

    monkeypatch.setattr(common, "get_http_session", OfflineSession)
    monkeypatch.setattr(telegram, "get_http_session", OfflineSession)
    with pytest.raises(common.DeliveryError) as exc:
        await common.post_json("test", "http://invalid", {})
    assert exc.value.retryable
    with pytest.raises(telegram.TelegramAPIError) as exc:
        await telegram.tg_call("sendMessage", {}, "token")
    assert exc.value.retryable


@pytest.mark.asyncio
async def test_telegram_server_error_over_http(receiver, monkeypatch):
    url, _, response = receiver
    response.update(status=502, body={"description": "upstream failed"})

    class ProxySession:
        def post(self, ignored_url, **kwargs):
            return http.get_http_session().post(url, **kwargs)

    monkeypatch.setattr(telegram, "get_http_session", ProxySession)
    with pytest.raises(telegram.TelegramAPIError) as exc:
        await telegram.tg_call("sendMessage", {}, "token")
    assert exc.value.status_code == 502 and exc.value.retryable


@pytest.mark.asyncio
async def test_telegram_target_missing_credentials_and_permanent_error(monkeypatch):
    for kwargs in ({}, {"chat_id": "1"}):
        with pytest.raises(telegram.TelegramAPIError) as exc:
            await telegram_target.send_telegram_message("hello", **kwargs)
        assert not exc.value.retryable
    monkeypatch.setattr(telegram_target, "tg_call", AsyncMock(
        side_effect=telegram.TelegramAPIError("forbidden", status_code=403)))
    with pytest.raises(telegram.TelegramAPIError, match="forbidden"):
        await telegram_target.send_telegram_message("hello", chat_id="1", bot_token="fake")


@pytest.mark.asyncio
async def test_ntfy_stream_flushes_idle_batch_and_tracks_heartbeats(initialized_db, monkeypatch):
    await topics.add_topic("news")
    await settings.update_settings({"db_batch_size": 10, "quiet_hours_start": 0,
                                   "quiet_hours_end": 0, "db_batch_flush_seconds": 1})
    release = asyncio.Event()
    requests = []

    async def stream(request):
        requests.append(request)
        response = web.StreamResponse()
        await response.prepare(request)
        for value in [b"\n", b"invalid\n", b"[]\n",
                      b'{"event":"message","priority":"bad"}\n',
                      b'{"event":"keepalive"}\n',
                      b'{"event":"message","id":"1","message":"hello"}\n',
                      b'{"event":"message","id":"1","message":"hello"}\n']:
            await response.write(value)
        await release.wait()
        return response

    app = web.Application()
    app.router.add_get("/news/json", stream)
    async with TestServer(app) as server:
        monkeypatch.setattr(ntfy, "NTFY_BASE_URL", str(server.make_url("")).rstrip("/"))
        monkeypatch.setattr(ntfy, "NTFY_TOKEN", "fake-token")
        await http.create_http_session()
        task = asyncio.create_task(ntfy.ntfy_worker("news"))
        try:
            async with asyncio.timeout(4):
                while not state.aggregation_buffer.get("news"):
                    await asyncio.sleep(0.02)
            assert len(state.aggregation_buffer["news"]) == 1
            assert state.topic_stats["news"]["inserted"] == 1
            assert state.worker_last_seen["news"] > 0
            assert requests[0].headers["Authorization"] == "Bearer fake-token"
            await aggregation.flush_aggregation()
            row = await telegram_queue.get_next_telegram_item()
            assert "hello" in row["payload"]["message"]
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_ntfy_rejects_http_errors_and_backs_off(initialized_db, monkeypatch):
    async def denied(request):
        return web.Response(status=401)

    app = web.Application()
    app.router.add_get("/news/json", denied)
    async with TestServer(app) as server:
        await http.create_http_session()
        monkeypatch.setattr(ntfy, "NTFY_BASE_URL", str(server.make_url("")).rstrip("/"))
        sleep = AsyncMock(side_effect=lambda _: state.shutdown_event.set())
        monkeypatch.setattr(ntfy.asyncio, "sleep", sleep)
        await ntfy.ntfy_worker("news")
        assert state.topic_stats["news"]["errors"] == 1
        assert 1 <= sleep.call_args.args[0] < 2


def test_webhook_payload_defaults():
    assert common.join_message(None) == ""
    assert webhook_target.webhook_payload("generic", "hi")["priority"] == 3


@pytest.mark.parametrize("header,expected", [("600", 600), ("1.5", 2), ("invalid", None)])
@pytest.mark.asyncio
async def test_webhook_retry_after_is_preserved(receiver, header, expected):
    url, _, response = receiver
    response.update(status=429, headers={"Retry-After": header})
    with pytest.raises(common.DeliveryError) as exc:
        await common.post_json("test", url, {})
    assert exc.value.retry_after == expected


@pytest.mark.asyncio
async def test_runtime_retry_settings_apply_without_restart(initialized_db):
    payload = {"topic": "news", "message": "waiting for target"}
    initial = await sender.process_queue_item(payload)
    assert initial == ("retry", 5, "target_error")
    await settings.update_settings({"delivery_queue_base_retry_seconds": 27})
    updated = await sender.process_queue_item(payload)
    assert updated == ("retry", 27, "target_error")


@pytest.mark.parametrize("legacy", ["legacy message", ("news", "legacy message")])
@pytest.mark.asyncio
async def test_sender_skips_sentinel_and_delivers_legacy_memory_item(
    initialized_db, receiver, monkeypatch, legacy,
):
    url, calls, _ = receiver
    await targets.create_delivery_target(
        "webhook", "webhook_generic", {"url": url}, is_default=True,
    )
    await state.telegram_queue.put(None)
    await state.telegram_queue.put(legacy)
    ack = AsyncMock(side_effect=AssertionError("memory-only items have no database ID"))
    monkeypatch.setattr(sender, "ack_telegram_item", ack)

    async def stop_after_delivery():
        state.shutdown_event.set()
        return await dead_letter.count_dead_letters()

    monkeypatch.setattr(sender, "count_dead_letters", stop_after_delivery)
    await asyncio.wait_for(sender.delivery_sender_loop(), 5)
    assert len(calls) == 1
    assert calls[0]["payload"]["message"] == "legacy message"
    assert state.telegram_queue.empty()
    assert await telegram_queue.count_telegram_queue() == 0
    ack.assert_not_awaited()


@pytest.mark.asyncio
async def test_ntfy_reconnects_after_eof_and_flushes_both_batches(initialized_db, monkeypatch):
    await topics.add_topic("news")
    await settings.update_settings({
        "db_batch_size": 10, "db_batch_flush_seconds": 100,
        "quiet_hours_start": 0, "quiet_hours_end": 0,
    })
    received = []

    async def upstream(request):
        received.append(request)
        event_id = str(len(received))
        if len(received) == 2:
            state.shutdown_event.set()
        return web.json_response({"event": "message", "id": event_id, "message": event_id})

    upstream_app = web.Application()
    upstream_app.router.add_get("/news/json", upstream)
    async with TestServer(upstream_app) as server:
        monkeypatch.setattr(ntfy, "NTFY_BASE_URL", str(server.make_url("")).rstrip("/"))
        await http.create_http_session()
        await asyncio.wait_for(ntfy.ntfy_worker("news"), 5)
    assert len(received) == 2
    assert [event.event_id for event in state.aggregation_buffer["news"]] == ["1", "2"]
    assert state.topic_stats["news"]["inserted"] == 2
    assert state.topic_stats["news"]["errors"] == 0
