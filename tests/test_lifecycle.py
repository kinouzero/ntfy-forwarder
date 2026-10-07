import asyncio
import gzip
import sqlite3
from unittest.mock import AsyncMock, MagicMock

import pytest

import app
from core import http, state
from db import topics
from models.event import NtfyEvent
from tasks import aggregation, backup, db_maintenance, monitor, daily_summary
from utils import filters, markdown, quiet_hours, telegram
from utils.rate_limit import RateLimiter


@pytest.mark.parametrize("url", ["", "ntfy", "file:///tmp/ntfy", "http://"])
def test_validate_config_rejects_invalid_urls(monkeypatch, url):
    monkeypatch.setattr(app, "NTFY_BASE_URL", url)
    with pytest.raises(RuntimeError, match="NTFY_BASE_URL"):
        app.validate_config()


@pytest.mark.asyncio
async def test_bootstrap_is_idempotent_and_shutdown_cleans_dynamic_workers(initialized_db,
                                                                           monkeypatch):
    await topics.add_topic("alpha")
    await topics.add_topic("beta")

    async def idle(topic):
        await asyncio.Event().wait()

    monkeypatch.setattr(app, "ntfy_worker", idle)
    await app.bootstrap_topics()
    first = dict(state.workers)
    await app.bootstrap_topics()
    assert state.workers == first
    dynamic = asyncio.create_task(idle("dynamic"))
    state.workers["dynamic"] = dynamic
    await app.shutdown()
    assert dynamic.cancelled()
    assert all(task.cancelled() for task in first.values())
    assert not app.running_tasks and not state.workers


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "site", "background", "plugins"])
async def test_main_cleans_resources_on_shutdown_and_failure(initialized_db, monkeypatch, failure):
    tasks = []

    async def idle():
        tasks.append(asyncio.current_task())
        if failure == "background":
            raise OSError("background failed")
        await asyncio.Event().wait()

    for name in ("delivery_sender_loop", "daily_summary_loop", "aggregation_loop", "digest_loop",
                 "retention_loop", "backup_loop", "worker_monitor_loop", "db_maintenance_loop"):
        monkeypatch.setattr(app, name, idle)
    runner = MagicMock(setup=AsyncMock(), cleanup=AsyncMock())
    monkeypatch.setattr(app.web, "AppRunner", lambda _: runner)

    async def start():
        if failure == "site":
            raise OSError("port unavailable")
        if failure is None:
            app.stop()

    monkeypatch.setattr(app.web, "TCPSite", lambda *a: MagicMock(start=start))
    if failure == "plugins":
        monkeypatch.setattr(app, "load_plugins", AsyncMock(side_effect=OSError("plugin failed")))
    if failure:
        with pytest.raises((OSError, RuntimeError)):
            await app.main()
    else:
        await app.main()
    assert all(task.done() for task in tasks)
    assert http.get_http_session() is None
    assert not app.running_tasks
    if failure != "plugins":
        runner.cleanup.assert_awaited_once()


def test_run_closes_loop_on_error(monkeypatch):
    calls = []

    async def fail():
        raise RuntimeError("startup failed")

    loop = asyncio.new_event_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda *args: calls.append(args))
    monkeypatch.setattr(app.asyncio, "new_event_loop", lambda: loop)
    monkeypatch.setattr(app, "main", fail)
    with pytest.raises(RuntimeError, match="startup failed"):
        app.run()
    assert loop.is_closed() and len(calls) == 2


@pytest.mark.asyncio
async def test_http_session_reused_and_recreated():
    session = await http.create_http_session()
    assert await http.create_http_session() is session
    await session.close()
    replacement = await http.create_http_session()
    assert replacement is not session and not replacement.closed
    await http.close_http_session()
    await http.close_http_session()


@pytest.mark.asyncio
async def test_aggregation_preserves_concurrent_arrivals(initialized_db, monkeypatch):
    first = NtfyEvent("news", "first")
    second = NtfyEvent("news", "arrived-during-flush")
    state.aggregation_buffer["news"] = [first]
    sent = []

    async def enqueue(payload):
        sent.append(payload)
        state.aggregation_buffer["news"].append(second)

    monkeypatch.setattr(aggregation, "enqueue_telegram", enqueue)
    await aggregation.flush_aggregation()
    assert len(sent) == 1 and "first" in sent[0]["message"]
    assert state.aggregation_buffer["news"] == [second]


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [OSError("db unavailable"), asyncio.CancelledError()])
async def test_aggregation_restores_only_unsent_messages(initialized_db, monkeypatch, error):
    events = [NtfyEvent("news", str(i)) for i in range(3)]
    state.aggregation_buffer["news"] = list(events)
    enqueue = AsyncMock(side_effect=[1, error])
    monkeypatch.setattr(aggregation, "enqueue_telegram", enqueue)
    with pytest.raises(type(error)):
        await aggregation.flush_aggregation()
    assert state.aggregation_buffer["news"] == events[1:]
    assert enqueue.await_count == 2


@pytest.mark.asyncio
async def test_aggregation_loop_survives_failure(initialized_db, monkeypatch):
    monkeypatch.setattr(aggregation, "flush_aggregation", AsyncMock(side_effect=OSError("busy")))
    sleep = AsyncMock(side_effect=[None, lambda _: None])

    async def stop_on_retry(seconds):
        if seconds == 1:
            state.shutdown_event.set()

    sleep.side_effect = stop_on_retry
    monkeypatch.setattr(aggregation.asyncio, "sleep", sleep)
    await aggregation.aggregation_loop()
    assert sleep.await_count == 2


@pytest.mark.asyncio
async def test_backup_includes_uncheckpointed_wal(tmp_path, monkeypatch):
    path = tmp_path / "live.db"
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        conn.execute("CREATE TABLE data(value TEXT)")
        conn.execute("INSERT INTO data VALUES ('committed in WAL')")
        conn.commit()
        monkeypatch.setattr(backup, "DB_PATH", str(path))
        monkeypatch.setattr(backup, "BACKUP_DIR", str(tmp_path / "backups"))
        result = await asyncio.to_thread(backup.create_backup)
        restored = tmp_path / "restored.db"
        restored.write_bytes(gzip.decompress(result.with_suffix(".db.gz").read_bytes()))
        restore = sqlite3.connect(restored)
        try:
            assert restore.execute("PRAGMA integrity_check").fetchone() == ("ok",)
            assert restore.execute("SELECT * FROM data").fetchone() == ("committed in WAL",)
        finally:
            restore.close()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_periodic_tasks_log_errors_and_continue(initialized_db, monkeypatch):
    sleep = AsyncMock(side_effect=lambda _: state.shutdown_event.set())
    monkeypatch.setattr(asyncio, "sleep", sleep)
    monkeypatch.setattr(asyncio, "to_thread", AsyncMock(side_effect=OSError("unavailable")))
    await backup.backup_loop()
    assert sleep.call_args.args == (86400,)
    state.shutdown_event.clear()
    await db_maintenance.db_maintenance_loop()
    assert sleep.call_args.args == (3600,)
    state.shutdown_event.clear()
    monkeypatch.setattr(asyncio, "to_thread", AsyncMock())
    await db_maintenance.db_maintenance_loop()


@pytest.mark.asyncio
async def test_monitor_restarts_crashed_worker_without_previous_traffic(monkeypatch):
    async def idle(topic):
        await asyncio.Event().wait()

    done = asyncio.create_task(asyncio.sleep(0))
    await done
    healthy = asyncio.create_task(idle("healthy"))
    state.workers.update(crashed=done, healthy=healthy)
    monkeypatch.setattr(monitor, "ntfy_worker", idle)
    monkeypatch.setattr(monitor.asyncio, "sleep",
                        AsyncMock(side_effect=lambda _: state.shutdown_event.set()))
    await monitor.worker_monitor_loop()
    assert state.workers["crashed"] is not done
    assert state.workers["healthy"] is healthy
    assert state.worker_last_seen["crashed"] > 0


def test_rate_limiter_boundaries_and_disabled():
    limiter = RateLimiter(2, 10)
    assert limiter.allow(0) and limiter.allow(1)
    assert not limiter.allow(9)
    assert limiter.allow(10)
    assert not limiter.allow(10)
    assert limiter.allow(11)
    assert RateLimiter(0, 10).allow()
    assert RateLimiter(1, 10).allow()


def test_formatting_and_timezone_boundaries(monkeypatch):
    assert filters.passes_filters("any")
    assert markdown.escape_md(None) == ""
    assert telegram.split_message(None, 1) == [""]
    for size in (0, -1):
        with pytest.raises(ValueError):
            telegram.split_message("hello", size)
    monkeypatch.setattr(quiet_hours, "TZ", "invalid/timezone")
    assert str(quiet_hours._quiet_tz()) == "UTC"
    assert 0 <= quiet_hours._now_hour() <= 23
    monkeypatch.setattr(daily_summary, "TZ", "invalid/timezone")
    assert str(daily_summary._summary_tz()) == "UTC"
    from datetime import datetime
    assert daily_summary._is_summary_time(datetime(2026, 1, 1, 8, 0))


@pytest.mark.asyncio
async def test_real_application_starts_serves_health_and_stops(initialized_db, monkeypatch):
    from aiohttp import ClientSession
    from tasks import backup, db_maintenance
    from db.settings import update_settings

    await update_settings({"daily_summary_enabled": False})
    monkeypatch.setattr(backup, "DB_PATH", str(initialized_db["db_path"]))
    monkeypatch.setattr(backup, "BACKUP_DIR", str(initialized_db["backup_dir"]))
    monkeypatch.setattr(db_maintenance, "DB_PATH", str(initialized_db["db_path"]))
    original_site = app.web.TCPSite
    ready = asyncio.Event()
    endpoint = {}

    class EphemeralSite(original_site):
        def __init__(self, runner, host, port):
            super().__init__(runner, "127.0.0.1", 0)

        async def start(self):
            await super().start()
            endpoint["url"] = f"http://127.0.0.1:{self._server.sockets[0].getsockname()[1]}"
            ready.set()

    monkeypatch.setattr(app.web, "TCPSite", EphemeralSite)
    task = asyncio.create_task(app.main())
    try:
        await asyncio.wait_for(ready.wait(), 3)
        async with ClientSession() as session:
            async with session.get(endpoint["url"] + "/health") as response:
                assert response.status == 200
                assert (await response.json())["status"] == "ok"
    finally:
        app.stop()
        await asyncio.wait_for(task, 3)
    assert http.get_http_session() is None
    assert not app.running_tasks


def test_entrypoint_propagates_database_startup_failure_and_closes_loop(monkeypatch):
    import runpy
    import signal

    from db import schema

    init_db = AsyncMock(side_effect=OSError("database path is not writable"))
    monkeypatch.setattr(schema, "init_db", init_db)
    loop = asyncio.new_event_loop()
    handlers = {}
    monkeypatch.setattr(loop, "add_signal_handler", lambda sig, fn: handlers.setdefault(sig, fn))
    monkeypatch.setattr(app.asyncio, "new_event_loop", lambda: loop)
    create_session = AsyncMock()
    monkeypatch.setattr(http, "create_http_session", create_session)
    try:
        with pytest.raises(OSError, match="database path is not writable"):
            runpy.run_path(app.__file__, run_name="__main__")
        init_db.assert_awaited_once()
        create_session.assert_not_awaited()
        assert set(handlers) == {signal.SIGINT, signal.SIGTERM}
        assert loop.is_closed()
        assert state.shutdown_event.is_set()
        assert http.get_http_session() is None
    finally:
        if not loop.is_closed():
            loop.close()
