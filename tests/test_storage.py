import asyncio
import sqlite3

import pytest

from db import targets, settings, topics, messages, dead_letter, errors, telegram_queue
from db.client import db
from db.schema import init_db
from services.queue import enqueue_telegram
from core.state import telegram_queue as memory_queue


@pytest.mark.asyncio
async def test_target_lifecycle_and_routing(initialized_db):
    assert await targets.resolve_delivery_target_for_topic("news") is None
    default = await targets.create_delivery_target(
        "default", "webhook_generic", {"url": "http://a"},
        is_default=True)
    custom = await targets.create_delivery_target("custom", "telegram", {"chat_id": "1"})
    assert (await targets.resolve_delivery_target_for_topic("news"))["id"] == default
    await targets.set_topic_delivery_target("news", custom)
    assert await targets.get_topic_delivery_target_id("news") == custom
    assert await targets.list_topic_delivery_target_ids() == {"news": custom}
    assert (await targets.resolve_delivery_target_for_topic("news"))["id"] == custom
    await targets.update_delivery_target(custom, name="renamed", kind="webhook_slack",
                                         config={"url": "http://b"}, enabled=False)
    assert (await targets.resolve_delivery_target_for_topic("news"))["id"] == default
    assert len(await targets.list_delivery_targets(include_disabled=False)) == 1
    await targets.set_default_delivery_target(custom)
    assert await targets.resolve_delivery_target_for_topic("unknown") is None
    await targets.update_delivery_target(custom, enabled=True)
    assert (await targets.get_default_delivery_target())["name"] == "renamed"
    await targets.set_topic_delivery_target("news", None)
    assert await targets.get_topic_delivery_target_id("news") is None
    await targets.set_topic_delivery_target("news", default)
    await targets.delete_delivery_target(default)
    assert await targets.list_topic_delivery_target_ids() == {}
    assert await targets.get_delivery_target(default) is None
    await targets.set_default_delivery_target(None)
    assert await targets.get_default_delivery_target() is None
    await targets.update_delivery_target(custom)


@pytest.mark.asyncio
async def test_failed_target_transaction_keeps_default_and_releases_lock(initialized_db):
    first = await targets.create_delivery_target("first", "telegram", is_default=True)
    with pytest.raises(sqlite3.IntegrityError):
        await targets.create_delivery_target("first", "telegram", is_default=True)
    second = await asyncio.wait_for(targets.create_delivery_target("second", "telegram"), 2)
    assert (await targets.get_default_delivery_target())["id"] == first
    with pytest.raises(sqlite3.IntegrityError):
        await targets.update_delivery_target(second, name="first")
    await asyncio.wait_for(targets.update_delivery_target(second, name="third"), 2)


@pytest.mark.parametrize("value,expected", [(None, {}), ("bad", {}), ("[]", {}),
                                            ('{"a":1}', {"a": 1})])
def test_load_target_config(value, expected):
    assert targets._loads_config(value) == expected


@pytest.mark.asyncio
async def test_settings_persistence_sanitization_and_cache(initialized_db):
    defaults = await settings.get_settings_snapshot()
    assert defaults == settings.SETTINGS_DEFAULTS
    values = await settings.update_settings({
        "quiet_hours_start": 99, "quiet_hours_end": -1,
        "daily_summary_enabled": "off", "daily_summary_hour": 88,
        "daily_summary_minute": 100, "db_batch_size": 0,
        "retention_days": "invalid", "unknown": 12,
    })
    assert values["quiet_hours_start"] == 23 and values["quiet_hours_end"] == 0
    assert values["daily_summary_enabled"] is False
    assert values["daily_summary_hour"] == 23 and values["daily_summary_minute"] == 59
    assert values["db_batch_size"] == 1 and values["retention_days"] == 30
    values["retention_days"] = 777
    assert (await settings.get_settings_snapshot())["retention_days"] == 30
    assert (await settings.list_settings_with_meta())["defaults"] == defaults
    assert (await settings.update_settings(None))["db_batch_size"] == 1
    conn = await db()
    try:
        await conn.execute("INSERT INTO app_settings VALUES ('unknown', '12', 0)")
        await conn.commit()
    finally:
        await conn.close()
    assert "unknown" not in await settings.get_settings_snapshot(force_refresh=True)
    assert settings._coerce_setting("unknown", "anything") == "anything"


@pytest.mark.asyncio
async def test_first_status_increment_is_counted(initialized_db):
    await topics.increment_topic_status_count("new", "received", 3)
    assert (await topics.list_topic_status_counts())["new"]["received"] == 3
    await topics.increment_topic_status_count("new", "received", 2)
    assert (await topics.list_topic_status_counts())["new"]["received"] == 5
    with pytest.raises(ValueError):
        await topics.increment_topic_status_count("new", "invalid")
    assert await topics.is_topic_enabled("unknown") is True


@pytest.mark.asyncio
async def test_queue_is_durable_without_memory_duplicate(initialized_db):
    item_id = await enqueue_telegram({"topic": "test", "message": "hello"})
    assert memory_queue.empty()
    assert await telegram_queue.count_telegram_queue() == 1
    assert (await telegram_queue.get_next_telegram_item())["id"] == item_id


@pytest.mark.asyncio
async def test_batch_insert_failure_rolls_back_messages_and_fts(initialized_db):
    with pytest.raises(ValueError):
        await messages.insert_messages("test", [{"id": "1", "message": "first"},
                                                {"id": "2", "priority": "bad"}])
    assert await messages.count_messages_by_topic_since(0) == {}
    conn = await db()
    try:
        assert (await (await conn.execute("SELECT count(*) FROM messages_fts")).fetchone())[0] == 0
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_empty_dead_letters_and_error_listing(initialized_db):
    assert await dead_letter.get_dead_letter(99) is None
    assert await dead_letter.delete_dead_letters([]) == 0
    assert await errors.list_errors() == {"items": [], "total": 0}
    await init_db()  # Migrations are idempotent on a real database.


@pytest.mark.asyncio
async def test_legacy_schema_migration_preserves_data(tmp_db_paths):
    conn = sqlite3.connect(tmp_db_paths["db_path"])
    try:
        conn.executescript('''
            CREATE TABLE topics(name TEXT PRIMARY KEY, enabled INTEGER, count INTEGER,
                                created_at INTEGER, updated_at INTEGER);
            INSERT INTO topics VALUES ('legacy', 1, 12, 0, 0);
            CREATE TABLE telegram_queue(id INTEGER PRIMARY KEY, payload TEXT, created_at INTEGER);
            INSERT INTO telegram_queue VALUES (1, '{"message":"saved"}', 0);
            CREATE TABLE topic_status_counts(topic TEXT PRIMARY KEY, received INTEGER,
                filtered INTEGER, rate_limited INTEGER, updated_at INTEGER);
        ''')
    finally:
        conn.close()
    await init_db()
    row = await topics.get_topic("legacy")
    assert row["count"] == 12 and row["reset_count_base"] == 0
    row = await telegram_queue.get_next_telegram_item()
    assert row["payload"]["message"] == "saved" and row["attempts"] == 0


@pytest.mark.asyncio
async def test_relative_database_path(tmp_db_paths, monkeypatch):
    from db import client, schema
    monkeypatch.chdir(tmp_db_paths["db_path"].parent)
    monkeypatch.setattr(client, "DB_PATH", "relative.db")
    monkeypatch.setattr(schema, "DB_PATH", "relative.db")
    await init_db()
    await topics.add_topic("relative")
    assert (await topics.get_topic("relative"))["name"] == "relative"


@pytest.mark.asyncio
async def test_dlq_move_rolls_back_if_queue_ack_fails(initialized_db):
    item_id = await telegram_queue.enqueue_telegram_item({"message": "hello"})
    conn = await db()
    try:
        await conn.execute("CREATE TRIGGER prevent_delete BEFORE DELETE ON telegram_queue "
                           "BEGIN SELECT RAISE(ABORT, 'simulated disk failure'); END")
        await conn.commit()
    finally:
        await conn.close()
    with pytest.raises(sqlite3.IntegrityError):
        await dead_letter.move_to_dead_letter({"message": "hello"}, 1, "failed",
                                              queue_item_id=item_id)
    assert await telegram_queue.count_telegram_queue() == 1
    assert await dead_letter.count_dead_letters() == 0


@pytest.mark.asyncio
async def test_dead_letter_legacy_payload_and_batch_delete(initialized_db):
    assert await dead_letter.get_dead_letter(999) is None
    for _ in range(2):
        await dead_letter.move_to_dead_letter("legacy", 1, "failure")
    letters = await dead_letter.list_dead_letters()
    assert (await dead_letter.get_dead_letter(letters[0]["id"]))["payload"] == "legacy"
    assert await dead_letter.requeue_dead_letter(letters[0]["id"])
    queued = await telegram_queue.get_next_telegram_item()
    from tasks.delivery_sender import _unpack_queue_item
    assert _unpack_queue_item(queued)[3] == "legacy"
    assert await dead_letter.delete_dead_letters([letters[1]["id"]]) == 1
    assert await dead_letter.count_dead_letters() == 0


@pytest.mark.parametrize("failing_alter", ["topics ADD", "attempts INTEGER",
                                           "next_attempt_at INTEGER", "disabled INTEGER"])
@pytest.mark.asyncio
async def test_migration_does_not_hide_operational_errors(tmp_db_paths, monkeypatch, failing_alter):
    from db import schema
    conn = await db()
    execute = conn.execute

    async def failing_execute(sql, *args):
        if sql.startswith("ALTER") and failing_alter in sql:
            raise sqlite3.OperationalError("simulated storage failure")
        return await execute(sql, *args)

    monkeypatch.setattr(conn, "execute", failing_execute)

    async def connect():
        return conn

    monkeypatch.setattr(schema, "db", connect)
    with pytest.raises(sqlite3.OperationalError, match="storage failure"):
        await schema.init_db()
    with pytest.raises(ValueError, match="no active connection"):
        await execute("SELECT 1")
