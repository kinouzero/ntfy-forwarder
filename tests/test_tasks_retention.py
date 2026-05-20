import asyncio
import time

import pytest
import sqlite3

from db.schema import init_db
from tasks import retention


async def _one_loop_sleep():
    called = {"done": False}

    async def _sleep(_):
        if called["done"]:
            raise asyncio.CancelledError()
        called["done"] = True
        return None

    return _sleep


@pytest.mark.asyncio
async def test_retention_deletes_old_rows(tmp_db_paths, monkeypatch):
    await init_db()

    now = int(time.time())
    old_ts = now - (3 * 86400)
    new_ts = now - 100

    conn = sqlite3.connect(str(tmp_db_paths["db_path"]))
    conn.execute(
        "INSERT INTO messages(topic, ntfy_id, message, priority, created_at) "
        "VALUES(?,?,?,?,?)",
        ("t1", "1", "old", 3, old_ts),
    )
    conn.execute(
        "INSERT INTO messages(topic, ntfy_id, message, priority, created_at) "
        "VALUES(?,?,?,?,?)",
        ("t1", "2", "new", 3, new_ts),
    )
    conn.execute(
        "INSERT INTO errors(ts, component, topic, error) VALUES(?,?,?,?)",
        (old_ts, "c", "t1", "old"),
    )
    conn.execute(
        "INSERT INTO errors(ts, component, topic, error) VALUES(?,?,?,?)",
        (new_ts, "c", "t1", "new"),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(retention, "RETENTION_DAYS", 1)
    monkeypatch.setattr(retention, "ERROR_RETENTION_DAYS", 1)
    monkeypatch.setattr(retention.time, "time", lambda: now)
    async def _to_thread(fn):
        fn()
        return None
    monkeypatch.setattr(retention.asyncio, "to_thread", _to_thread)
    monkeypatch.setattr(retention.asyncio, "sleep", await _one_loop_sleep())

    with pytest.raises(asyncio.CancelledError):
        await retention.retention_loop()

    conn = sqlite3.connect(str(tmp_db_paths["db_path"]))
    cur = conn.execute("SELECT COUNT(*) FROM messages")
    msg_count = cur.fetchone()[0]
    cur = conn.execute("SELECT COUNT(*) FROM errors")
    err_count = cur.fetchone()[0]
    conn.close()

    assert msg_count == 1
    assert err_count == 1
