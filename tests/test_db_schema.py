import sqlite3
import pytest

from db.schema import init_db


@pytest.mark.asyncio
async def test_init_db_creates_tables(tmp_db_paths):
    await init_db()

    conn = sqlite3.connect(str(tmp_db_paths["db_path"]))
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )
    names = {row[0] for row in cur.fetchall()}
    conn.close()

    assert "topics" in names
    assert "messages" in names
    assert "errors" in names
    assert "telegram_queue" in names
