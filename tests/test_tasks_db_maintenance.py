import sqlite3

import pytest

from db.schema import init_db
from tasks import db_maintenance


@pytest.mark.asyncio
async def test_db_maintenance_runs(tmp_db_paths, monkeypatch):
    await init_db()
    monkeypatch.setattr(db_maintenance, "DB_PATH", str(tmp_db_paths["db_path"]))
    db_maintenance._run_db_maintenance()

    conn = sqlite3.connect(str(tmp_db_paths["db_path"]))
    cur = conn.execute("PRAGMA integrity_check")
    row = cur.fetchone()
    conn.close()
    assert row[0] == "ok"
