import sys
from pathlib import Path
import pytest
import sqlite3

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import core.config as config  # noqa: E402
from db import client as db_client  # noqa: E402
import db.schema as db_schema  # noqa: E402
import db.messages as db_messages  # noqa: E402
import db.errors as db_errors  # noqa: E402
import db.topics as db_topics  # noqa: E402
import db.telegram_queue as db_tg_queue  # noqa: E402
import db.dead_letter as db_dead_letter  # noqa: E402
import tasks.retention as tasks_retention  # noqa: E402


def drain_queue(q):
    try:
        while True:
            q.get_nowait()
    except Exception:
        return

class _AsyncCursor:
    def __init__(self, cursor):
        self._cursor = cursor

    async def fetchone(self):
        return self._cursor.fetchone()

    async def fetchall(self):
        return self._cursor.fetchall()


class _AsyncConn:
    def __init__(self, path):
        self._conn = sqlite3.connect(path, isolation_level=None)
        self._conn.row_factory = sqlite3.Row

    async def execute(self, *args, **kwargs):
        cur = self._conn.execute(*args, **kwargs)
        return _AsyncCursor(cur)

    async def commit(self):
        self._conn.commit()

    async def close(self):
        self._conn.close()


async def _async_db(path):
    return _AsyncConn(path)


@pytest.fixture
def tmp_db_paths(tmp_path, monkeypatch):
    db_path = tmp_path / "ntfy.db"
    export_dir = tmp_path / "exports"
    backup_dir = tmp_path / "backups"

    monkeypatch.setattr(config, "DB_PATH", str(db_path))
    monkeypatch.setattr(config, "EXPORT_DIR", str(export_dir))
    monkeypatch.setattr(config, "BACKUP_DIR", str(backup_dir))

    monkeypatch.setattr(db_client, "DB_PATH", str(db_path))
    monkeypatch.setattr(db_schema, "DB_PATH", str(db_path))
    monkeypatch.setattr(db_schema, "EXPORT_DIR", str(export_dir))
    monkeypatch.setattr(db_schema, "BACKUP_DIR", str(backup_dir))

    async def _db():
        return await _async_db(str(db_path))

    monkeypatch.setattr(db_client, "db", _db)
    monkeypatch.setattr(db_schema, "db", _db)
    monkeypatch.setattr(db_messages, "db", _db)
    monkeypatch.setattr(db_errors, "db", _db)
    monkeypatch.setattr(db_topics, "db", _db)
    monkeypatch.setattr(db_tg_queue, "db", _db)
    monkeypatch.setattr(db_dead_letter, "db", _db)
    monkeypatch.setattr(tasks_retention, "db", _db)

    return {
        "db_path": db_path,
        "export_dir": export_dir,
        "backup_dir": backup_dir,
    }
