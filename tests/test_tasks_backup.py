import asyncio
from pathlib import Path

import pytest

from tasks import backup
import core.config as config


async def _one_loop_sleep():
    called = {"done": False}

    async def _sleep(_):
        if called["done"]:
            raise asyncio.CancelledError()
        called["done"] = True
        return None

    return _sleep


@pytest.mark.asyncio
async def test_backup_creates_gz(tmp_path, monkeypatch):
    db_path = tmp_path / "ntfy.db"
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()
    db_path.write_text("data")

    monkeypatch.setattr(config, "DB_PATH", str(db_path))
    monkeypatch.setattr(config, "BACKUP_DIR", str(backup_dir))
    monkeypatch.setattr(backup, "DB_PATH", str(db_path))
    monkeypatch.setattr(backup, "BACKUP_DIR", str(backup_dir))
    async def _to_thread(fn):
        fn()
        return None
    monkeypatch.setattr(backup.asyncio, "to_thread", _to_thread)
    monkeypatch.setattr(backup.asyncio, "sleep", await _one_loop_sleep())

    with pytest.raises(asyncio.CancelledError):
        await backup.backup_loop()

    files = list(Path(backup_dir).glob("ntfy-*.db"))
    gz_files = list(Path(backup_dir).glob("ntfy-*.db.gz"))

    assert len(files) == 1
    assert len(gz_files) == 1
