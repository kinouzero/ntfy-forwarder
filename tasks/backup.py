import asyncio
import gzip
import shutil
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from core.config import DB_PATH, BACKUP_DIR
from core.state import shutdown_event
from core.logging import log


def create_backup():
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    backup_path = Path(BACKUP_DIR) / f"ntfy-{ts}.db"
    # The backup API includes committed WAL pages and gives a consistent snapshot.
    source_uri = Path(DB_PATH).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(source_uri, uri=True)) as source:
        with closing(sqlite3.connect(backup_path)) as destination:
            source.backup(destination)
    with backup_path.open("rb") as source:
        with gzip.open(str(backup_path) + ".gz", "wb") as destination:
            shutil.copyfileobj(source, destination)
    return backup_path


async def backup_loop():
    while not shutdown_event.is_set():
        try:
            await asyncio.to_thread(create_backup)
        except Exception as exc:
            log("ERROR", "database backup failed", error=str(exc))
        await asyncio.sleep(86400)
