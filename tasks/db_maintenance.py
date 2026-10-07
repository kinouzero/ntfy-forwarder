import asyncio
import sqlite3
import time

from core.config import (
    DB_PATH,
)
from core.state import shutdown_event
from core.logging import log
from db.settings import get_settings_snapshot
from core.metrics import (
    db_maintenance_seconds,
    db_maintenance_runs_total,
)


def _run_db_maintenance():
    conn = sqlite3.connect(DB_PATH)
    try:
        start = time.monotonic()
        conn.execute("PRAGMA optimize")
        db_maintenance_seconds.labels(operation="optimize").observe(
            time.monotonic() - start
        )
        db_maintenance_runs_total.labels(operation="optimize").inc()

        start = time.monotonic()
        conn.execute("ANALYZE")
        db_maintenance_seconds.labels(operation="analyze").observe(
            time.monotonic() - start
        )
        db_maintenance_runs_total.labels(operation="analyze").inc()
        conn.commit()
    finally:
        conn.close()


async def db_maintenance_loop():
    while not shutdown_event.is_set():
        try:
            await asyncio.to_thread(_run_db_maintenance)
            log("DEBUG", "db maintenance completed")
        except Exception as exc:
            log("WARN", "db maintenance failed", error=str(exc))
        settings = await get_settings_snapshot()
        await asyncio.sleep(int(settings["db_maintenance_interval_seconds"]))
