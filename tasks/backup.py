import asyncio
import gzip
import shutil
from datetime import datetime, timezone

from core.config import (
    DB_PATH,
    BACKUP_DIR,
)

async def backup_loop():

    while True:

        ts = datetime.now(timezone.utc).strftime(
            "%Y%m%d-%H%M%S"
        )

        backup_path = (
            f"{BACKUP_DIR}/"
            f"ntfy-{ts}.db"
        )

        def _do_backup():
            shutil.copy2(
                DB_PATH,
                backup_path,
            )

            with open(
                backup_path,
                "rb",
            ) as f_in:

                with gzip.open(
                    backup_path + ".gz",
                    "wb",
                ) as f_out:

                    shutil.copyfileobj(
                        f_in,
                        f_out,
                    )

        await asyncio.to_thread(_do_backup)

        await asyncio.sleep(86400)
