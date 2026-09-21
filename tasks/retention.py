import asyncio
import time

from db.client import db
from db.settings import get_settings_snapshot
from core.state import shutdown_event

async def retention_loop():

    while not shutdown_event.is_set():
        settings = await get_settings_snapshot()
        retention_days = int(settings["retention_days"])
        error_retention_days = int(settings["error_retention_days"])

        conn = await db()

        msg_limit = (
            int(time.time())
            - (retention_days * 86400)
        )

        err_limit = (
            int(time.time())
            - (error_retention_days * 86400)
        )

        await conn.execute(
            '''
            DELETE FROM messages
            WHERE created_at < ?
            ''',
            (msg_limit,),
        )

        await conn.execute(
            '''
            DELETE FROM errors
            WHERE ts < ?
            ''',
            (err_limit,),
        )

        await conn.commit()
        await conn.close()

        await asyncio.sleep(86400)
