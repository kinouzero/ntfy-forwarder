import asyncio
import time

from db.client import db

from core.config import (
    RETENTION_DAYS,
    ERROR_RETENTION_DAYS,
)
from core.state import shutdown_event

async def retention_loop():

    while not shutdown_event.is_set():

        conn = await db()

        msg_limit = (
            int(time.time())
            - (RETENTION_DAYS * 86400)
        )

        err_limit = (
            int(time.time())
            - (ERROR_RETENTION_DAYS * 86400)
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
