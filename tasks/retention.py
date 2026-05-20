import asyncio
import time
import sqlite3

from db.client import db

from core.config import (
    RETENTION_DAYS,
    ERROR_RETENTION_DAYS,
    DB_PATH,
)

async def retention_loop():

    while True:

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

        def _vacuum():
            conn2 = sqlite3.connect(DB_PATH)
            conn2.execute("VACUUM")
            conn2.execute("PRAGMA optimize")
            conn2.close()

        await asyncio.to_thread(_vacuum)

        await asyncio.sleep(86400)
