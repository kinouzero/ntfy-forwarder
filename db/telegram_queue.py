import json
import time

from db.client import db


async def enqueue_telegram_item(payload):
    conn = await db()
    cur = await conn.execute(
        '''
        INSERT INTO telegram_queue(
            payload,
            created_at,
            attempts,
            next_attempt_at
        )
        VALUES (?, ?, 0, 0)
        ''',
        (
            json.dumps(payload, ensure_ascii=True),
            int(time.time()),
        ),
    )
    await conn.commit()
    row_id = getattr(cur, "lastrowid", None)
    if row_id is None:
        row_id = cur._cursor.lastrowid  # test shim fallback
    row_id = int(row_id)
    await conn.close()
    return row_id


async def get_next_telegram_item():
    conn = await db()
    cur = await conn.execute(
        '''
        SELECT id, payload, attempts
        FROM telegram_queue
        WHERE next_attempt_at <= ?
        ORDER BY id ASC
        LIMIT 1
        ''',
        (int(time.time()),),
    )
    row = await cur.fetchone()
    await conn.close()
    if row is None:
        return None
    return {
        "id": int(row["id"]),
        "payload": json.loads(row["payload"]),
        "attempts": int(row["attempts"] or 0),
    }


async def ack_telegram_item(item_id):
    conn = await db()
    await conn.execute(
        '''
        DELETE FROM telegram_queue
        WHERE id = ?
        ''',
        (int(item_id),),
    )
    await conn.commit()
    await conn.close()


async def retry_telegram_item(item_id, delay_seconds):
    conn = await db()
    await conn.execute(
        '''
        UPDATE telegram_queue
        SET attempts = attempts + 1,
            next_attempt_at = ?
        WHERE id = ?
        ''',
        (
            int(time.time()) + int(delay_seconds),
            int(item_id),
        ),
    )
    await conn.commit()
    await conn.close()


async def count_telegram_queue():
    conn = await db()
    cur = await conn.execute(
        '''
        SELECT COUNT(*) AS c
        FROM telegram_queue
        '''
    )
    row = await cur.fetchone()
    await conn.close()
    return int(row["c"] or 0)
