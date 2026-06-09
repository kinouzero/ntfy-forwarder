import json
import time

from db.client import db


async def move_to_dead_letter(payload, attempts, last_error, topic=None):
    conn = await db()
    await conn.execute(
        '''
        INSERT INTO telegram_dead_letter(
            payload,
            topic,
            last_error,
            attempts,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (
            json.dumps(payload, ensure_ascii=True),
            topic,
            str(last_error),
            int(attempts),
            int(time.time()),
            int(time.time()),
        ),
    )
    await conn.commit()
    await conn.close()


async def list_dead_letters(limit=200):
    result = await query_dead_letters(limit=limit, offset=0)
    return result["items"]


def _build_dead_letter_filters(topic=None, reason=None, query=None):
    clauses = []
    params = []
    if query:
        clauses.append(
            "(topic LIKE ? OR last_error LIKE ? OR payload LIKE ?)"
        )
        like = f"%{query}%"
        params.extend([like, like, like])
    if topic:
        clauses.append("topic = ?")
        params.append(topic)
    if reason:
        clauses.append("last_error = ?")
        params.append(reason)
    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    return where, params


async def query_dead_letters(
    limit=200,
    offset=0,
    topic=None,
    reason=None,
    query=None,
):
    conn = await db()
    where, params = _build_dead_letter_filters(
        topic=topic,
        reason=reason,
        query=query,
    )
    cur = await conn.execute(
        f'''
        SELECT id, payload, topic, last_error, attempts, created_at, updated_at
        FROM telegram_dead_letter
        {where}
        ORDER BY id DESC
        LIMIT ?
        OFFSET ?
        ''',
        (
            *params,
            int(limit),
            int(offset),
        ),
    )
    rows = await cur.fetchall()
    cur = await conn.execute(
        f'''
        SELECT COUNT(*) AS c
        FROM telegram_dead_letter
        {where}
        ''',
        tuple(params),
    )
    total = int((await cur.fetchone())["c"] or 0)
    await conn.close()
    return {
        "total": total,
        "items": [
            {
                "id": int(row["id"]),
                "payload": json.loads(row["payload"]),
                "topic": row["topic"],
                "last_error": row["last_error"],
                "attempts": int(row["attempts"] or 0),
                "created_at": int(row["created_at"] or 0),
                "updated_at": int(row["updated_at"] or 0),
            }
            for row in rows
        ],
    }


async def get_dead_letter(item_id):
    conn = await db()
    cur = await conn.execute(
        '''
        SELECT id, payload, topic, last_error, attempts
        FROM telegram_dead_letter
        WHERE id = ?
        ''',
        (int(item_id),),
    )
    row = await cur.fetchone()
    await conn.close()
    if row is None:
        return None
    return {
        "id": int(row["id"]),
        "payload": json.loads(row["payload"]),
        "topic": row["topic"],
        "last_error": row["last_error"],
        "attempts": int(row["attempts"] or 0),
    }


async def delete_dead_letter(item_id):
    conn = await db()
    await conn.execute(
        '''
        DELETE FROM telegram_dead_letter
        WHERE id = ?
        ''',
        (int(item_id),),
    )
    await conn.commit()
    await conn.close()


async def delete_dead_letters(ids):
    ids = [int(i) for i in ids]
    if not ids:
        return 0
    conn = await db()
    placeholders = ",".join("?" for _ in ids)
    await conn.execute(
        f'''
        DELETE FROM telegram_dead_letter
        WHERE id IN ({placeholders})
        ''',
        tuple(ids),
    )
    await conn.commit()
    await conn.close()
    return len(ids)


async def clear_dead_letters(topic=None, reason=None, query=None):
    conn = await db()
    where, params = _build_dead_letter_filters(
        topic=topic,
        reason=reason,
        query=query,
    )
    cur = await conn.execute(
        f'''
        SELECT COUNT(*) AS c
        FROM telegram_dead_letter
        {where}
        ''',
        tuple(params),
    )
    row = await cur.fetchone()
    count = int(row["c"] or 0)
    await conn.execute(
        f'''
        DELETE FROM telegram_dead_letter
        {where}
        ''',
        tuple(params),
    )
    await conn.commit()
    await conn.close()
    return count


async def count_dead_letters():
    conn = await db()
    cur = await conn.execute(
        '''
        SELECT COUNT(*) AS c
        FROM telegram_dead_letter
        '''
    )
    row = await cur.fetchone()
    await conn.close()
    return int(row["c"] or 0)
