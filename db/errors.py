import time

from db.client import db
from core.logging import log

async def log_error(component, topic, err):

    log(
        "ERROR",
        str(err),
        component=component,
        topic=topic,
    )

    conn = await db()

    await conn.execute(
        '''
        INSERT INTO errors(
            ts,
            component,
            topic,
            error
        )
        VALUES (?, ?, ?, ?)
        ''',
        (
            int(time.time()),
            component,
            topic,
            str(err),
        ),
    )

    await conn.commit()
    await conn.close()


async def list_errors(limit=100):
    return await query_errors(limit=limit, offset=0)


def _build_error_filters(query=None, component=None, topic=None):
    clauses = []
    params = []
    if query:
        clauses.append("(error LIKE ? OR component LIKE ? OR topic LIKE ?)")
        like = f"%{query}%"
        params.extend([like, like, like])
    if component:
        clauses.append("component = ?")
        params.append(component)
    if topic:
        clauses.append("topic = ?")
        params.append(topic)
    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)
    return where, params


async def query_errors(limit=100, offset=0, query=None, component=None, topic=None):
    conn = await db()
    where, params = _build_error_filters(
        query=query,
        component=component,
        topic=topic,
    )
    cur = await conn.execute(
        f'''
        SELECT id, ts, component, topic, error
        FROM errors
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
        FROM errors
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
                "ts": int(row["ts"] or 0),
                "component": row["component"],
                "topic": row["topic"],
                "error": row["error"],
            }
            for row in rows
        ],
    }


async def count_errors_since(ts):
    conn = await db()
    cur = await conn.execute(
        '''
        SELECT COUNT(*) AS c
        FROM errors
        WHERE ts >= ?
        ''',
        (int(ts),),
    )
    row = await cur.fetchone()
    await conn.close()
    return int(row["c"] or 0)


async def clear_errors():
    conn = await db()
    cur = await conn.execute(
        '''
        SELECT COUNT(*) AS c
        FROM errors
        '''
    )
    row = await cur.fetchone()
    count = int(row["c"] or 0)
    await conn.execute(
        '''
        DELETE FROM errors
        '''
    )
    await conn.commit()
    await conn.close()
    return count
