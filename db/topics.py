import time

from db.client import db

async def add_topic(topic):

    conn = await db()

    await conn.execute(
        '''
        INSERT OR IGNORE INTO topics(
            name,
            enabled,
            count,
            created_at,
            updated_at
        )
        VALUES (?, 1, 0, ?, ?)
        ''',
        (
            topic,
            int(time.time()),
            int(time.time()),
        ),
    )

    await conn.commit()
    await conn.close()


async def increment_topic_count(topic):

    conn = await db()

    await conn.execute(
        '''
        UPDATE topics
        SET count = count + 1,
            updated_at = ?
        WHERE name = ?
        ''',
        (
            int(time.time()),
            topic,
        ),
    )

    await conn.commit()
    await conn.close()


async def list_topics():
    conn = await db()
    cur = await conn.execute(
        '''
        SELECT name, enabled, count, created_at, updated_at
        FROM topics
        ORDER BY name
        '''
    )
    rows = await cur.fetchall()
    await conn.close()
    return rows


async def get_topic(name):
    conn = await db()
    cur = await conn.execute(
        '''
        SELECT name, enabled, count, created_at, updated_at
        FROM topics
        WHERE name = ?
        ''',
        (name,),
    )
    row = await cur.fetchone()
    await conn.close()
    return row


async def set_topic_enabled(name, enabled):
    conn = await db()
    await conn.execute(
        '''
        UPDATE topics
        SET enabled = ?,
            updated_at = ?
        WHERE name = ?
        ''',
        (
            1 if enabled else 0,
            int(time.time()),
            name,
        ),
    )
    await conn.commit()
    await conn.close()
