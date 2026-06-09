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
            reset_count_base,
            created_at,
            updated_at
        )
        VALUES (?, 1, 0, 0, ?, ?)
        ''',
        (
            topic,
            int(time.time()),
            int(time.time()),
        ),
    )
    await conn.execute(
        '''
        INSERT OR IGNORE INTO topic_status_counts(
            topic,
            received,
            filtered,
            rate_limited,
            disabled,
            updated_at
        )
        VALUES (?, 0, 0, 0, 0, ?)
        ''',
        (
            topic,
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
        SELECT name, enabled, count, reset_count_base, created_at, updated_at
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
        SELECT name, enabled, count, reset_count_base, created_at, updated_at
        FROM topics
        WHERE name = ?
        ''',
        (name,),
    )
    row = await cur.fetchone()
    await conn.close()
    return row


async def is_topic_enabled(name):
    row = await get_topic(name)
    if row is None:
        return True
    return bool(row["enabled"])


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


async def reset_topic_count_base(name):
    conn = await db()
    await conn.execute(
        '''
        UPDATE topics
        SET reset_count_base = count,
            updated_at = ?
        WHERE name = ?
        ''',
        (
            int(time.time()),
            name,
        ),
    )
    await conn.commit()
    await conn.close()


async def increment_topic_status_count(name, field, delta=1):
    if field not in {"received", "filtered", "rate_limited", "disabled"}:
        raise ValueError(f"invalid status field: {field}")

    conn = await db()
    await conn.execute(
        f'''
        INSERT INTO topic_status_counts(
            topic,
            received,
            filtered,
            rate_limited,
            disabled,
            updated_at
        )
        VALUES (?, 0, 0, 0, 0, ?)
        ON CONFLICT(topic) DO UPDATE SET
            {field} = {field} + ?,
            updated_at = ?
        ''',
        (
            name,
            int(time.time()),
            int(delta),
            int(time.time()),
        ),
    )
    await conn.commit()
    await conn.close()


async def list_topic_status_counts():
    conn = await db()
    cur = await conn.execute(
        '''
        SELECT topic, received, filtered, rate_limited, disabled
        FROM topic_status_counts
        '''
    )
    rows = await cur.fetchall()
    await conn.close()
    return {
        row["topic"]: {
            "received": int(row["received"] or 0),
            "filtered": int(row["filtered"] or 0),
            "rate_limited": int(row["rate_limited"] or 0),
            "disabled": int(row["disabled"] or 0),
        }
        for row in rows
    }
