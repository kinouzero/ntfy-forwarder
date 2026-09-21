import time
import sqlite3

from db.client import db

async def insert_message(topic, evt):

    inserted = await insert_messages(topic, [evt])
    return inserted[0]


async def insert_messages(topic, events):
    conn = await db()
    inserted = []

    try:
        await conn.execute("BEGIN")

        for evt in events:
            try:
                await conn.execute(
                    '''
                    INSERT INTO messages(
                        topic,
                        ntfy_id,
                        message,
                        priority,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (
                        topic,
                        evt.get("id"),
                        evt.get("message"),
                        int(evt.get("priority", 3)),
                        int(time.time()),
                    ),
                )

                await conn.execute(
                    '''
                    INSERT INTO messages_fts(
                        topic,
                        message
                    )
                    VALUES (?, ?)
                    ''',
                    (
                        topic,
                        evt.get("message", ""),
                    ),
                )
                inserted.append(True)
            except sqlite3.IntegrityError:
                inserted.append(False)

        await conn.commit()
        return inserted

    finally:
        await conn.close()


async def count_messages_by_topic_since(since_ts):
    conn = await db()
    cur = await conn.execute(
        '''
        SELECT topic, COUNT(*) AS c
        FROM messages
        WHERE created_at >= ?
        GROUP BY topic
        ''',
        (int(since_ts),),
    )
    rows = await cur.fetchall()
    await conn.close()
    return {
        row["topic"]: int(row["c"])
        for row in rows
    }


async def clear_all_messages():
    conn = await db()
    await conn.execute("DELETE FROM messages")
    await conn.execute("DELETE FROM messages_fts")
    await conn.commit()
    await conn.close()
