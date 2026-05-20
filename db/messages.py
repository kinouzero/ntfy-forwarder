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
