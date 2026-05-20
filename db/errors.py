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
