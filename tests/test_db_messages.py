import pytest

from db import client as db_client
from db.schema import init_db
from db.messages import insert_message
from db.errors import log_error


@pytest.mark.asyncio
async def test_insert_message_dedup(tmp_db_paths):
    await init_db()

    evt = {"id": "1", "message": "hello", "priority": 3}

    inserted = await insert_message("topic-1", evt)
    assert inserted is True

    inserted_again = await insert_message("topic-1", evt)
    assert inserted_again is False


@pytest.mark.asyncio
async def test_log_error_writes(tmp_db_paths):
    await init_db()

    await log_error("component", "topic-1", "boom")

    conn = await db_client.db()
    cur = await conn.execute(
        "SELECT COUNT(*) AS c FROM errors"
    )
    row = await cur.fetchone()
    await conn.close()

    assert row["c"] == 1
