import pytest

from db.schema import init_db
from db.telegram_queue import (
    enqueue_telegram_item,
    get_next_telegram_item,
    ack_telegram_item,
    retry_telegram_item,
    count_telegram_queue,
)


@pytest.mark.asyncio
async def test_telegram_queue_enqueue_retry_ack(tmp_db_paths):
    await init_db()

    item_id = await enqueue_telegram_item(
        {"topic": "t1", "message": "hello"}
    )
    assert item_id > 0
    assert await count_telegram_queue() == 1

    row = await get_next_telegram_item()
    assert row is not None
    assert row["id"] == item_id
    assert row["payload"]["topic"] == "t1"

    await retry_telegram_item(item_id, 60)
    row = await get_next_telegram_item()
    assert row is None

    await ack_telegram_item(item_id)
    assert await count_telegram_queue() == 0
