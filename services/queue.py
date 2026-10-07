from db.telegram_queue import enqueue_telegram_item


async def enqueue_telegram(payload):
    # SQLite is the source of truth. Mirroring into memory races with the sender
    # polling the database and can deliver the same item twice.
    return await enqueue_telegram_item(payload)
