from core.state import telegram_queue
from db.telegram_queue import enqueue_telegram_item


async def enqueue_telegram(payload):
    item_id = await enqueue_telegram_item(payload)
    await telegram_queue.put(
        {
            "id": item_id,
            "payload": payload,
        }
    )
    return item_id
