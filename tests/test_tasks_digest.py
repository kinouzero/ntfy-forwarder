import asyncio

import pytest

from tasks import digest
from tests.conftest import drain_queue


async def _one_loop_sleep():
    called = {"done": False}

    async def _sleep(_):
        if called["done"]:
            raise asyncio.CancelledError()
        called["done"] = True
        return None

    return _sleep


@pytest.mark.asyncio
async def test_digest_sends_when_threshold_exceeded(monkeypatch):
    digest.digest_buffer.clear()
    drain_queue(digest.telegram_queue)

    digest.digest_buffer["t1"] = 11
    digest.digest_buffer["t2"] = 5

    monkeypatch.setattr(digest.asyncio, "sleep", await _one_loop_sleep())

    with pytest.raises(asyncio.CancelledError):
        await digest.digest_loop()

    assert digest.telegram_queue.qsize() == 1
    drain_queue(digest.telegram_queue)
