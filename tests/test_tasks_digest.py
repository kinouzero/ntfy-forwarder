import asyncio

import pytest

from tasks import digest


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
    sent = []

    digest.digest_buffer["t1"] = 11
    digest.digest_buffer["t2"] = 5

    async def enabled(_topic):
        return True

    monkeypatch.setattr(digest, "is_topic_enabled", enabled)
    async def enqueue(payload):
        sent.append(payload)
    monkeypatch.setattr(digest, "enqueue_telegram", enqueue)
    monkeypatch.setattr(digest.asyncio, "sleep", await _one_loop_sleep())

    with pytest.raises(asyncio.CancelledError):
        await digest.digest_loop()

    assert len(sent) == 1


@pytest.mark.asyncio
async def test_digest_drops_disabled_topic(monkeypatch):
    digest.digest_buffer.clear()
    sent = []

    digest.digest_buffer["t1"] = 11

    async def disabled(_topic):
        return False

    monkeypatch.setattr(digest, "is_topic_enabled", disabled)
    async def enqueue(payload):
        sent.append(payload)
    monkeypatch.setattr(digest, "enqueue_telegram", enqueue)
    monkeypatch.setattr(digest.asyncio, "sleep", await _one_loop_sleep())

    with pytest.raises(asyncio.CancelledError):
        await digest.digest_loop()

    assert len(sent) == 0
    assert digest.digest_buffer == {}
