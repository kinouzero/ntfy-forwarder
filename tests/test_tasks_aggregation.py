import asyncio

import pytest

from tasks import aggregation as agg
from models.event import NtfyEvent
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
async def test_aggregation_below_min_sends_individual(monkeypatch):
    agg.aggregation_buffer.clear()
    drain_queue(agg.telegram_queue)

    monkeypatch.setattr(agg, "AGGREGATION_MIN_COUNT", 3)
    monkeypatch.setattr(agg, "AGGREGATION_INTERVAL", 0)
    monkeypatch.setattr(agg.asyncio, "sleep", await _one_loop_sleep())

    e1 = NtfyEvent.from_json("t1", {"message": "m1"})
    e2 = NtfyEvent.from_json("t1", {"message": "m2"})
    agg.aggregation_buffer["t1"] = [e1, e2]

    with pytest.raises(asyncio.CancelledError):
        await agg.aggregation_loop()

    assert agg.telegram_queue.qsize() == 2
    drain_queue(agg.telegram_queue)


@pytest.mark.asyncio
async def test_aggregation_above_min_sends_summary(monkeypatch):
    agg.aggregation_buffer.clear()
    drain_queue(agg.telegram_queue)

    monkeypatch.setattr(agg, "AGGREGATION_MIN_COUNT", 2)
    monkeypatch.setattr(agg, "AGGREGATION_INTERVAL", 0)
    monkeypatch.setattr(agg.asyncio, "sleep", await _one_loop_sleep())

    e1 = NtfyEvent.from_json("t1", {"message": "m1"})
    e2 = NtfyEvent.from_json("t1", {"message": "m2"})
    agg.aggregation_buffer["t1"] = [e1, e2]

    with pytest.raises(asyncio.CancelledError):
        await agg.aggregation_loop()

    assert agg.telegram_queue.qsize() == 1
    drain_queue(agg.telegram_queue)
