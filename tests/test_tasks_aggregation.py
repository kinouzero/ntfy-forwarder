import asyncio

import pytest

from tasks import aggregation as agg
from models.event import NtfyEvent


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
    sent = []

    async def enabled(_topic):
        return True

    monkeypatch.setattr(agg, "is_topic_enabled", enabled)
    async def enqueue(payload):
        sent.append(payload)
    monkeypatch.setattr(agg, "enqueue_telegram", enqueue)
    async def settings():
        return {"aggregation_min_count": 3, "aggregation_interval": 0}
    monkeypatch.setattr(agg, "get_settings_snapshot", settings)
    monkeypatch.setattr(agg.asyncio, "sleep", await _one_loop_sleep())

    e1 = NtfyEvent.from_json("t1", {"message": "m1"})
    e2 = NtfyEvent.from_json("t1", {"message": "m2"})
    agg.aggregation_buffer["t1"] = [e1, e2]

    with pytest.raises(asyncio.CancelledError):
        await agg.aggregation_loop()

    assert len(sent) == 2


@pytest.mark.asyncio
async def test_aggregation_above_min_sends_summary(monkeypatch):
    agg.aggregation_buffer.clear()
    sent = []

    async def enabled(_topic):
        return True

    monkeypatch.setattr(agg, "is_topic_enabled", enabled)
    async def enqueue(payload):
        sent.append(payload)
    monkeypatch.setattr(agg, "enqueue_telegram", enqueue)
    async def settings():
        return {"aggregation_min_count": 2, "aggregation_interval": 0}
    monkeypatch.setattr(agg, "get_settings_snapshot", settings)
    monkeypatch.setattr(agg.asyncio, "sleep", await _one_loop_sleep())

    e1 = NtfyEvent.from_json("t1", {"message": "m1"})
    e2 = NtfyEvent.from_json("t1", {"message": "m2"})
    agg.aggregation_buffer["t1"] = [e1, e2]

    with pytest.raises(asyncio.CancelledError):
        await agg.aggregation_loop()

    assert len(sent) == 1


@pytest.mark.asyncio
async def test_aggregation_drops_disabled_topic(monkeypatch):
    agg.aggregation_buffer.clear()
    sent = []

    async def disabled(_topic):
        return False

    monkeypatch.setattr(agg, "is_topic_enabled", disabled)
    async def enqueue(payload):
        sent.append(payload)
    monkeypatch.setattr(agg, "enqueue_telegram", enqueue)
    async def settings():
        return {"aggregation_min_count": 10, "aggregation_interval": 0}
    monkeypatch.setattr(agg, "get_settings_snapshot", settings)
    monkeypatch.setattr(agg.asyncio, "sleep", await _one_loop_sleep())

    e1 = NtfyEvent.from_json("t1", {"message": "m1"})
    agg.aggregation_buffer["t1"] = [e1]

    with pytest.raises(asyncio.CancelledError):
        await agg.aggregation_loop()

    assert len(sent) == 0
    assert agg.aggregation_buffer["t1"] == []
