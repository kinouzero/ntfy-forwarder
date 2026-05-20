import asyncio
import time

import pytest

from tasks import monitor


async def _one_loop_sleep():
    called = {"done": False}

    async def _sleep(_):
        if called["done"]:
            raise asyncio.CancelledError()
        called["done"] = True
        return None

    return _sleep


@pytest.mark.asyncio
async def test_monitor_restarts_stale_worker(monkeypatch):
    monitor.workers.clear()
    monitor.worker_last_seen.clear()

    async def fake_worker(_topic):
        await asyncio.sleep(0)

    old_task = asyncio.create_task(fake_worker("t1"))
    monitor.workers["t1"] = old_task
    monitor.worker_last_seen["t1"] = int(time.time()) - 999

    monkeypatch.setattr(monitor, "ntfy_worker", fake_worker)
    monkeypatch.setattr(monitor.asyncio, "sleep", await _one_loop_sleep())

    with pytest.raises(asyncio.CancelledError):
        await monitor.worker_monitor_loop()

    assert monitor.workers["t1"] is not old_task
