import asyncio

import pytest

from tasks import daily_summary


async def _one_loop_sleep():
    called = {"done": False}

    async def _sleep(_):
        if called["done"]:
            raise asyncio.CancelledError()
        called["done"] = True
        return None

    return _sleep


@pytest.mark.asyncio
async def test_build_daily_summary(monkeypatch):
    async def _topics():
        return [
            {"name": "a", "enabled": 1},
            {"name": "b", "enabled": 0},
        ]

    async def _status():
        return {
            "a": {"filtered": 2, "rate_limited": 1},
            "b": {"filtered": 1, "rate_limited": 0},
        }

    async def _counts(_since):
        return {"a": 5, "b": 2}

    async def _q():
        return 3

    async def _dlq():
        return 1

    async def _err(_since):
        return 4

    monkeypatch.setattr(daily_summary, "list_topics", _topics)
    monkeypatch.setattr(daily_summary, "list_topic_status_counts", _status)
    monkeypatch.setattr(daily_summary, "count_messages_by_topic_since", _counts)
    monkeypatch.setattr(daily_summary, "count_telegram_queue", _q)
    monkeypatch.setattr(daily_summary, "count_dead_letters", _dlq)
    monkeypatch.setattr(daily_summary, "count_errors_since", _err)

    msg = await daily_summary._build_daily_summary()
    assert "Daily Summary" in msg
    assert "Total 24h" in msg
    assert "Dead letters" in msg
    assert "\n- " not in msg
    assert "• " in msg
    assert "\\n" not in msg


@pytest.mark.asyncio
async def test_daily_summary_loop_sends_once(monkeypatch):
    calls = []

    async def _enqueue(payload):
        calls.append(payload)
        return 1

    async def settings():
        return {"daily_summary_enabled": True, "daily_summary_hour": 8, "daily_summary_minute": 0}
    monkeypatch.setattr(daily_summary, "get_settings_snapshot", settings)
    async def _build():
        return "x"
    monkeypatch.setattr(daily_summary, "_build_daily_summary", _build)
    monkeypatch.setattr(daily_summary, "enqueue_telegram", _enqueue)
    monkeypatch.setattr(daily_summary, "_summary_tz", lambda: None)
    monkeypatch.setattr(daily_summary, "_should_send_today", lambda _now, last, **kw: last is None)
    monkeypatch.setattr(daily_summary.asyncio, "sleep", await _one_loop_sleep())

    with pytest.raises(asyncio.CancelledError):
        await daily_summary.daily_summary_loop()

    assert len(calls) == 1


@pytest.mark.parametrize("hour,minute,last_sent,expected", [
    (7, 59, None, False),
    (8, 0, None, True),
    (8, 30, None, True),
    (9, 0, None, True),
    (9, 0, "2026-10-07", False),
    (9, 0, "2026-10-06", True),
])
def test_summary_schedule_once_per_day(hour, minute, last_sent, expected):
    from datetime import datetime

    now = datetime(2026, 10, 7, hour, minute)
    assert daily_summary._should_send_today(now, last_sent) is expected
