from datetime import datetime
from zoneinfo import ZoneInfo

from core.config import (
    QUIET_HOURS_START,
    QUIET_HOURS_END,
    TZ,
)


def _quiet_tz():
    try:
        return ZoneInfo(TZ)
    except Exception:
        return ZoneInfo("UTC")


def _now_hour():
    return datetime.now(_quiet_tz()).hour


def in_quiet_hours():
    return in_quiet_hours_window(QUIET_HOURS_START, QUIET_HOURS_END)


def in_quiet_hours_window(start_hour, end_hour):
    now = _now_hour()

    # Equal boundaries mean the window is disabled.
    if start_hour == end_hour:
        return False

    if start_hour > end_hour:
        return now >= start_hour or now < end_hour

    return start_hour <= now < end_hour
