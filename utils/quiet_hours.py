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

    now = _now_hour()

    # Equal boundaries mean the window is disabled.
    if QUIET_HOURS_START == QUIET_HOURS_END:
        return False

    if QUIET_HOURS_START > QUIET_HOURS_END:
        return now >= QUIET_HOURS_START or now < QUIET_HOURS_END

    return QUIET_HOURS_START <= now < QUIET_HOURS_END
