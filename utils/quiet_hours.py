from datetime import datetime

from core.config import (
    QUIET_HOURS_START,
    QUIET_HOURS_END,
)

def in_quiet_hours():

    now = datetime.now().hour

    if QUIET_HOURS_START > QUIET_HOURS_END:
        return now >= QUIET_HOURS_START or now < QUIET_HOURS_END

    return QUIET_HOURS_START <= now < QUIET_HOURS_END
