import json
from datetime import datetime, timezone

from core.config import LOG_LEVEL

LEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
LEVEL_RANK = {lvl: i for i, lvl in enumerate(LEVELS)}

def log(level, msg, **kwargs):
    if LEVEL_RANK.get(level, 99) < LEVEL_RANK.get(LOG_LEVEL, 1):
        return
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "msg": msg,
    }
    payload.update(kwargs)
    print(json.dumps(payload), flush=True)
