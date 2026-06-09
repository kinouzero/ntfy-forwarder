import re

from core.config import (
    FILTER_INCLUDE_REGEX,
    FILTER_EXCLUDE_REGEX,
    FILTER_MIN_PRIORITY,
)

_include = [
    re.compile(pattern, re.I)
    for pattern in FILTER_INCLUDE_REGEX
]
_exclude = [
    re.compile(pattern, re.I)
    for pattern in FILTER_EXCLUDE_REGEX
]


def passes_filters(event_or_message):
    if isinstance(event_or_message, str):
        message = event_or_message
        priority = 0
    else:
        message = getattr(event_or_message, "message", "") or ""
        priority = int(getattr(event_or_message, "priority", 0) or 0)

    if FILTER_MIN_PRIORITY > 0 and priority < FILTER_MIN_PRIORITY:
        return False

    if _include and not any(r.search(message) for r in _include):
        return False

    if any(r.search(message) for r in _exclude):
        return False

    return True
