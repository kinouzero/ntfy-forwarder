import types

from utils.markdown import escape_md
from utils.filters import passes_filters
from utils.rate_limit import RateLimiter
from utils.telegram import split_message
import utils.quiet_hours as quiet_hours


def _fake_datetime(hour):
    class _DT:
        @classmethod
        def now(cls):
            return types.SimpleNamespace(hour=hour)

    return _DT


def test_escape_md_escapes_special_chars():
    raw = r"_*[]()~`>#+-=|{}.! backslash\\"
    escaped = escape_md(raw)
    assert r"\_" in escaped
    assert r"\*" in escaped
    assert r"\[" in escaped
    assert r"\]" in escaped
    assert r"\(" in escaped
    assert r"\)" in escaped
    assert r"\~" in escaped
    assert r"\`" in escaped
    assert r"\>" in escaped
    assert r"\#" in escaped
    assert r"\+" in escaped
    assert r"\-" in escaped
    assert r"\=" in escaped
    assert r"\|" in escaped
    assert r"\{" in escaped
    assert r"\}" in escaped
    assert r"\." in escaped
    assert r"\!" in escaped


def test_passes_filters_blocks_debug():
    assert passes_filters("DEBUG something") is False
    assert passes_filters("debug something") is False
    assert passes_filters("INFO something") is True


def test_in_quiet_hours_wraps_midnight():
    quiet_hours.QUIET_HOURS_START = 23
    quiet_hours.QUIET_HOURS_END = 7

    quiet_hours.datetime = _fake_datetime(23)
    assert quiet_hours.in_quiet_hours() is True

    quiet_hours.datetime = _fake_datetime(6)
    assert quiet_hours.in_quiet_hours() is True

    quiet_hours.datetime = _fake_datetime(12)
    assert quiet_hours.in_quiet_hours() is False


def test_in_quiet_hours_same_day_window():
    quiet_hours.QUIET_HOURS_START = 9
    quiet_hours.QUIET_HOURS_END = 17

    quiet_hours.datetime = _fake_datetime(10)
    assert quiet_hours.in_quiet_hours() is True

    quiet_hours.datetime = _fake_datetime(8)
    assert quiet_hours.in_quiet_hours() is False


def test_rate_limiter_allows_and_blocks():
    limiter = RateLimiter(2, 10)
    assert limiter.allow(now=0) is True
    assert limiter.allow(now=1) is True
    assert limiter.allow(now=2) is False
    assert limiter.allow(now=20) is True


def test_split_message_chunks():
    msg = "alpha beta gamma delta epsilon"
    chunks = split_message(msg, 10)
    assert all(len(c) <= 10 for c in chunks)
    assert "".join(c.replace(" ", "") for c in chunks) == msg.replace(" ", "")
