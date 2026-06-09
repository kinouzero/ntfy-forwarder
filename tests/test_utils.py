import re

from utils.markdown import escape_md
import utils.filters as filters
from utils.rate_limit import RateLimiter
from utils.telegram import split_message
import utils.quiet_hours as quiet_hours


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
    filters._include = []
    filters._exclude = [re.compile(r"DEBUG", re.I)]
    filters.FILTER_MIN_PRIORITY = 0
    assert filters.passes_filters("DEBUG something") is False
    assert filters.passes_filters("debug something") is False
    assert filters.passes_filters("INFO something") is True


def test_passes_filters_include_and_min_priority():
    class Event:
        def __init__(self, message, priority):
            self.message = message
            self.priority = priority

    filters._include = [re.compile(r"alert", re.I)]
    filters._exclude = []
    filters.FILTER_MIN_PRIORITY = 3

    assert filters.passes_filters(Event("this is alert", 4)) is True
    assert filters.passes_filters(Event("this is info", 4)) is False
    assert filters.passes_filters(Event("this is alert", 2)) is False


def test_in_quiet_hours_wraps_midnight(monkeypatch):
    monkeypatch.setattr(quiet_hours, "QUIET_HOURS_START", 23)
    monkeypatch.setattr(quiet_hours, "QUIET_HOURS_END", 7)

    monkeypatch.setattr(quiet_hours, "_now_hour", lambda: 23)
    assert quiet_hours.in_quiet_hours() is True

    monkeypatch.setattr(quiet_hours, "_now_hour", lambda: 6)
    assert quiet_hours.in_quiet_hours() is True

    monkeypatch.setattr(quiet_hours, "_now_hour", lambda: 12)
    assert quiet_hours.in_quiet_hours() is False


def test_in_quiet_hours_same_day_window(monkeypatch):
    monkeypatch.setattr(quiet_hours, "QUIET_HOURS_START", 9)
    monkeypatch.setattr(quiet_hours, "QUIET_HOURS_END", 17)

    monkeypatch.setattr(quiet_hours, "_now_hour", lambda: 10)
    assert quiet_hours.in_quiet_hours() is True

    monkeypatch.setattr(quiet_hours, "_now_hour", lambda: 8)
    assert quiet_hours.in_quiet_hours() is False


def test_in_quiet_hours_disabled_when_same_boundaries(monkeypatch):
    monkeypatch.setattr(quiet_hours, "QUIET_HOURS_START", 8)
    monkeypatch.setattr(quiet_hours, "QUIET_HOURS_END", 8)
    monkeypatch.setattr(quiet_hours, "_now_hour", lambda: 8)
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
