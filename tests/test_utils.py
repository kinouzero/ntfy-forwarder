from utils.markdown import escape_md
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


def test_split_message_chunks():
    msg = "alpha beta gamma delta epsilon"
    chunks = split_message(msg, 10)
    assert all(len(c) <= 10 for c in chunks)
    assert "".join(c.replace(" ", "") for c in chunks) == msg.replace(" ", "")
