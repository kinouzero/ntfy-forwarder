import pytest

import app


def test_validate_config_requires_ntfy(monkeypatch):
    with pytest.raises(RuntimeError):
        monkeypatch.setattr(app, "NTFY_BASE_URL", "")
        app.validate_config()


def test_validate_config_allows_targets_autodetection(monkeypatch):
    monkeypatch.setattr(app, "NTFY_BASE_URL", "http://ntfy")
    app.validate_config()


def test_topic_allowed_lists(monkeypatch):
    monkeypatch.setattr(app, "TOPIC_ALLOWLIST", {"a", "b"})
    monkeypatch.setattr(app, "TOPIC_DENYLIST", {"b"})

    assert app.topic_allowed("a") is True
    assert app.topic_allowed("b") is False
    assert app.topic_allowed("c") is False
