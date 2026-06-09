import pytest

import app


def test_validate_config_requires_ntfy(monkeypatch):
    with pytest.raises(RuntimeError):
        monkeypatch.setattr(app, "NTFY_BASE_URL", "")
        app.validate_config()


def test_validate_config_requires_telegram_when_enabled(monkeypatch):
    with pytest.raises(RuntimeError):
        monkeypatch.setattr(app, "NTFY_BASE_URL", "http://ntfy")
        monkeypatch.setattr(app, "DELIVERY_TARGETS", ("telegram",))
        monkeypatch.setattr(app, "TG_TOKEN", "")
        monkeypatch.setattr(app, "TG_ADMIN", "")
        app.validate_config()


def test_validate_config_allows_telegram_disabled(monkeypatch):
    monkeypatch.setattr(app, "NTFY_BASE_URL", "http://ntfy")
    monkeypatch.setattr(app, "DELIVERY_TARGETS", ())
    monkeypatch.setattr(app, "TG_TOKEN", "")
    monkeypatch.setattr(app, "TG_ADMIN", "")
    app.validate_config()


def test_validate_config_requires_discord_webhook(monkeypatch):
    with pytest.raises(RuntimeError):
        monkeypatch.setattr(app, "NTFY_BASE_URL", "http://ntfy")
        monkeypatch.setattr(app, "DELIVERY_TARGETS", ("discord",))
        monkeypatch.setattr(app, "DISCORD_WEBHOOK_URL", "")
        app.validate_config()


def test_validate_config_requires_slack_webhook(monkeypatch):
    with pytest.raises(RuntimeError):
        monkeypatch.setattr(app, "NTFY_BASE_URL", "http://ntfy")
        monkeypatch.setattr(app, "DELIVERY_TARGETS", ("slack",))
        monkeypatch.setattr(app, "SLACK_WEBHOOK_URL", "")
        app.validate_config()


def test_validate_config_accepts_generic_webhook_target(monkeypatch):
    monkeypatch.setattr(app, "NTFY_BASE_URL", "http://ntfy")
    monkeypatch.setattr(app, "DELIVERY_TARGETS", ("webhook",))
    monkeypatch.setattr(app, "GENERIC_WEBHOOK_URL", "https://example.invalid/hook")
    app.validate_config()


def test_topic_allowed_lists(monkeypatch):
    monkeypatch.setattr(app, "TOPIC_ALLOWLIST", {"a", "b"})
    monkeypatch.setattr(app, "TOPIC_DENYLIST", {"b"})

    assert app.topic_allowed("a") is True
    assert app.topic_allowed("b") is False
    assert app.topic_allowed("c") is False
