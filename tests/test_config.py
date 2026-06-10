import pytest

import app


def test_validate_config_requires_ntfy(monkeypatch):
    with pytest.raises(RuntimeError):
        monkeypatch.setattr(app, "NTFY_BASE_URL", "")
        app.validate_config()


def test_validate_config_allows_targets_autodetection(monkeypatch):
    monkeypatch.setattr(app, "NTFY_BASE_URL", "http://ntfy")
    app.validate_config()
