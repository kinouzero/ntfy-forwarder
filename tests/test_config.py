import pytest

import app


def test_validate_config_requires_ntfy(monkeypatch):
    with pytest.raises(RuntimeError):
        monkeypatch.setattr(app, "NTFY_BASE_URL", "")
        app.validate_config()


def test_validate_config_allows_ui_managed_targets(monkeypatch):
    monkeypatch.setattr(app, "NTFY_BASE_URL", "http://ntfy")
    app.validate_config()


def test_all_runtime_modules_import_after_ui_migration():
    import importlib
    import pkgutil

    for package_name in ("core", "db", "api", "models", "services", "tasks", "utils"):
        package = importlib.import_module(package_name)
        for module in pkgutil.walk_packages(package.__path__, package_name + "."):
            importlib.import_module(module.name)
