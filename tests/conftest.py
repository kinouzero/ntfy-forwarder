import asyncio
import sys
from pathlib import Path

import pytest
import pytest_asyncio

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import core.config as config  # noqa: E402
import core.state as state  # noqa: E402
from core.http import close_http_session  # noqa: E402
from db import client as db_client  # noqa: E402
import db.schema as db_schema  # noqa: E402
import db.settings as db_settings  # noqa: E402


def drain_queue(q):
    while not q.empty():
        q.get_nowait()


@pytest.fixture(autouse=True)
def tmp_db_paths(tmp_path, monkeypatch):
    paths = {
        "db_path": tmp_path / "ntfy.db",
        "export_dir": tmp_path / "exports",
        "backup_dir": tmp_path / "backups",
    }
    for module in (config, db_schema):
        for key, path in paths.items():
            monkeypatch.setattr(module, key.upper(), str(path))
    monkeypatch.setattr(db_client, "DB_PATH", str(paths["db_path"]))
    monkeypatch.setattr(db_settings, "_CACHE", {"ts": 0, "values": {}})
    return paths


@pytest_asyncio.fixture(autouse=True)
async def isolated_runtime(monkeypatch):
    old_event = state.shutdown_event
    event = asyncio.Event()
    for name, module in list(sys.modules.items()):
        if name == "app" or name.startswith(("core.", "services.", "tasks.", "api.")):
            if getattr(module, "shutdown_event", None) is old_event:
                monkeypatch.setattr(module, "shutdown_event", event)
    for values in (state.workers, state.worker_last_seen, state.aggregation_buffer,
                   state.digest_buffer, state.recent_events, state.topic_stats,
                   state.topic_rates):
        values.clear()
    drain_queue(state.telegram_queue)
    yield
    tasks = [task for task in state.workers.values() if isinstance(task, asyncio.Task)]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await close_http_session()
    state.workers.clear()
    drain_queue(state.telegram_queue)


@pytest_asyncio.fixture
async def initialized_db(tmp_db_paths):
    await db_schema.init_db()
    return tmp_db_paths


@pytest_asyncio.fixture
async def web_client(initialized_db, monkeypatch):
    from aiohttp.test_utils import TestClient, TestServer
    from api import web

    monkeypatch.setattr(web, "ACCESS_TOKEN", "test-token")
    monkeypatch.setattr(web, "OIDC_ENABLED", False)
    monkeypatch.setattr(web, "ACCESS_LOCAL_ENABLED", False)
    monkeypatch.setattr(web, "_HEALTH_TARGET_CACHE", {})
    monkeypatch.setattr(web, "_AUTH_RATE_LIMIT", {})

    async def idle_worker(topic):
        await asyncio.Event().wait()

    monkeypatch.setattr(web, "ntfy_worker", idle_worker)
    async with TestClient(TestServer(await web.create_web_app()), headers={
        "X-Access-Token": "test-token",
    }) as client:
        yield client
