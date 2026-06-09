import json

import pytest

from db.dead_letter import list_dead_letters
from db.schema import init_db
from db.topics import add_topic, set_topic_enabled
from services import ntfy


class _FakeContent:
    def __init__(self, line):
        self._line = line
        self._sent = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._sent:
            raise StopAsyncIteration
        self._sent = True
        ntfy.shutdown_event.set()
        return self._line


class _FakeResponse:
    def __init__(self, line):
        self.content = _FakeContent(line)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self, line):
        self._line = line

    def get(self, *_args, **_kwargs):
        return _FakeResponse(self._line)


@pytest.mark.asyncio
async def test_ntfy_worker_disabled_topic_routes_to_dlq(tmp_db_paths, monkeypatch):
    await init_db()
    await add_topic("t1")
    await set_topic_enabled("t1", False)

    ntfy.shutdown_event.clear()
    ntfy.topic_stats.clear()
    ntfy.recent_events.clear()

    raw = {"event": "message", "id": "evt-1", "message": "hello"}
    line = (json.dumps(raw) + "\n").encode()

    monkeypatch.setattr(ntfy, "get_http_session", lambda: _FakeSession(line))

    async def fail_insert(*_args, **_kwargs):
        raise AssertionError("disabled topic should not insert regular messages")

    monkeypatch.setattr(ntfy, "insert_messages", fail_insert)

    await ntfy.ntfy_worker("t1")
    ntfy.shutdown_event.clear()

    dlq = await list_dead_letters(limit=10)
    assert len(dlq) == 1
    assert dlq[0]["topic"] == "t1"
    assert dlq[0]["last_error"] == "topic_disabled"
    assert dlq[0]["payload"]["message"] == "hello"
