import pytest

from services import telegram


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Session:
    def __init__(self, payload):
        self._payload = payload
        self.called = False

    def post(self, url, json, timeout):
        self.called = True
        return _Resp(self._payload)


@pytest.mark.asyncio
async def test_tg_call_uses_http_session(monkeypatch):
    session = _Session({"ok": True})

    monkeypatch.setattr(
        telegram,
        "get_http_session",
        lambda: session,
    )
    monkeypatch.setattr(
        telegram,
        "TG_API",
        "https://example.test/botXXX",
    )

    result = await telegram.tg_call(
        "sendMessage",
        {"chat_id": "1", "text": "hi"},
    )

    assert session.called is True
    assert result == {"ok": True}


@pytest.mark.asyncio
async def test_tg_call_requires_token(monkeypatch):
    monkeypatch.setattr(
        telegram,
        "TG_API",
        None,
    )
    monkeypatch.setattr(
        telegram,
        "get_http_session",
        lambda: None,
    )

    with pytest.raises(RuntimeError):
        await telegram.tg_call(
            "sendMessage",
            {"chat_id": "1", "text": "hi"},
        )


@pytest.mark.asyncio
async def test_process_message_success(monkeypatch):
    from tasks import telegram_sender

    async def ok_send(_m):
        return None

    monkeypatch.setattr(
        telegram_sender,
        "send_telegram_message",
        ok_send,
    )

    result = await telegram_sender.process_message("hi")
    assert result is True


@pytest.mark.asyncio
async def test_process_message_retries_and_fails(monkeypatch):
    async def bad_call(_m, _p):
        raise RuntimeError("boom")

    from tasks import telegram_sender
    monkeypatch.setattr(
        telegram_sender,
        "send_telegram_message",
        lambda _m: bad_call(None, None),
    )
    monkeypatch.setattr(telegram_sender, "MAX_RETRIES", 1)
    async def _sleep(_t):
        return None
    monkeypatch.setattr(telegram_sender.asyncio, "sleep", _sleep)

    async def _noop(*_a, **_kw):
        return None
    monkeypatch.setattr(telegram_sender, "log_error", _noop)

    result = await telegram_sender.process_message("hi")
    assert result is False
