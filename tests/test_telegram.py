import pytest

from services import telegram


class _Resp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    def raise_for_status(self):
        return None

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Session:
    def __init__(self, payload, status=200):
        self._payload = payload
        self._status = status
        self.called = []

    def post(self, url, json, timeout):
        self.called.append((url, json))
        return _Resp(self._payload, self._status)


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

    assert len(session.called) == 1
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

    async def ok_send(_m, attachment_url=None, priority=3):
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
    from tasks import telegram_sender
    from services.telegram import TelegramAPIError

    monkeypatch.setattr(
        telegram_sender,
        "send_telegram_message",
        lambda *_a, **_kw: (_ for _ in ()).throw(TelegramAPIError("boom", retryable=True)),
    )

    with pytest.raises(TelegramAPIError):
        await telegram_sender.process_message("hi")


@pytest.mark.asyncio
async def test_process_queue_item_skips_disabled_topic(monkeypatch):
    from tasks import telegram_sender

    async def disabled(_topic):
        return False

    async def fail_process(_message):
        raise AssertionError("disabled topic should not be sent")

    monkeypatch.setattr(telegram_sender, "is_topic_enabled", disabled)
    monkeypatch.setattr(telegram_sender, "process_message", fail_process)

    status, _delay, _reason = await telegram_sender.process_queue_item(
        {"topic": "t1", "message": "hello"}
    )

    assert status == "drop"


@pytest.mark.asyncio
async def test_process_queue_item_accepts_legacy_string(monkeypatch):
    from tasks import telegram_sender

    calls = []

    async def ok_process(message, attachment_url=None, priority=3):
        calls.append(message)
        return True

    monkeypatch.setattr(telegram_sender, "process_message", ok_process)

    status, _delay, _reason = await telegram_sender.process_queue_item("hello")

    assert status == "sent"
    assert calls == ["hello"]

@pytest.mark.asyncio
async def test_tg_call_raises_on_telegram_error(monkeypatch):
    session = _Session(
        {"ok": False, "description": "Bad Request: CHAT_NOT_FOUND"},
        status=400,
    )

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

    with pytest.raises(RuntimeError, match="CHAT_NOT_FOUND"):
        await telegram.tg_call(
            "sendMessage",
            {"chat_id": "0", "text": "x"},
        )


@pytest.mark.asyncio
async def test_tg_call_429_is_retryable(monkeypatch):
    session = _Session(
        {"ok": False, "description": "Too Many Requests", "parameters": {"retry_after": 8}},
        status=429,
    )
    monkeypatch.setattr(telegram, "get_http_session", lambda: session)
    monkeypatch.setattr(telegram, "TG_API", "https://example.test/botXXX")

    with pytest.raises(telegram.TelegramAPIError) as exc:
        await telegram.tg_call("sendMessage", {"chat_id": "1", "text": "x"})

    assert exc.value.retryable is True
    assert exc.value.retry_after == 8


@pytest.mark.asyncio
async def test_send_telegram_message_sends_attachment(monkeypatch):
    from tasks import telegram_sender

    calls = []

    async def fake_tg_call(method, payload):
        calls.append((method, payload))
        return {"ok": True}

    monkeypatch.setattr(telegram_sender, "tg_call", fake_tg_call)
    monkeypatch.setattr(telegram_sender, "TELEGRAM_MAX_MESSAGE_LENGTH", 4096)

    await telegram_sender.send_telegram_message(
        "hello",
        attachment_url="https://example.com/file.txt",
        priority=1,
    )

    assert calls[0][0] == "sendMessage"
    assert calls[1][0] == "sendDocument"
    assert calls[0][1]["disable_notification"] is True
    assert calls[1][1]["disable_notification"] is True


@pytest.mark.asyncio
async def test_send_telegram_message_high_priority_not_silent(monkeypatch):
    from tasks import telegram_sender

    calls = []

    async def fake_tg_call(method, payload):
        calls.append((method, payload))
        return {"ok": True}

    monkeypatch.setattr(telegram_sender, "tg_call", fake_tg_call)
    monkeypatch.setattr(telegram_sender, "TELEGRAM_MAX_MESSAGE_LENGTH", 4096)

    await telegram_sender.send_telegram_message("hello", priority=5)

    assert calls[0][0] == "sendMessage"
    assert calls[0][1]["disable_notification"] is False


@pytest.mark.asyncio
async def test_process_queue_item_marks_non_retryable_dead(monkeypatch):
    from tasks import telegram_sender
    from services.telegram import TelegramAPIError

    async def enabled(_topic):
        return True

    async def fail(_message, attachment_url=None, priority=3):
        raise TelegramAPIError(
            "forbidden",
            status_code=403,
            retryable=False,
        )

    monkeypatch.setattr(telegram_sender, "is_topic_enabled", enabled)
    monkeypatch.setattr(telegram_sender, "process_message", fail)
    async def _noop(*_a, **_kw):
        return None
    monkeypatch.setattr(telegram_sender, "log_error", _noop)

    status, delay, reason = await telegram_sender.process_queue_item(
        {"topic": "t1", "message": "hello", "attempts": 2}
    )

    assert status == "dead"
    assert delay >= 1
    assert reason == "telegram_4xx"
