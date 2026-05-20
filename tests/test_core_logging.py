import json

from core.logging import log


def test_log_emits_json(capsys):
    log("INFO", "hello", extra=1)
    out = capsys.readouterr().out.strip()
    payload = json.loads(out)

    assert payload["level"] == "INFO"
    assert payload["msg"] == "hello"
    assert payload["extra"] == 1
