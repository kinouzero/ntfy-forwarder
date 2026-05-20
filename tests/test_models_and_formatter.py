from models.event import NtfyEvent
from services.formatter import build_message


def test_ntfy_event_from_json_defaults():
    evt = NtfyEvent.from_json("topic-1", {"message": "hello"})
    assert evt.topic == "topic-1"
    assert evt.message == "hello"
    assert evt.priority == 3


def test_build_message_escapes_and_formats():
    evt = NtfyEvent.from_json(
        "my_topic",
        {
            "message": "hello _world_",
            "priority": 4,
        },
    )
    evt.title = "title [x]"
    evt.tags = ["a+b", "c*d"]
    evt.attachment = {"url": "https://example.com/file_(1).txt"}

    msg = build_message(evt)

    assert "⚠️" in msg
    assert "*my\\_topic*" in msg
    assert "*title \\[x\\]*" in msg
    assert "Tags: a\\+b, c\\*d" in msg
    assert "hello \\_world\\_" in msg
    assert "https://example\\.com/file\\_\\(1\\)\\.txt" in msg
