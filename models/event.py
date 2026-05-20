from dataclasses import dataclass

@dataclass
class NtfyEvent:

    topic: str
    message: str
    event_id: str | None = None
    priority: int = 3
    title: str | None = None
    tags: list | None = None
    attachment: dict | None = None

    @classmethod
    def from_json(cls, topic, raw):

        return cls(
            topic=topic,
            message=raw.get("message", ""),
            event_id=raw.get("id"),
            priority=int(raw.get("priority", 3)),
            title=raw.get("title"),
            tags=raw.get("tags"),
            attachment=raw.get("attachment"),
        )
