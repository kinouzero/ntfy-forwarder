import os

NTFY_BASE_URL = os.getenv("NTFY_BASE_URL", "http://ntfy").rstrip("/")
NTFY_TOKEN = os.getenv("NTFY_TOKEN")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_ADMIN = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or ""
TELEGRAM_ENABLED = os.getenv("TELEGRAM_ENABLED", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)

DB_PATH = os.getenv("DB_PATH", "/app/data/ntfy.db")

BOOTSTRAP_TOPICS = os.getenv("BOOTSTRAP_TOPICS", "")
TOPIC_ALLOWLIST = {
    t.strip()
    for t in os.getenv("TOPIC_ALLOWLIST", "").split(",")
    if t.strip()
}
TOPIC_DENYLIST = {
    t.strip()
    for t in os.getenv("TOPIC_DENYLIST", "").split(",")
    if t.strip()
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TZ = os.getenv("TZ", "UTC")

RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "30"))
ERROR_RETENTION_DAYS = int(os.getenv("ERROR_RETENTION_DAYS", "7"))

AGGREGATION_INTERVAL = int(os.getenv("AGGREGATION_INTERVAL", "30"))
AGGREGATION_MIN_COUNT = int(os.getenv("AGGREGATION_MIN_COUNT", "10"))
MAX_AGGREGATION_BUFFER = int(os.getenv("MAX_AGGREGATION_BUFFER", "1000"))
MAX_DIGEST_BUFFER = int(os.getenv("MAX_DIGEST_BUFFER", "1000"))

QUIET_HOURS_START = int(os.getenv("QUIET_HOURS_START", "23"))
QUIET_HOURS_END = int(os.getenv("QUIET_HOURS_END", "7"))

EXPORT_DIR = "/app/data/exports"
BACKUP_DIR = "/app/data/backups"

RATE_LIMIT_PER_TOPIC = int(os.getenv("RATE_LIMIT_PER_TOPIC", "0"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))

DB_BATCH_SIZE = int(os.getenv("DB_BATCH_SIZE", "1"))
DB_BATCH_FLUSH_SECONDS = int(os.getenv("DB_BATCH_FLUSH_SECONDS", "1"))

TELEGRAM_MAX_MESSAGE_LENGTH = int(
    os.getenv("TELEGRAM_MAX_MESSAGE_LENGTH", "4096")
)

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
ADMIN_WEB_URL = os.getenv("ADMIN_WEB_URL", "")
SEND_ADMIN_LINK_ON_START = os.getenv(
    "SEND_ADMIN_LINK_ON_START",
    "false",
).lower() in ("1", "true", "yes", "on")
ADMIN_RECENT_EVENTS = int(os.getenv("ADMIN_RECENT_EVENTS", "50"))
