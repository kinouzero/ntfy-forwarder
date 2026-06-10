import os

NTFY_BASE_URL = os.getenv("NTFY_BASE_URL", "http://ntfy").rstrip("/")
NTFY_TOKEN = os.getenv("NTFY_TOKEN")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_ADMIN = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or ""

DB_PATH = os.getenv("DB_PATH", "/app/data/ntfy.db")

BOOTSTRAP_TOPICS = os.getenv("BOOTSTRAP_TOPICS", "")

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

DB_BATCH_SIZE = int(os.getenv("DB_BATCH_SIZE", "1"))
DB_BATCH_FLUSH_SECONDS = int(os.getenv("DB_BATCH_FLUSH_SECONDS", "1"))

TELEGRAM_MAX_MESSAGE_LENGTH = int(
    os.getenv("TELEGRAM_MAX_MESSAGE_LENGTH", "4096")
)
DELIVERY_QUEUE_MAX_ATTEMPTS = int(
    os.getenv("DELIVERY_QUEUE_MAX_ATTEMPTS", "8")
)
DELIVERY_QUEUE_BASE_RETRY_SECONDS = int(
    os.getenv("DELIVERY_QUEUE_BASE_RETRY_SECONDS", "5")
)
DELIVERY_QUEUE_MAX_RETRY_SECONDS = int(
    os.getenv("DELIVERY_QUEUE_MAX_RETRY_SECONDS", "300")
)
DB_MAINTENANCE_INTERVAL_SECONDS = int(
    os.getenv("DB_MAINTENANCE_INTERVAL_SECONDS", "3600")
)
DAILY_SUMMARY_ENABLED = os.getenv("DAILY_SUMMARY_ENABLED", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
DAILY_SUMMARY_HOUR = int(os.getenv("DAILY_SUMMARY_HOUR", "8"))
DAILY_SUMMARY_MINUTE = int(os.getenv("DAILY_SUMMARY_MINUTE", "0"))
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
ADMIN_RECENT_EVENTS = int(os.getenv("ADMIN_RECENT_EVENTS", "50"))

GENERIC_WEBHOOK_URL = os.getenv("GENERIC_WEBHOOK_URL", "").strip()
GENERIC_WEBHOOK_AUTH_HEADER = os.getenv(
    "GENERIC_WEBHOOK_AUTH_HEADER",
    "",
).strip()

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

WHATSAPP_API_BASE = os.getenv(
    "WHATSAPP_API_BASE",
    "https://graph.facebook.com",
).rstrip("/")
WHATSAPP_API_VERSION = os.getenv("WHATSAPP_API_VERSION", "v23.0").strip()
WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID", "").strip()
WHATSAPP_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN", "").strip()
WHATSAPP_TO = os.getenv("WHATSAPP_TO", "").strip()

ACTIVE_TARGETS = tuple(
    t for t, enabled in (
        ("telegram", bool(TG_TOKEN and TG_ADMIN)),
        ("webhook", bool(GENERIC_WEBHOOK_URL)),
        ("discord", bool(DISCORD_WEBHOOK_URL)),
        ("slack", bool(SLACK_WEBHOOK_URL)),
        (
            "whatsapp",
            bool(WHATSAPP_PHONE_NUMBER_ID and WHATSAPP_ACCESS_TOKEN and WHATSAPP_TO),
        ),
    ) if enabled
)
