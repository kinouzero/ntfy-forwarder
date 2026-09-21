import os

NTFY_BASE_URL = os.getenv("NTFY_BASE_URL", "http://ntfy").rstrip("/")
NTFY_TOKEN = os.getenv("NTFY_TOKEN")

DB_PATH = os.getenv("DB_PATH", "/app/data/ntfy.db")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
TZ = os.getenv("TZ", "UTC")

RETENTION_DAYS = 30
ERROR_RETENTION_DAYS = 7

AGGREGATION_INTERVAL = 30
AGGREGATION_MIN_COUNT = 10
MAX_AGGREGATION_BUFFER = 1000
MAX_DIGEST_BUFFER = 1000

QUIET_HOURS_START = 23
QUIET_HOURS_END = 7

EXPORT_DIR = "/app/data/exports"
BACKUP_DIR = "/app/data/backups"

DB_BATCH_SIZE = 1
DB_BATCH_FLUSH_SECONDS = 1

DELIVERY_QUEUE_MAX_ATTEMPTS = 8
DELIVERY_QUEUE_BASE_RETRY_SECONDS = 5
DELIVERY_QUEUE_MAX_RETRY_SECONDS = 300
DB_MAINTENANCE_INTERVAL_SECONDS = 3600
DAILY_SUMMARY_ENABLED = True
DAILY_SUMMARY_HOUR = 8
DAILY_SUMMARY_MINUTE = 0
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", "")
ADMIN_RECENT_EVENTS = int(os.getenv("ADMIN_RECENT_EVENTS", "50"))
ACCESS_ALLOW_QUERY_TOKEN = os.getenv(
    "ACCESS_ALLOW_QUERY_TOKEN",
    "true",
).lower() in (
    "1",
    "true",
    "yes",
    "on",
)

OIDC_ENABLED = os.getenv("OIDC_ENABLED", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
OIDC_ISSUER_URL = os.getenv("OIDC_ISSUER_URL", "").strip().rstrip("/")
OIDC_CLIENT_ID = os.getenv("OIDC_CLIENT_ID", "").strip()
OIDC_CLIENT_SECRET = os.getenv("OIDC_CLIENT_SECRET", "").strip()
OIDC_REDIRECT_URI = os.getenv("OIDC_REDIRECT_URI", "").strip()
OIDC_SCOPES = os.getenv("OIDC_SCOPES", "openid profile email").strip()
OIDC_SESSION_SECRET = os.getenv("OIDC_SESSION_SECRET", "").strip()
ACCESS_SESSION_SECRET = os.getenv(
    "ACCESS_SESSION_SECRET",
    OIDC_SESSION_SECRET,
).strip()
OIDC_SESSION_TTL_SECONDS = int(
    os.getenv("OIDC_SESSION_TTL_SECONDS", str(24 * 3600))
)
OIDC_STATE_TTL_SECONDS = int(
    os.getenv("OIDC_STATE_TTL_SECONDS", "300")
)
OIDC_CLOCK_SKEW_SECONDS = int(
    os.getenv("OIDC_CLOCK_SKEW_SECONDS", "60")
)
OIDC_ALLOWED_EMAILS = tuple(
    v.strip().lower()
    for v in os.getenv("OIDC_ALLOWED_EMAILS", "").split(",")
    if v.strip()
)
OIDC_ALLOWED_DOMAINS = tuple(
    v.strip().lower()
    for v in os.getenv("OIDC_ALLOWED_DOMAINS", "").split(",")
    if v.strip()
)
OIDC_VERIFY_TLS = os.getenv("OIDC_VERIFY_TLS", "true").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
OIDC_REQUIRE_VERIFIED_EMAIL = os.getenv(
    "OIDC_REQUIRE_VERIFIED_EMAIL",
    "false",
).lower() in (
    "1",
    "true",
    "yes",
    "on",
)

ACCESS_LOCAL_ENABLED = os.getenv("ACCESS_LOCAL_ENABLED", "false").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
ACCESS_LOCAL_USERNAME = os.getenv("ACCESS_LOCAL_USERNAME", "").strip()
ACCESS_LOCAL_PASSWORD = os.getenv("ACCESS_LOCAL_PASSWORD", "").strip()
ACCESS_LOCAL_SESSION_TTL_SECONDS = int(
    os.getenv("ACCESS_LOCAL_SESSION_TTL_SECONDS", str(24 * 3600))
)
OIDC_LOGIN_TEXT = os.getenv(
    "OIDC_LOGIN_TEXT",
    "Login with SSO",
).strip()
OIDC_LOGIN_ICON = os.getenv(
    "OIDC_LOGIN_ICON",
    "bi-shield-lock",
).strip()
