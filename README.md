# Ntfy Forwarder

`ntfy` forwarder to multiple targets (Telegram, Discord, Slack, WhatsApp, webhook), with:
- web admin interface
- SQLite persistence
- filters / quiet hours / rate limiting
- queue + retry + DLQ
- Prometheus metrics

## Quick Start

```yaml
services:
  forwarder:
    image: your-forwarder:latest
    ports:
      - "8081:8081"
    environment:
      NTFY_BASE_URL: "http://ntfy"
      ADMIN_TOKEN: "change-me"
      BOOTSTRAP_TOPICS: "topic-a,topic-b"
      TZ: "Europe/Paris"
      LOG_LEVEL: "INFO"

      # Targets are auto-detected based on env vars
      TELEGRAM_BOT_TOKEN: "..."
      TELEGRAM_ADMIN_CHAT_ID: "123456789"
      DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/..."
      SLACK_WEBHOOK_URL: "https://hooks.slack.com/services/..."
      GENERIC_WEBHOOK_URL: "https://example.com/webhook"
      # GENERIC_WEBHOOK_AUTH_HEADER: "Bearer ..."

      # WhatsApp Cloud API (optional)
      # WHATSAPP_PHONE_NUMBER_ID: "..."
      # WHATSAPP_ACCESS_TOKEN: "..."
      # WHATSAPP_TO: "..."
```

## Target Activation

Active targets are auto-detected from configured env vars:
- `telegram`: `TELEGRAM_BOT_TOKEN` + `TELEGRAM_ADMIN_CHAT_ID`
- `discord`: `DISCORD_WEBHOOK_URL`
- `slack`: `SLACK_WEBHOOK_URL`
- `webhook`: `GENERIC_WEBHOOK_URL`
- `whatsapp`: `WHATSAPP_PHONE_NUMBER_ID` + `WHATSAPP_ACCESS_TOKEN` + `WHATSAPP_TO`

## Environment Variables

### Required
- `NTFY_BASE_URL` (default: `http://ntfy`)
- `NTFY_TOKEN`
- `ADMIN_TOKEN` (required for admin UI)
- `ADMIN_RECENT_EVENTS` (default: `50`)
- `DB_PATH` (default: `/app/data/ntfy.db`)
- `TZ` (default: `UTC`)
- `LOG_LEVEL` (default: `INFO`)

### Targets
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_ADMIN_CHAT_ID`
- `TELEGRAM_MAX_MESSAGE_LENGTH` (default: `4096`)
- `DISCORD_WEBHOOK_URL`
- `SLACK_WEBHOOK_URL`
- `GENERIC_WEBHOOK_URL`
- `GENERIC_WEBHOOK_AUTH_HEADER` (optional)
- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_ACCESS_TOKEN`
- `WHATSAPP_TO`
- `WHATSAPP_API_BASE` (default: `https://graph.facebook.com`)
- `WHATSAPP_API_VERSION` (default: `v23.0`)

### Retry / queue
- `DELIVERY_QUEUE_MAX_ATTEMPTS` (default: `8`)
- `DELIVERY_QUEUE_BASE_RETRY_SECONDS` (default: `5`)
- `DELIVERY_QUEUE_MAX_RETRY_SECONDS` (default: `300`)

### Topics
- `BOOTSTRAP_TOPICS`

### Behavior
- `QUIET_HOURS_START` (default: `23`)
- `QUIET_HOURS_END` (default: `7`)

### Performance / maintenance
- `DB_BATCH_SIZE` (default: `1`)
- `DB_BATCH_FLUSH_SECONDS` (default: `1`)
- `RETENTION_DAYS` (default: `30`)
- `ERROR_RETENTION_DAYS` (default: `7`)
- `DB_MAINTENANCE_INTERVAL_SECONDS` (default: `3600`)

### Aggregation / digest / summary
- `AGGREGATION_INTERVAL` (default: `30`)
- `AGGREGATION_MIN_COUNT` (default: `10`)
- `MAX_AGGREGATION_BUFFER` (default: `1000`)
- `MAX_DIGEST_BUFFER` (default: `1000`)
- `DAILY_SUMMARY_ENABLED` (default: `true`)
- `DAILY_SUMMARY_HOUR` (default: `8`)
- `DAILY_SUMMARY_MINUTE` (default: `0`)

## API

### System
- `GET /health`
- `GET /metrics`

### Admin Pages
- `GET /admin?token=...`
- `GET /admin/stats?token=...`
- `GET /admin/errors?token=...`
- `GET /admin/queue?token=...`
- `GET /admin/topic/{name}?token=...`

### Topics API
- `GET /api/topics?token=...`
- `GET /api/topics/{name}?token=...`
- `POST /api/topics/{name}/toggle?token=...`
- `POST /api/topics/{name}/clear?token=...`
- `POST /api/topics/{name}/reset_count?token=...`
- `POST /api/topics/clear_all?token=...`
- `POST /api/topics/pause_all?token=...`
- `POST /api/topics/resume_all?token=...`
- `GET /api/topics/export?token=...`
- `POST /api/topics/import?token=...`

### Stats / Errors / DLQ API
- `GET /api/stats?token=...`
- `GET /api/errors?token=...&q=...&offset=0&limit=100&format=csv`
- `POST /api/errors/clear?token=...`
- `GET /api/queue/dead_letters?token=...&q=...&offset=0&limit=100`
- `POST /api/queue/dead_letters/{id}/requeue?token=...`
- `POST /api/queue/dead_letters/{id}/delete?token=...`
- `POST /api/queue/dead_letters/requeue_batch?token=...`
- `POST /api/queue/dead_letters/clear?token=...`

## Observability

- Dashboard: `observability/grafana-dashboard.json`
- Alerts: `observability/prometheus-alerts.yml`

## Development

```bash
pytest -q
flake8 --jobs 1 .
```
