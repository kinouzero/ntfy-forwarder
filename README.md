# Zero Notification Forwarder

Forwarder `ntfy` vers plusieurs targets (Telegram, Discord, Slack, WhatsApp, webhook), avec:
- persistance SQLite
- filtres / quiet hours / rate limiting
- queue + retry + DLQ
- interface admin web
- métriques Prometheus

## Démarrage rapide

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

      # Targets auto-détectées par présence des variables
      TELEGRAM_BOT_TOKEN: "..."
      TELEGRAM_ADMIN_CHAT_ID: "123456789"
      DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/..."
      SLACK_WEBHOOK_URL: "https://hooks.slack.com/services/..."
      GENERIC_WEBHOOK_URL: "https://example.com/webhook"
      # GENERIC_WEBHOOK_AUTH_HEADER: "Bearer ..."

      # WhatsApp Cloud API (optionnel)
      # WHATSAPP_PHONE_NUMBER_ID: "..."
      # WHATSAPP_ACCESS_TOKEN: "..."
      # WHATSAPP_TO: "..."
```

## Activation des targets

Les targets actives sont auto-détectées selon les variables présentes:
- `telegram`: `TELEGRAM_BOT_TOKEN` + `TELEGRAM_ADMIN_CHAT_ID`
- `discord`: `DISCORD_WEBHOOK_URL`
- `slack`: `SLACK_WEBHOOK_URL`
- `webhook`: `GENERIC_WEBHOOK_URL`
- `whatsapp`: `WHATSAPP_PHONE_NUMBER_ID` + `WHATSAPP_ACCESS_TOKEN` + `WHATSAPP_TO`

## Variables d’environnement

### Essentielles
- `NTFY_BASE_URL` (défaut: `http://ntfy`)
- `NTFY_TOKEN`
- `ADMIN_TOKEN` (requis pour l’UI admin)
- `ADMIN_RECENT_EVENTS` (défaut: `50`)
- `DB_PATH` (défaut: `/app/data/ntfy.db`)
- `TZ` (défaut: `UTC`)
- `LOG_LEVEL` (défaut: `INFO`)

### Targets
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_ADMIN_CHAT_ID`
- `TELEGRAM_MAX_MESSAGE_LENGTH` (défaut: `4096`)
- `DISCORD_WEBHOOK_URL`
- `SLACK_WEBHOOK_URL`
- `GENERIC_WEBHOOK_URL`
- `GENERIC_WEBHOOK_AUTH_HEADER` (optionnel)
- `WHATSAPP_PHONE_NUMBER_ID`
- `WHATSAPP_ACCESS_TOKEN`
- `WHATSAPP_TO`
- `WHATSAPP_API_BASE` (défaut: `https://graph.facebook.com`)
- `WHATSAPP_API_VERSION` (défaut: `v23.0`)

### Retry / queue
- `DELIVERY_QUEUE_MAX_ATTEMPTS` (défaut: `8`)
- `DELIVERY_QUEUE_BASE_RETRY_SECONDS` (défaut: `5`)
- `DELIVERY_QUEUE_MAX_RETRY_SECONDS` (défaut: `300`)

### Topics
- `BOOTSTRAP_TOPICS`

### Comportement
- `QUIET_HOURS_START` (défaut: `23`)
- `QUIET_HOURS_END` (défaut: `7`)

### Performance / maintenance
- `DB_BATCH_SIZE` (défaut: `1`)
- `DB_BATCH_FLUSH_SECONDS` (défaut: `1`)
- `RETENTION_DAYS` (défaut: `30`)
- `ERROR_RETENTION_DAYS` (défaut: `7`)
- `DB_MAINTENANCE_INTERVAL_SECONDS` (défaut: `3600`)

### Agrégation / digest / résumé
- `AGGREGATION_INTERVAL` (défaut: `30`)
- `AGGREGATION_MIN_COUNT` (défaut: `10`)
- `MAX_AGGREGATION_BUFFER` (défaut: `1000`)
- `MAX_DIGEST_BUFFER` (défaut: `1000`)
- `DAILY_SUMMARY_ENABLED` (défaut: `true`)
- `DAILY_SUMMARY_HOUR` (défaut: `8`)
- `DAILY_SUMMARY_MINUTE` (défaut: `0`)

## API

### Système
- `GET /health`
- `GET /metrics`

### Admin pages
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

### Stats / erreurs / DLQ API
- `GET /api/stats?token=...`
- `GET /api/errors?token=...&q=...&offset=0&limit=100&format=csv`
- `POST /api/errors/clear?token=...`
- `GET /api/queue/dead_letters?token=...&q=...&offset=0&limit=100`
- `POST /api/queue/dead_letters/{id}/requeue?token=...`
- `POST /api/queue/dead_letters/{id}/delete?token=...`
- `POST /api/queue/dead_letters/requeue_batch?token=...`
- `POST /api/queue/dead_letters/clear?token=...`

## Observabilité

- Dashboard: `observability/grafana-dashboard.json`
- Alerts: `observability/prometheus-alerts.yml`

## Développement

```bash
pytest -q
flake8 --jobs 1 .
```
