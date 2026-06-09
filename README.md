# Zero Notification Forwarder

Service de forward ntfy vers plusieurs cibles (Telegram, Discord, Slack, WhatsApp,
webhook generic) avec aggregation, digest, retention, et UI admin.

## Endpoints
1. `GET /health`
2. `GET /metrics`
3. `GET /admin?token=...`
4. `GET /admin/stats?token=...`
5. `GET /admin/topic/{name}?token=...`
6. `GET /api/topics?token=...`
7. `GET /api/topics/{name}?token=...`
8. `POST /api/topics/{name}/toggle?token=...`
9. `POST /api/topics/{name}/clear?token=...`
10. `POST /api/topics/{name}/reset_count?token=...`
11. `POST /api/topics/clear_all?token=...`
12. `POST /api/topics/pause_all?token=...`
13. `POST /api/topics/resume_all?token=...`
14. `GET /api/stats?token=...`
15. `GET /api/topics/export?token=...`
16. `POST /api/topics/import?token=...`
17. `GET /admin/errors?token=...`
18. `GET /api/errors?token=...&limit=100`
19. `GET /admin/queue?token=...`
20. `GET /api/queue/dead_letters?token=...`
21. `POST /api/queue/dead_letters/{id}/requeue?token=...`
22. `POST /api/queue/dead_letters/{id}/delete?token=...`
23. `POST /api/queue/dead_letters/requeue_batch?token=...`

## Variables d'environnement

### Essentielles
1. `NTFY_BASE_URL` (defaut `http://ntfy`)
2. `ADMIN_TOKEN` (requis pour l'UI admin)
3. `ADMIN_RECENT_EVENTS` (defaut `50`)
4. `TZ` (defaut `UTC`)
5. `LOG_LEVEL` (defaut `INFO`)

### Targets
1. Auto-detection: une target est active si ses variables sont presentes.
2. Telegram: `TELEGRAM_BOT_TOKEN` + `TELEGRAM_ADMIN_CHAT_ID`
3. Discord: `DISCORD_WEBHOOK_URL`
4. Slack: `SLACK_WEBHOOK_URL`
5. Webhook generic: `GENERIC_WEBHOOK_URL` (+ `GENERIC_WEBHOOK_AUTH_HEADER` optionnel)
6. WhatsApp: `WHATSAPP_PHONE_NUMBER_ID` + `WHATSAPP_ACCESS_TOKEN` + `WHATSAPP_TO`

### Topics
1. `BOOTSTRAP_TOPICS`
2. `TOPIC_ALLOWLIST`
3. `TOPIC_DENYLIST`

### Filtres avances
1. `FILTER_INCLUDE_REGEX` (liste regex separee par `;;`, optionnel)
2. `FILTER_EXCLUDE_REGEX` (defaut `DEBUG`, liste regex separee par `;;`)
3. `FILTER_MIN_PRIORITY` (defaut `0`, applique sur la priorite ntfy)

### DB / stockage
1. `DB_PATH` (defaut `/app/data/ntfy.db`)

### Retention
1. `RETENTION_DAYS` (defaut `30`)
2. `ERROR_RETENTION_DAYS` (defaut `7`)

### Aggregation / digest
1. `AGGREGATION_INTERVAL` (defaut `30`)
2. `AGGREGATION_MIN_COUNT` (defaut `10`)
3. `MAX_AGGREGATION_BUFFER` (defaut `1000`)
4. `MAX_DIGEST_BUFFER` (defaut `1000`)

### Quiet hours
1. `QUIET_HOURS_START` (defaut `23`)
2. `QUIET_HOURS_END` (defaut `7`)

### Rate limiting
1. `RATE_LIMIT_PER_TOPIC` (defaut `0`)
2. `RATE_LIMIT_WINDOW_SECONDS` (defaut `60`)

### Batch DB
1. `DB_BATCH_SIZE` (defaut `1`)
2. `DB_BATCH_FLUSH_SECONDS` (defaut `1`)

### Targets config
1. `TELEGRAM_BOT_TOKEN` + `TELEGRAM_ADMIN_CHAT_ID` (telegram)
2. `TELEGRAM_MAX_MESSAGE_LENGTH` (defaut `4096`)
3. `DISCORD_WEBHOOK_URL` (discord)
4. `SLACK_WEBHOOK_URL` (slack)
5. `GENERIC_WEBHOOK_URL` (webhook)
6. `GENERIC_WEBHOOK_AUTH_HEADER` (optionnel)
7. `WHATSAPP_PHONE_NUMBER_ID` + `WHATSAPP_ACCESS_TOKEN` + `WHATSAPP_TO` (whatsapp)
8. `WHATSAPP_API_BASE` (defaut `https://graph.facebook.com`)
9. `WHATSAPP_API_VERSION` (defaut `v23.0`)
10. `TELEGRAM_QUEUE_MAX_ATTEMPTS` (defaut `8`, retry global sender)
11. `TELEGRAM_QUEUE_BASE_RETRY_SECONDS` (defaut `5`, retry global sender)
12. `TELEGRAM_QUEUE_MAX_RETRY_SECONDS` (defaut `300`, retry global sender)

### Maintenance DB
1. `DB_MAINTENANCE_INTERVAL_SECONDS` (defaut `3600`)

### Daily summary / health
1. `DAILY_SUMMARY_ENABLED` (defaut `true`)
2. `DAILY_SUMMARY_HOUR` (defaut `8`)
3. `DAILY_SUMMARY_MINUTE` (defaut `0`)

## Notes
1. La queue Telegram est persistante en SQLite (`telegram_queue`) pour reprise apres restart.
2. Les notifications avec piece jointe ntfy envoient aussi un `sendDocument` Telegram si URL disponible.
3. Les messages en echec sortent en DLQ (`telegram_dead_letter`) apres nombre max de tentatives.
4. Dashboards/alerts examples: `observability/grafana-dashboard.json`, `observability/prometheus-alerts.yml`.
5. `/api/errors` supporte recherche/pagination: `q`, `component`, `topic`, `offset`, `limit`, `format=csv`.
6. `/api/queue/dead_letters` supporte filtres/pagination: `topic`, `reason`, `offset`, `limit`.

## Compose example
```yaml
services:
  forwarder:
    image: your-forwarder:latest
    ports:
      - "8081:8081"
    environment:
      NTFY_BASE_URL: "http://ntfy"
      TELEGRAM_BOT_TOKEN: "..."
      TELEGRAM_ADMIN_CHAT_ID: "123456789"
      DISCORD_WEBHOOK_URL: "https://discord.com/api/webhooks/..."
      SLACK_WEBHOOK_URL: "https://hooks.slack.com/services/..."
      ADMIN_TOKEN: "your-admin-token"
      BOOTSTRAP_TOPICS: "topic-a,topic-b"
      LOG_LEVEL: "INFO"
      TZ: "Europe/Paris"
```

## Dev
```bash
pytest
flake8 --jobs 1 .
```
