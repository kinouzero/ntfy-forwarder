# Zero Notification Forwarder

Service de forward ntfy -> Telegram avec aggregation, digest, retention, et UI admin.

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
2. `TELEGRAM_ENABLED` (defaut `true`)
3. `TELEGRAM_BOT_TOKEN` (requis si `TELEGRAM_ENABLED=true`)
4. `TELEGRAM_ADMIN_CHAT_ID` (requis si `TELEGRAM_ENABLED=true`)
5. `ADMIN_TOKEN` (requis pour l'UI admin)
6. `ADMIN_RECENT_EVENTS` (defaut `50`)
7. `TZ` (defaut `UTC`)
8. `LOG_LEVEL` (defaut `INFO`)

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

### Telegram
1. `TELEGRAM_MAX_MESSAGE_LENGTH` (defaut `4096`)
2. `TELEGRAM_QUEUE_MAX_ATTEMPTS` (defaut `8`)
3. `TELEGRAM_QUEUE_BASE_RETRY_SECONDS` (defaut `5`)
4. `TELEGRAM_QUEUE_MAX_RETRY_SECONDS` (defaut `300`)

### Maintenance DB
1. `DB_MAINTENANCE_INTERVAL_SECONDS` (defaut `3600`)

### Daily summary / health
1. `DAILY_SUMMARY_ENABLED` (defaut `true`)
2. `DAILY_SUMMARY_HOUR` (defaut `8`)
3. `DAILY_SUMMARY_MINUTE` (defaut `0`)
4. `HEALTH_TELEGRAM_CHECK_ENABLED` (defaut `true`)

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
      TELEGRAM_ENABLED: "true"
      TELEGRAM_BOT_TOKEN: "..."
      TELEGRAM_ADMIN_CHAT_ID: "123456789"
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
