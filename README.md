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
10. `POST /api/topics/clear_all?token=...`
11. `POST /api/topics/pause_all?token=...`
12. `POST /api/topics/resume_all?token=...`
13. `GET /api/stats?token=...`

## Variables d'environnement

### Essentielles
1. `NTFY_BASE_URL` (defaut `http://ntfy`)
2. `TELEGRAM_ENABLED` (defaut `true`)
3. `TELEGRAM_BOT_TOKEN` (requis si `TELEGRAM_ENABLED=true`)
4. `TELEGRAM_ADMIN_CHAT_ID` (requis si `TELEGRAM_ENABLED=true`)
5. `ADMIN_TOKEN` (requis pour l'UI admin)
6. `ADMIN_WEB_URL` (URL publique vers `/admin?token=...`)
7. `SEND_ADMIN_LINK_ON_START` (defaut `false`)
8. `ADMIN_RECENT_EVENTS` (defaut `50`)
9. `TZ` (defaut `UTC`)
10. `LOG_LEVEL` (defaut `INFO`)

### Topics
1. `BOOTSTRAP_TOPICS`
2. `TOPIC_ALLOWLIST`
3. `TOPIC_DENYLIST`

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
      ADMIN_WEB_URL: "https://notif.example.com/admin?token=your-admin-token"
      SEND_ADMIN_LINK_ON_START: "true"
      BOOTSTRAP_TOPICS: "topic-a,topic-b"
      LOG_LEVEL: "INFO"
      TZ: "Europe/Paris"
```

## Dev
```bash
pytest
flake8 --jobs 1 .
```
