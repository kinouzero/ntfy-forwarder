# Ntfy Forwarder

Forward `ntfy` messages to configurable targets (Telegram / Discord / Slack / generic webhook) with:
- Web UI
- Topic-to-target routing
- Queue, retry, dead-letter queue (DLQ)
- Error history and exports
- Prometheus metrics

## Overview

This project is now **UI-driven** for runtime config:
- Topics are managed in the app
- Targets are managed in the app
- Queue/behavior/performance/summary settings are managed in the app

No environment variables are used anymore for topics/targets/settings categories.

## Quick Start

```yaml
services:
  forwarder:
    build: .
    image: ntfy-forwarder:local
    ports:
      - "8081:8081"
    volumes:
      - forwarder_data:/app/data
    environment:
      NTFY_BASE_URL: "http://ntfy"
      ACCESS_TOKEN: "change-me"
      TZ: "Europe/Paris"
      LOG_LEVEL: "INFO"

      # Optional OIDC
      # OIDC_ENABLED: "true"
      # OIDC_ISSUER_URL: "https://sso.example.com/application/o/forwarder/"
      # OIDC_CLIENT_ID: "ntfy-forwarder"
      # OIDC_CLIENT_SECRET: "..."
      # OIDC_REDIRECT_URI: "https://forwarder.example.com/auth/callback"
      # OIDC_SESSION_SECRET: "change-me-long-random-secret"

      # Optional local login
      # ACCESS_LOCAL_ENABLED: "true"
      # ACCESS_LOCAL_USERNAME: "admin"
      # ACCESS_LOCAL_PASSWORD: "change-me"

volumes:
  forwarder_data:
```

Set `NTFY_BASE_URL` to a reachable ntfy server; the example assumes a service
named `ntfy` on the same Docker network. Run `docker compose up --build -d` and
open `http://localhost:8081/?token=change-me` using your own private access token.

Then:
1. Open `/targets` and create at least one target.
2. Mark one target as default.
3. Open `/` and assign per-topic targets if needed.
4. Open `/settings` to tune runtime behavior.

## Target Model

Supported target kinds:
- `telegram`
- `webhook_discord`
- `webhook_slack`
- `webhook_generic`

Target configs:
- `telegram`: `chat_id`, `bot_token`, `max_message_length`
- `webhook_*`: `url`, optional `auth_header`

Routing behavior:
- If a topic has a linked target, it is used.
- Otherwise the default target is used.

## Runtime Settings (UI)

All of these are configured from `/settings`:
- Retry / queue
- Behavior
- Performance / maintenance
- Aggregation / digest / summary

## Environment Variables

### Core
- `NTFY_BASE_URL` (default: `http://ntfy`)
- `NTFY_TOKEN` (optional)
- `DB_PATH` (default: `/app/data/ntfy.db`)
- `EXPORT_DIR` (default: `exports` beside `DB_PATH`)
- `BACKUP_DIR` (default: `backups` beside `DB_PATH`)
- `TZ` (default: `UTC`)
- `LOG_LEVEL` (default: `INFO`)

### Access
- `ACCESS_TOKEN` (optional)
- `ACCESS_ALLOW_QUERY_TOKEN` (default: `true`)
- `ACCESS_SESSION_SECRET` (optional, fallback: `OIDC_SESSION_SECRET`)

### Access (OIDC)
- `OIDC_ENABLED` (default: `false`)
- `OIDC_ISSUER_URL`
- `OIDC_CLIENT_ID`
- `OIDC_CLIENT_SECRET`
- `OIDC_REDIRECT_URI`
- `OIDC_SESSION_SECRET`
- `OIDC_SESSION_TTL_SECONDS` (default: `86400`)
- `OIDC_STATE_TTL_SECONDS` (default: `300`)
- `OIDC_CLOCK_SKEW_SECONDS` (default: `60`)
- `OIDC_SCOPES` (default: `openid profile email`)
- `OIDC_ALLOWED_EMAILS` (optional)
- `OIDC_ALLOWED_DOMAINS` (optional)
- `OIDC_VERIFY_TLS` (default: `true`)
- `OIDC_REQUIRE_VERIFIED_EMAIL` (default: `false`)
- `OIDC_LOGIN_TEXT` (default: `Login with SSO`)
- `OIDC_LOGIN_ICON` (default: `bi-shield-lock`)

### Access (Local Login)
- `ACCESS_LOCAL_ENABLED` (default: `false`)
- `ACCESS_LOCAL_USERNAME`
- `ACCESS_LOCAL_PASSWORD`
- `ACCESS_LOCAL_SESSION_TTL_SECONDS` (default: `86400`)

## Web Pages

- `GET /`
- `GET /topic/{name}`
- `GET /stats`
- `GET /errors`
- `GET /queue`
- `GET /targets`
- `GET /settings`
- `GET /login`
- `GET /logout`
- `GET /auth/login`
- `GET /auth/callback`
- `GET /auth/logout`

## API

### System
- `GET /health`
- `GET /metrics`

### Topics
- `GET /api/topics`
- `GET /api/topics/{name}`
- `POST /api/topics/{name}/toggle`
- `POST /api/topics/{name}/target`
- `POST /api/topics/{name}/clear`
- `POST /api/topics/{name}/reset_count`
- `POST /api/topics/clear_all`
- `POST /api/topics/hard_clear_all`
- `POST /api/topics/pause_all`
- `POST /api/topics/resume_all`
- `GET /api/topics/export`
- `POST /api/topics/import`

### Targets
- `GET /api/targets`
- `POST /api/targets`
- `PUT /api/targets/{id}`
- `DELETE /api/targets/{id}`
- `POST /api/targets/{id}/default`

### Settings
- `GET /api/settings`
- `POST /api/settings`

### Stats / Errors / DLQ
- `GET /api/stats`
- `GET /api/errors?q=...&offset=0&limit=100&format=csv`
- `POST /api/errors/clear`
- `GET /api/queue/dead_letters?q=...&offset=0&limit=100`
- `POST /api/queue/dead_letters/{id}/requeue`
- `POST /api/queue/dead_letters/{id}/delete`
- `POST /api/queue/dead_letters/requeue_batch`
- `POST /api/queue/dead_letters/clear`

## Security Notes

- Prefer cookie/header auth over query token in public environments (`ACCESS_ALLOW_QUERY_TOKEN=false`).
- Keep `OIDC_VERIFY_TLS=true` in production.
- Restrict OIDC access with `OIDC_ALLOWED_EMAILS` or `OIDC_ALLOWED_DOMAINS`.

## Observability

- Grafana dashboard: `observability/grafana-dashboard.json`
- Prometheus alerts: `observability/prometheus-alerts.yml`

## Development

Requires Python 3.12 or newer.

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
pytest -q --cov --cov-report=term-missing --cov-report=html --cov-report=xml
flake8 --jobs 1 .
```

Coverage includes all application modules and branches; `.coveragerc` enforces
**100% coverage**. Open `htmlcov/index.html` for per-file results. CI runs the same
check and publishes the HTML/XML reports. Tests use temporary SQLite databases,
the real `aiosqlite` driver, local HTTP servers and RSA-signed OIDC tokens.
They require no external Telegram account or ntfy server.

For local use, set `DB_PATH` to a writable path, for example:

```bash
DB_PATH=./data/ntfy.db ACCESS_TOKEN=your-token NTFY_BASE_URL=http://localhost:8080 python app.py
```

## Delivery and troubleshooting

- Configure an enabled default target, or a target for each topic. Missing
  destinations trigger retries and eventual preservation in the dead-letter queue.
- Runtime behavior uses **Settings** in SQLite. Equal quiet-hours boundaries disable
  filtering; defaults filter priorities below 4 between 23:00 and 07:00 in `TZ`.
  The default aggregation interval is 30 seconds.
- **Errors** shows failures; **Queue** lets you inspect and requeue dead letters.
  Requeue preserves the topic, title and attachment of raw messages received while
  a topic was disabled.
- `/health` returns HTTP 503 if SQLite is unavailable and reports stale workers and
  destination failures in its JSON. Docker runs `healthcheck.sh` automatically.
- SQLite is the authoritative delivery queue. Moves to/from the dead-letter queue
  are transactional. Graceful shutdown flushes partial reception/aggregation buffers.
- Queued delivery is at least once: a crash after acceptance by a destination but
  before acknowledgment commits may cause a duplicate. Reception/aggregation buffers
  remain in memory; forced termination before enqueue can lose buffered events.
  ntfy cache replay across restarts is not implemented.
- Backups use SQLite's online backup API and include committed WAL data, producing
  `.db` and `.db.gz` files in `BACKUP_DIR`.
