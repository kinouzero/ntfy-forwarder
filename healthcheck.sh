#!/bin/sh
set -eu
curl -sf --max-time 10 http://localhost:8081/health >/dev/null
test -f "${DB_PATH:-/app/data/ntfy.db}"
