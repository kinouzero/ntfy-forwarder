#!/bin/sh

set -e

curl -sf http://localhost:8081/health >/dev/null

test -f /app/data/ntfy.db

exit 0