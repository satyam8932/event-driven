#!/usr/bin/env bash
# Kill a random worker container mid-processing to test crash recovery.
set -euo pipefail

WORKER_ID=$(docker compose ps -q worker | head -1)
if [ -z "$WORKER_ID" ]; then
    echo "No worker container found. Is the stack running?" >&2
    exit 1
fi

echo "==> Killing worker $WORKER_ID (SIGKILL)..."
docker kill "$WORKER_ID"
echo "    Done. Another worker should pick up in-flight messages via RabbitMQ redelivery."
echo "    Monitor with: docker compose logs -f worker"
