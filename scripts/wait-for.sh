#!/usr/bin/env bash
# Usage: wait-for.sh host:port [-- command args]
set -e
TIMEOUT=60
HOST=$(echo "$1" | cut -d: -f1)
PORT=$(echo "$1" | cut -d: -f2)
shift

echo "Waiting for $HOST:$PORT..."
for i in $(seq 1 $TIMEOUT); do
    if nc -z "$HOST" "$PORT" 2>/dev/null; then
        echo "$HOST:$PORT is ready."
        exec "$@"
        exit 0
    fi
    sleep 1
done
echo "Timed out waiting for $HOST:$PORT" >&2
exit 1
