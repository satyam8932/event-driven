#!/usr/bin/env bash
# Requeue all messages from a DLQ back to the main exchange.
# Usage: requeue-dlq.sh <stage>  (e.g. requeue-dlq.sh tts)
set -euo pipefail

STAGE="${1:?Usage: requeue-dlq.sh <stage>}"
RABBITMQ_URL="${RABBITMQ_URL:-http://guest:guest@localhost:15672}"
DLQ="q.${STAGE}.dlq"
EXCHANGE="pipeline"
ROUTING_KEY="job.${STAGE}"

echo "==> Requeuing from $DLQ to $EXCHANGE ($ROUTING_KEY)..."

# Use rabbitmq management API to get and requeue messages
COUNT=0
while true; do
    MSG=$(curl -sf -X POST "$RABBITMQ_URL/api/queues/%2F/$DLQ/get" \
        -H "Content-Type: application/json" \
        -d '{"count":1,"ackmode":"ack_requeue_false","encoding":"auto"}' 2>/dev/null || echo "[]")

    if [ "$MSG" = "[]" ] || [ -z "$MSG" ]; then
        break
    fi

    PAYLOAD=$(echo "$MSG" | python3 -c "import sys,json,base64; msgs=json.load(sys.stdin); print(msgs[0]['payload'])" 2>/dev/null || break)

    curl -sf -X POST "$RABBITMQ_URL/api/exchanges/%2F/$EXCHANGE/publish" \
        -H "Content-Type: application/json" \
        -d "{\"routing_key\":\"$ROUTING_KEY\",\"payload\":\"$PAYLOAD\",\"payload_encoding\":\"string\",\"properties\":{\"delivery_mode\":2}}" > /dev/null

    COUNT=$((COUNT + 1))
    echo "    Requeued message $COUNT"
done

echo "==> Done. Requeued $COUNT messages."
