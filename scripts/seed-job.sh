#!/usr/bin/env bash
# Submit a test job and poll until completion or failure
set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
MANUSCRIPT="${1:-"Act 1. The storm arrived at midnight. Thunder cracked the sky open. Scene 2. Old Elara lit a candle. Its flame held steady against the dark. She had waited thirty years for this moment."}"

echo "==> Submitting job..."
RESPONSE=$(curl -sf -X POST "$API_URL/jobs" \
    -H "Content-Type: application/json" \
    -d "{\"manuscript\": \"$MANUSCRIPT\"}")

JOB_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['job_id'])")
echo "    job_id: $JOB_ID"

echo "==> Polling status..."
for i in $(seq 1 60); do
    STATUS=$(curl -sf "$API_URL/jobs/$JOB_ID" | python3 -c "import sys,json; print(json.load(sys.stdin)['status'])")
    echo "    [$i] status: $STATUS"
    if [[ "$STATUS" == "COMPLETED" || "$STATUS" == "FAILED" ]]; then
        echo "==> Final status: $STATUS"
        exit 0
    fi
    sleep 3
done
echo "==> Timed out waiting for completion"
exit 1
