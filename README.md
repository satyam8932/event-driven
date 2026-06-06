# Distributed Multi-Modal GenAI Pipeline

Asynchronous, event-driven pipeline that ingests text manuscripts and produces "audio drama" output. Built from core infrastructure primitives — no managed workflow orchestrators.

## Architecture

```
                       ┌─────────────────────────────────────────────────────┐
                       │                   docker-compose                     │
                       │                                                       │
  POST /jobs           │  ┌──────┐   outbox   ┌───────┐   RabbitMQ           │
 ──────────────────►   │  │ API  │──────────►│ Relay │──────────────────┐   │
                       │  └──────┘            └───────┘                  │   │
  GET /jobs/{id}       │                                                  ▼   │
 ◄──────────────────   │  ┌──────────────────────────────────────────────┐   │
                       │  │           pipeline exchange (topic)           │   │
                       │  └──┬──────────────┬──────────┬──────────┬──────┘   │
                       │     │              │          │          │           │
                       │   job.parse   job.tts    job.stitch  job.notify     │
                       │     │              │          │          │           │
                       │  ┌──▼──┐       ┌──▼──┐   ┌──▼──┐   ┌──▼──┐       │
                       │  │parse│       │ tts │   │stitch│  │notify│       │
                       │  │ ×N  │       │ ×N  │   │ ×N  │   │ ×N  │       │
                       │  └──┬──┘       └──┬──┘   └──┬──┘   └──┬──┘       │
                       │     │  outbox      │  outbox  │  outbox  │           │
                       │     └──────────────┴──────────┴──────────┘           │
                       │                                                       │
                       │  Infrastructure: Postgres · Redis · MinIO · RabbitMQ │
                       └─────────────────────────────────────────────────────┘
```

### Pipeline stages

| Stage | What happens | Resilience |
|---|---|---|
| **Ingest** | Manuscript → MinIO; job + outbox row committed atomically | 503 on DB/storage failure |
| **Parse** | Download txt, simulated LLM (15% 500 rate), split into blocks | Retry with backoff → DLQ |
| **TTS** | Per block: Redis counting semaphore (max 3 global) + SHA-256 cache | Semaphore-full → short delay retry; cache hit → skip vendor |
| **Stitch** | Concatenate audio blocks, upload final WAV | Retry with backoff → DLQ |
| **Notify** | POST webhook (or log); job → COMPLETED | Idempotent via `processed_events` |

### Delivery guarantee

**System does NOT provide exactly-once delivery.** It provides: **at-least-once delivery + idempotent consumers = effectively-once processing.** Every stage can receive duplicates; all are safe to re-run without side effects.

## Quick Start

```bash
# Copy env, start everything
cp .env.example .env
make up

# Watch logs
make logs

# Submit a test job
make seed

# Poll status manually
curl http://localhost:8000/jobs/<job_id>
```

## Verification Scenarios

### 1. Happy path
```bash
make seed
# Watch: status transitions PENDING → PARSING → TTS → STITCHING → NOTIFYING → COMPLETED
```

### 2. Idempotency cache (TTS cost control)
```bash
# Submit same manuscript twice — vendor call count must not double
make seed
make seed
# Look for "tts_cache_hit_redis" or "tts_cache_hit_db" in worker logs
docker compose logs worker | grep tts_cache_hit
```

### 3. Global TTS concurrency (max 3)
```bash
# Scale workers, submit ~10 jobs simultaneously
make scale-workers n=3
for i in $(seq 1 10); do make seed & done; wait
# Assert Redis semaphore never exceeds 3
docker compose exec redis redis-cli ZCARD tts:semaphore
```

### 4. Poison pill → DLQ (no queue blocking)
```bash
make seed-poison
# Watch 3 retry attempts with backoff in logs
# Job lands in q.tts.dlq; other concurrent jobs still complete
docker compose logs worker | grep -E "retry_scheduled|routed_to_dlq|permanent_error"
```

### 5. Crash recovery (docker kill)
```bash
# Start a job, kill worker mid-processing
make seed &
sleep 5
make kill-worker
# Another worker (or restarted worker) picks up via RabbitMQ redelivery
docker compose logs worker | grep "message_received"
```

### 6. Broker-down durability (outbox)
```bash
docker compose stop rabbitmq
make seed   # API returns 202; outbox row written
docker compose start rabbitmq
# Relay drains outbox; pipeline proceeds
docker compose logs relay | grep relay_published
```

### 7. Duplicate delivery
```bash
# Use RabbitMQ management UI (localhost:15672) to manually re-publish a JobCreated
# Worker should log "duplicate_event_skipped" with no double-processing
```

## Failure Injection Knobs

| Knob | Where | Default | Effect |
|---|---|---|---|
| `FAILURE_RATE` | `src/app/vendors/llm.py:8` | `0.15` | % chance LLM returns 500 |
| `RETRY_MAX_ATTEMPTS` | `.env` | `3` | Attempts before DLQ |
| `RETRY_BASE_MS` | `.env` | `2000` | Base retry delay (ms) |
| `TTS_MAX_CONCURRENT` | `.env` | `3` | Global TTS semaphore limit |
| `JANITOR_LEASE_TIMEOUT` | `.env` | `120` | Seconds before janitor reclaims stuck task |
| `POISON_MARKER` | `src/app/vendors/tts.py:8` | `__POISON_PILL__` | Manuscript prefix that always fails TTS |

## Service Ports (dev override)

| Service | Port |
|---|---|
| API | 8000 |
| RabbitMQ management | 15672 |
| MinIO console | 9001 |
| Postgres | 5432 |
| Redis | 6379 |

## Development

```bash
# Install deps (requires uv)
uv pip install -e ".[dev]"

# Lint + format
make lint
make fmt

# Type check
make type-check

# Unit tests (no Docker required)
make test-unit

# All tests
make test
```

## Key Design Decisions

- **Outbox pattern**: state change + event emission are one DB transaction. No dual-write risk.
- **No Celery/Temporal**: choreography via RabbitMQ topic exchange + queue-per-stage.
- **Manual ack after commit**: crash during processing → broker redelivers; idempotency absorbs duplicates.
- **ZSET-based semaphore**: atomic Lua script; lease expiry self-heals crashed holders. Full semaphore → delay-queue retry (never blocks consumer).
- **SHA-256 content-addressed storage**: `audio/{hash}.wav` — re-running produces the same key, no duplicates.
- **Janitor**: belt-and-suspenders for the rare "acked then crashed" case via lease reaper + outbox re-emit.
