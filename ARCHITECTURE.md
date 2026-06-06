# Architecture: Distributed Multi-Modal GenAI Pipeline

A fully async, choreography-driven backend that accepts text manuscript uploads and produces audio drama output. Built on RabbitMQ + PostgreSQL + Redis + MinIO — no managed workflow orchestrators.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Infrastructure Services](#2-infrastructure-services)
3. [Application Services](#3-application-services)
4. [Source File Map](#4-source-file-map)
5. [Data Model](#5-data-model)
6. [Broker Topology](#6-broker-topology)
7. [End-to-End Data Flow](#7-end-to-end-data-flow)
8. [Core Reliability Mechanisms](#8-core-reliability-mechanisms)
9. [Edge Cases and Gotchas — How Each is Solved](#9-edge-cases-and-gotchas--how-each-is-solved)
10. [SOLID Principles Applied](#10-solid-principles-applied)
11. [Commands and Scripts](#11-commands-and-scripts)
12. [Configuration Reference](#12-configuration-reference)
13. [Optimization Strategy](#13-optimization-strategy)
14. [Scaling to 100,000 Jobs](#14-scaling-to-100000-jobs)
15. [Future Improvements](#15-future-improvements)
16. [Interview Questions This Architecture Answers](#16-interview-questions-this-architecture-answers)

---

## 1. System Overview

```
User
 │
 ▼
[API]  POST /jobs  ──►  PUT manuscript → MinIO
                    ──►  INSERT job(PENDING) + INSERT outbox(JobCreated)  ← one atomic tx
                         │
                    [Relay] polls outbox → publish → RabbitMQ exchange "pipeline"
                         │
              ┌──────────┼──────────────────────────────┐
              ▼          ▼                              ▼
       q.parse      q.tts (semaphore=3)           q.stitch
       [Worker]     [Worker]                      [Worker]
          │              │                              │
          ▼              ▼                              ▼
       q.notify    q.tts.dlq (after 3 fails)      q.stitch.dlq
       [Worker]
```

**Key design choices:**
- Workers never publish to the broker. They write outbox rows; the relay publishes. This makes "update state" and "emit event" atomic.
- No Temporal/Airflow/Celery. Choreography via queue-per-stage.
- Delivery guarantee: **at-least-once delivery + idempotent consumers = effectively-once processing**.

---

## 2. Infrastructure Services

| Service | Image | Role | Healthcheck |
|---|---|---|---|
| `postgres` | `postgres:16-alpine` | Source of truth: jobs, tasks, outbox, dedup ledger, TTS cache | `pg_isready` |
| `rabbitmq` | `rabbitmq:3.13-management-alpine` | Message broker; management UI on `:15672` | `rabbitmq-diagnostics ping` |
| `redis` | `redis:7-alpine` | Distributed TTS semaphore + L1 TTS cache | `redis-cli ping` |
| `minio` | `minio/minio:latest` | Object storage for manuscripts and audio assets | `curl /minio/health/live` |
| `minio-init` | `minio/mc:latest` | One-shot: creates `pipeline` bucket on first boot | exit code 0 |
| `migrate` | app image | One-shot: runs `alembic upgrade head` via async engine; uses `engine.begin()` so DDL commits atomically | exit code 0 |

All app services (`api`, `worker`, `relay`, `janitor`) depend on every infra service being healthy and both one-shot services completing successfully before starting.

> **Migration gotcha:** `migrations/env.py` must use `engine.begin()` (not `engine.connect()`) for the async connection. `begin()` auto-commits on clean exit. `connect()` auto-rolls-back unless `commit()` is explicitly called — alembic logs "Running upgrade" and exits 0, but all DDL is silently discarded. See edge case #32.

---

## 3. Application Services

| Service | Entry point | Role | Scale |
|---|---|---|---|
| `api` | `app.api.main` | FastAPI: accepts uploads, returns status | 1+ |
| `worker` | `app.workers.__main__` | Runs stage consumers (parse/tts/stitch/notify) | N (interesting to scale) |
| `relay` | `app.relay.__main__` | Polls outbox, publishes to broker | 1–2 |
| `janitor` | `app.janitor.__main__` | Lease reaper + table pruning | 1 |

`stop_grace_period: 30s` is set on all four. On `docker stop`, the service gets SIGTERM and has 30 seconds to drain in-flight messages before SIGKILL.

---

## 4. Source File Map

```
src/app/
├── config.py                   pydantic-settings: reads env vars, typed, cached
├── logging.py                  structlog JSON renderer + correlation_id via contextvars
│
├── domain/                     Pure Python — zero I/O (SRP: no infrastructure coupling)
│   ├── enums.py                JobStatus, TaskStage, JOB_STAGE_TRANSITIONS, ROUTING_KEY
│   ├── events.py               EventEnvelope (Pydantic model), TtsCompletedData etc.
│   └── errors.py               RetryableError, PermanentError, SemaphoreFullError,
│                               VendorError, StorageError, PoisonPillError,
│                               DuplicateEventError, StaleTransitionError
│
├── db/
│   ├── engine.py               async SQLAlchemy engine, session factory
│   ├── models.py               ORM: Job, Task, OutboxEvent, ProcessedEvent, TtsCache
│   └── uow.py                  unit_of_work() async context manager (begin/commit/rollback)
│
├── repositories/
│   ├── interfaces.py           Protocol definitions (DIP/ISP) — callers depend on abstractions
│   ├── job_repo.py             JobRepository: create, get, transition_status, mark_failed
│   ├── task_repo.py            TaskRepository: create, claim, complete, fail, mark_dead,
│   │                           reset_to_queued, find_expired_leases
│   ├── outbox_repo.py          OutboxRepository: add, fetch_unpublished (SKIP LOCKED),
│   │                           mark_published, mark_failed, prune_published
│   └── cache_repo.py           ProcessedEventRepository: record (ON CONFLICT DO NOTHING),
│                               prune_old
│                               TtsCacheRepository: get, set
│
├── infra/
│   ├── broker.py               aio-pika RobustConnection factory, connection singleton
│   ├── storage.py              MinIO/S3 put_object/get_object; separate internal vs public
│   │                           endpoints so presigned URLs resolve from the browser
│   ├── redis.py                redis.asyncio client singleton
│   ├── semaphore.py            ZSET-based leased counting semaphore (3 Lua scripts)
│   └── locks.py                Per-hash Redis SET NX lock; prevents TTS cache stampede
│
├── messaging/
│   ├── topology.py             Declares all exchanges, queues, bindings, DLX, delay queues
│   ├── retry.py                schedule_retry (backoff + jitter + new event_id),
│   │                           route_to_dlq, republish_for_semaphore_retry
│   └── consumer.py             Generic StageConsumer (OCP); _reset_task_for_retry,
│                               _mark_task_dead handle DB state on error paths
│
├── vendors/
│   ├── llm.py                  Simulated LLM parse: asyncio.sleep + 15% injected failure
│   └── tts.py                  Simulated TTS: asyncio.sleep; "__POISON_PILL__" → PoisonPillError
│
├── services/                   Use-cases (SRP): one handler per pipeline stage
│   ├── ingestion.py            PUT manuscript → MinIO, INSERT job + parse task + outbox (one tx)
│   ├── parsing.py              dedup+claim tx → LLM → PUT parsed.json → result+next tx
│   ├── tts.py                  dedup+claim tx → per-block semaphore+cache → result+next tx
│   ├── stitch.py               dedup+claim tx → combine audio → PUT final.wav → result+next tx
│   └── notify.py               dedup+claim tx → fire webhook → mark job COMPLETED
│
├── workers/
│   └── __main__.py             Reads WORKER_STAGES env var; starts one StageConsumer per stage
│
├── relay/
│   └── __main__.py             Polls outbox every 0.5s; FOR UPDATE SKIP LOCKED; publishes;
│                               marks published_at after broker ACK
│
├── janitor/
│   └── __main__.py             reap_expired_leases, prune_outbox, prune_processed_events
│
└── api/
    ├── main.py                 FastAPI app, lifespan (broker + db init)
    ├── schemas.py              Request/response Pydantic models
    └── routes/
        ├── jobs.py             POST /jobs, GET /jobs/{id}
        └── health.py           GET /health
```

### Why each file/layer exists

**`domain/`** — pure types with no imports from infra or db. If you change the database or broker, domain types never change. The error taxonomy (Retryable vs Permanent) is domain knowledge — it belongs here, not in messaging/.

**`repositories/interfaces.py`** — Protocol types let service code declare what it needs without knowing the implementation. Tests can inject mocks without touching SQLAlchemy.

**`infra/semaphore.py`** — The counting semaphore lives in infra, not services, because it's a Redis primitive. The TTS limit (3) is a business constraint, but the enforcement mechanism is infrastructure.

**`messaging/consumer.py`** — Generic. The same class handles all 4 pipeline stages. The handler callable is injected. New stages require no edits to consumer.py (Open/Closed Principle).

**`relay/`** — The relay is a separate service rather than code inside the worker because:
1. Workers should never touch the broker directly — workers write outbox rows.
2. The relay can be restarted independently without affecting workers.
3. Relay crash-safety is isolated: if it crashes after broker-ACK but before writing `published_at`, no messages are lost.

**`janitor/`** — Separate service so it can run on a slow interval without holding worker resources. It's the belt-and-suspenders for crash recovery, not the primary recovery path.

---

## 5. Data Model

### `jobs`
```
id             uuid PK
status         VARCHAR(32)   PENDING→PARSING→TTS→STITCHING→NOTIFYING→COMPLETED | FAILED
manuscript_key TEXT          MinIO object key for the uploaded text
final_key      TEXT (null)   MinIO object key for the stitched audio
correlation_id uuid          Propagated through all log lines and events
created_at     timestamptz
updated_at     timestamptz
```

### `tasks`
One row per (job_id, stage). The pipeline processes stages sequentially; each stage has exactly one task row.
```
id             uuid PK
job_id         uuid FK → jobs.id CASCADE DELETE
stage          VARCHAR(32)   parse | tts | stitch | notify
status         VARCHAR(32)   QUEUED → PROCESSING → DONE | FAILED | DEAD
attempts       INT
locked_by      VARCHAR(128)  worker-id that claimed it
lock_expires_at timestamptz  DB clock; janitor reaps if expired
input_ref      TEXT          MinIO key or JSON for stage input
output_ref     TEXT          MinIO key or JSON for stage output
error          TEXT (null)
```
**Indexes:** `(job_id, stage)` unique; `(status, lock_expires_at)` for janitor scan.

### `outbox`
```
id             bigserial PK
aggregate_id   uuid          job_id — for correlation
event_type     VARCHAR(128)
routing_key    VARCHAR(128)  e.g. "job.tts"
payload        JSONB         full EventEnvelope
occurred_at    timestamptz
published_at   timestamptz (null)  NULL = not yet published
attempts       INT
```
**Partial index:** `WHERE published_at IS NULL` — keeps relay scans fast as the table grows.

### `processed_events`
Consumer-side dedup ledger.
```
event_id       uuid PK       one UUID per event delivery attempt
stage          VARCHAR(32)
processed_at   timestamptz   pruned by janitor after 24h
```

### `tts_cache`
Durable mirror of the Redis TTS cache. Redis is L1 (in-memory, 1h TTL). This table is L2 (survives Redis restart).
```
text_hash      char(64) PK   SHA-256 of the text block
object_key     TEXT          MinIO key for the generated audio
created_at     timestamptz
```

### State machines
State transitions are enforced by `UPDATE ... WHERE status = <expected> RETURNING id`. Zero rows returned = wrong state = skip + ack.

```
Job:  PENDING → PARSING → TTS → STITCHING → NOTIFYING → COMPLETED
                                                        ↘ FAILED (from any state)

Task: QUEUED → PROCESSING → DONE
                          ↘ FAILED (reset to QUEUED for retry)
                          ↘ DEAD   (after 3 retries or PermanentError)
```

---

## 6. Broker Topology

```
pipeline (topic exchange, durable)
│
├── routing key "job.parse"  →  q.parse  ──[DLX dlx.parse]──►  q.parse.dlq
├── routing key "job.tts"    →  q.tts    ──[DLX dlx.tts]────►  q.tts.dlq
├── routing key "job.stitch" →  q.stitch ──[DLX dlx.stitch]──►  q.stitch.dlq
└── routing key "job.notify" →  q.notify ──[DLX dlx.notify]──►  q.notify.dlq

pipeline.delay (direct exchange, durable)
│
├── routing key "q.delay.parse.2000"  →  q.delay.parse.2000  (TTL=2s, DLX→pipeline, DLX-RK=job.parse)
├── routing key "q.delay.parse.4000"  →  q.delay.parse.4000  (TTL=4s, ...)
├── routing key "q.delay.parse.8000"  →  q.delay.parse.8000  (TTL=8s, ...)
│   (same pattern for tts / stitch / notify — 4 stages × 3 buckets = 12 delay queues)
```

**Delay queue trick (no plugin required):** When a message needs retry, it's published to a **per-stage** delay queue with `x-message-ttl` and `x-dead-letter-routing-key: job.{stage}`. When TTL expires, RabbitMQ dead-letters back to `pipeline` exchange using that fixed routing key, routing to the correct work queue. This achieves delayed retry using only core RabbitMQ features.

**Why per-stage delay queues (not shared):** A shared `q.delay.2000` queue has no `x-dead-letter-routing-key`. RabbitMQ then uses the message's original routing key when dead-lettering — which was `q.delay.2000`, not `job.parse`. That routing key has no binding on `pipeline`, so the message is silently dropped. Per-stage queues fix this by setting `x-dead-letter-routing-key` to the correct work queue routing key at declaration time.

**Why per-stage DLQs instead of one global DLQ:** Per-stage DLQs allow operators to inspect and replay failures by stage. A tts DLQ failure can be replayed after fixing the TTS vendor without touching parse DLQ entries.

**Publisher confirms:** aio-pika enables `publisher_confirms=True` by default when creating a channel. `await exchange.publish(message, routing_key=...)` blocks until the broker sends `Basic.Ack`. The relay marks `published_at` only after this returns — no silent drops.

---

## 7. End-to-End Data Flow

### Happy path

```
1. POST /jobs {manuscript: "..."}
   API:
   ├── PUT manuscript → MinIO at "manuscripts/{job_id}/manuscript.txt"
   ├── BEGIN TX
   │   ├── INSERT INTO jobs (id, status=PENDING, manuscript_key, correlation_id)
   │   ├── INSERT INTO tasks (job_id, stage=parse, status=QUEUED, input_ref=manuscript_key)
   │   └── INSERT INTO outbox (routing_key="job.parse", payload=JobCreated envelope)
   └── COMMIT → return {job_id, status: PENDING}

2. Relay loop (every 0.5s):
   SELECT ... FROM outbox WHERE published_at IS NULL
   FOR UPDATE SKIP LOCKED LIMIT 50
   → finds the JobCreated row
   → await exchange.publish(message, routing_key="job.parse")  # waits for broker ACK
   → UPDATE outbox SET published_at=now()

3. Worker (parse stage) receives message from q.parse:
   BEGIN TX
   ├── INSERT INTO processed_events (event_id) ON CONFLICT DO NOTHING → True (new)
   ├── SELECT task WHERE job_id=X AND stage=parse
   └── UPDATE task SET status=PROCESSING, locked_by=..., lock_expires_at=now()+120s
       WHERE status=QUEUED RETURNING id  → got it
   COMMIT
   
   [OUTSIDE TX] download manuscript from MinIO
   [OUTSIDE TX] await llm.parse_manuscript(text)  → blocks list
   [OUTSIDE TX] PUT parsed/{job_id}/blocks.json → MinIO
   
   BEGIN TX
   ├── UPDATE task SET status=DONE, output_ref=parsed_key
   ├── INSERT INTO tasks (job_id, stage=tts, status=QUEUED, input_ref=parsed_key)
   └── INSERT INTO outbox (routing_key="job.tts", payload=ParseCompleted)
   COMMIT
   
   ACK message

4. Relay → publish to q.tts

5. Worker (tts stage):
   Same pattern: dedup+claim TX → per-block semaphore+cache → result TX → ACK

6. Worker (stitch stage):
   dedup+claim TX → combine audio → PUT final/{job_id}.wav → result TX → ACK

7. Worker (notify stage):
   dedup+claim TX → fire webhook → UPDATE job SET status=COMPLETED → ACK

GET /jobs/{id} → SELECT FROM jobs → returns {status, final_key}
```

### Error paths

**Transient error (VendorError, StorageError):**
```
Worker handler raises RetryableError
Consumer:
1. _reset_task_for_retry(job_id):  UPDATE task SET status=QUEUED WHERE status=PROCESSING
2. schedule_retry():
   - increment x-attempt header
   - generate new event_id in message body (critical: prevents dedup blocking retry)
   - publish to q.delay.{stage}.{2000|4000|8000} (jitter applied)
3. ACK original message

After TTL expires: message dead-letters to pipeline exchange with routing key job.{stage} → q.{stage}
Worker tries again with fresh event_id → passes dedup → claims task (now QUEUED) → proceeds
```

**Permanent error (PoisonPillError):**
```
Worker handler raises PermanentError
Consumer:
1. _mark_task_dead(job_id, error):
   UPDATE task SET status=DEAD
   UPDATE job SET status=FAILED
2. route_to_dlq(): publish to q.{stage}.dlq
3. ACK original
```

**Max retries exceeded (3 attempts):**
```
schedule_retry() sees attempt > retry_max_attempts:
→ calls route_to_dlq() instead
→ consumer then calls _mark_task_dead() → job FAILED
```

**Semaphore full:**
```
SemaphoreFullError raised from semaphore.acquire()
Consumer:
1. _reset_task_for_retry(job_id): reset task to QUEUED
2. republish_for_semaphore_retry(): new event_id + publish to q.delay.tts.2000 (shortest bucket, stage-specific)
3. ACK original
Message returns in 2s; tries to acquire semaphore again
```

---

## 8. Core Reliability Mechanisms

### 8.1 Outbox Pattern (no dual-write)

Without the outbox, a worker that commits DB state and then crashes before publishing to the broker loses the event permanently. The outbox solves this:

```
BEGIN TX
  UPDATE task status
  INSERT INTO outbox (routing_key, payload)
COMMIT  ← both succeed or both fail
           ↑
           If crash here, no state change, no event lost.

Relay publishes separately.
If relay crashes after broker-ACK but before writing published_at:
  Row stays NULL → relay re-publishes on restart → duplicate delivery.
  Consumer idempotency absorbs the duplicate. Correct behaviour.
```

### 8.2 Two-Layer Idempotency

Every stage handler applies two guards in the same short transaction:

```python
# Layer 1: dedup ledger
is_new = await event_repo.record(event_id, stage)  # INSERT ON CONFLICT DO NOTHING
if not is_new:
    raise DuplicateEventError()  # already processed, ack and skip

# Layer 2: state-machine guard
claimed = await task_repo.claim(task_id, worker_id, lease_seconds)
# UPDATE task SET status=PROCESSING WHERE status=QUEUED RETURNING id
if not claimed:
    raise StaleTransitionError()  # another worker beat us, ack and skip
```

Layer 1 catches duplicate broker deliveries (same event_id). Layer 2 catches concurrent workers racing on the same task.

**Critical detail:** Retry messages get a **new event_id** (generated in `retry.py`). Without this, the retry would hit Layer 1 with the same event_id (already recorded from the failed attempt) and be silently discarded.

### 8.3 Manual ACK Order

```
process message
  → do work
    → COMMIT result to DB
      → ACK  ← only here, after commit
```

If the worker crashes before ACK, RabbitMQ redelivers. The new delivery hits the idempotency guards — safe.

If the worker crashes after ACK but before commit (rare, tiny window) — the message is gone. The janitor's lease reaper recovers: it finds the task stuck PROCESSING with an expired lease, resets it to QUEUED, and re-emits the stage event via outbox.

### 8.4 Lease Reaper (Janitor)

The janitor runs every 30 seconds (configurable):

```python
expired = SELECT tasks WHERE status='PROCESSING' AND lock_expires_at < now()
for task in expired:
    UPDATE task SET status=QUEUED
    INSERT INTO outbox (re-emit stage event with fresh event_id)
```

This is the belt-and-suspenders. The primary recovery path is broker redelivery (ack-after-commit). The janitor handles the edge case where a worker acked before committing, or where broker redelivery hasn't fired yet.

Lease expiry uses **DB clock** (`datetime.now(UTC)`), never the worker's wall clock. Skew between workers doesn't affect correctness.

### 8.5 Distributed TTS Semaphore (max 3 concurrent)

Redis ZSET where each member is a holder's token and the score is the lease expiry timestamp.

**Acquire (Lua, atomic):**
```lua
ZREMRANGEBYSCORE key '-inf' now   -- evict expired holders
count = ZCARD key
if count < limit:
    ZADD key lease_until token
    return 1                       -- acquired
return 0                           -- full
```

**Why Lua?** Redis is single-threaded but operations between commands are not atomic. Two workers could both read ZCARD=2, both decide "there's room", both ZADD — exceeding the limit. The Lua script executes atomically.

**Crash safety:** If a worker crashes while holding a semaphore slot, its token stays in the ZSET. The `ZREMRANGEBYSCORE` at the start of the next acquire call evicts expired tokens. No manual cleanup needed.

**Semaphore full → don't block:** If acquire fails, the consumer immediately resets the task to QUEUED and republishes to the 2s delay queue. It does NOT busy-wait or hold the message. Holding the message while waiting blocks other messages behind it in the prefetch window — deadlock for all tasks waiting on a full semaphore.

### 8.6 TTS Cache (two-level)

Prevents paying the vendor twice for the same text block.

```
SHA-256(text_block) → hash

L1: Redis GET tts:cache:{hash}           → hit: return cached key (fast path)
L2: SELECT FROM tts_cache WHERE text_hash=hash  → hit: warm Redis + return
Miss: per-hash lock (SET NX) → vendor call → PUT to MinIO → INSERT tts_cache → SET Redis
```

**Cache stampede guard:** Two workers processing identical blocks simultaneously both miss L1 and L2. Without a lock, both call the vendor. The `SET NX` lock ensures only one calls the vendor; the other waits and re-checks after acquiring.

**Content-addressed MinIO keys:** `audio/{hash}.wav` — re-running produces the same object key. Idempotent by construction.

---

## 9. Edge Cases and Gotchas — How Each is Solved

| # | Problem | Solution |
|---|---|---|
| 1 | **Dual-write**: DB commits but broker publish fails | Outbox pattern: both in one TX; relay publishes separately |
| 2 | **Broker down at publish time** | Outbox accumulates; relay drains on recovery |
| 3 | **Retry eaten by dedup**: retry delivers same `event_id`, `processed_events` blocks it | `retry.py` generates a new `event_id` per retry via `_refresh_event_id()` |
| 4 | **Task stuck PROCESSING on retry**: claim fails because status never reset | Consumer calls `_reset_task_for_retry()` before publishing to delay queue |
| 5 | **DLQ never marks job FAILED**: consumer routes to DLQ but skips DB update | Consumer calls `_mark_task_dead()` which marks task DEAD and job FAILED |
| 6 | **Poison pill blocks queue** | Retries via delay queues (not live redelivery); after 3 → DLQ; queue never paused |
| 7 | **Retry thundering herd** | Exponential backoff (2s→4s→8s) + ±30% jitter |
| 8 | **No native delayed retry in RabbitMQ core** | Per-stage TTL delay queues + `x-dead-letter-routing-key: job.{stage}` → DLX back to correct work queue (core features only, no plugin). Shared delay queues fail: RabbitMQ uses the message's routing key when no DLX-RK is set, which routes nowhere on the topic exchange. |
| 9 | **Worker `docker kill` mid-task** | manual-ack-after-commit → auto redelivery; janitor lease reaper as backup |
| 10 | **Ack-before-done = message loss** | Strict order: work → DB commit → then ack |
| 11 | **Semaphore holder crashes, slot leaked** | ZSET lease score; `ZREMRANGEBYSCORE` evicts expired holders on next acquire |
| 12 | **Semaphore acquire/check race** | Single atomic Lua script |
| 13 | **Semaphore full → consumer deadlock** | Republish to short delay queue + ack; never block prefetch window |
| 14 | **Cache stampede (identical blocks)** | Per-hash `SET NX` lock + double-check after acquiring |
| 15 | **Duplicate artifacts on re-run** | Content-addressed S3 keys (`audio/{sha256}.wav`) |
| 16 | **MinIO presigned-URL host mismatch in Docker** | Separate `minio_endpoint` (internal) and `minio_public_endpoint` (host-visible) for presigning |
| 17 | **Long DB locks during vendor I/O** | Claim in short tx → vendor I/O outside any tx → commit result in short tx |
| 18 | **Concurrent Alembic migrations** | One-shot `migrate` service; Alembic advisory lock prevents double-migration |
| 19 | **Lease clock skew across workers** | DB `now()` for lease timestamps; never worker wall clock |
| 20 | **`outbox` table growth** | Partial index `WHERE published_at IS NULL` for fast scans; janitor prunes published rows older than 24h |
| 21 | **`processed_events` table grows unbounded** | Janitor `prune_old()` deletes rows older than 24h |
| 22 | **Duplicate delivery to notify stage = double webhook** | `processed_events` dedup catches it; second delivery → `DuplicateEventError` → ack without re-firing webhook |
| 23 | **Multiple relays double-publish** | `FOR UPDATE SKIP LOCKED` — each relay takes a different batch |
| 24 | **Relay crashes after broker-ACK, before `published_at`** | Row stays NULL → re-published on restart → duplicate delivery → consumer idempotency absorbs it |
| 25 | **Redis restart while semaphore held** | Workers get `ConnectionError` → retryable → republish to delay queue; ZSET lost → slots briefly unenforced; accepted behaviour |
| 26 | **Postgres unavailable at `POST /jobs`** | API returns 503; DB is source of truth — no valid state without it |
| 27 | **MinIO unavailable during worker I/O** | `StorageError` → retryable → delay queue retry; after 3 → DLQ, job FAILED |
| 28 | **Missing `correlation_id` in logs** | Set at job creation; stored in `jobs.correlation_id`; propagated in every outbox payload; set in contextvars at message receive |
| 29 | **SIGKILL without drain** | `stop_grace_period: 30s` gives SIGTERM time to drain; SIGKILL falls back to broker redelivery |
| 30 | **Out-of-order events** | State-machine guard (`UPDATE WHERE status=QUEUED`) rejects invalid transitions |
| 31 | **Exactly-once illusion** | Documented explicitly: at-least-once delivery + idempotent consumers = effectively-once processing |
| 32 | **Alembic migration silently rolls back**: `env.py` used `engine.connect()`, which auto-rollbacks if `commit()` not explicitly called; DDL ran and alembic logged success, but tables were never persisted | Changed to `engine.begin()` — async context manager auto-commits on clean exit; DDL now persists |
| 33 | **ORM UUID type vs VARCHAR migration mismatch**: migration created `id` columns as `VARCHAR(36)`; ORM mapped them as `UUID(as_uuid=False)` → asyncpg casts parameters as `$1::uuid` → PostgreSQL raises `operator does not exist: character varying = uuid` on every SELECT/UPDATE | Migration corrected to use `postgresql.UUID(as_uuid=False)` for all UUID columns |
| 34 | **Env vars look hardcoded but aren't**: `Settings` class defaults like `postgres_host: str = "postgres"` are Python fallbacks, not hardcoded values; pydantic-settings resolves: env var > `.env` file > class default. `alembic.ini` URL is a static placeholder overridden at runtime by `env.py` calling `get_settings()` | 12-factor compliant; override any value via environment variable without touching code |

---

## 10. SOLID Principles Applied

**Single Responsibility (SRP)**
- `domain/` contains pure types and zero I/O.
- Each `services/` file handles exactly one pipeline stage.
- `relay/` only dispatches; `janitor/` only maintains; `api/` only handles HTTP.

**Open/Closed (OCP)**
- `StageConsumer` is generic. Adding a new pipeline stage requires writing a handler function and registering it — no changes to `consumer.py`.
- `topology.py` iterates `STAGES = ["parse", "tts", "stitch", "notify"]`; adding a stage means adding to this list.

**Liskov Substitution (LSP)**
- `ProcessedEventRepository` satisfies `IProcessedEventRepository` Protocol. Tests substitute mock implementations.

**Interface Segregation (ISP)**
- `repositories/interfaces.py` defines separate Protocol types per repository. Services declare only the narrow interface they need, not a fat "God repo" interface.

**Dependency Inversion (DIP)**
- Services import Protocol types from `repositories/interfaces.py`, not concrete SQLAlchemy classes.
- Infra adapters are behind `Protocol` types. Unit tests inject fakes; integration tests inject real impls.

---

## 11. Commands and Scripts

```bash
# Start all services (copies .env.example → .env on first run)
make up

# Tear down (removes volumes)
make down

# Run all tests in Docker (unit + integration)
make test

# Unit tests only
make test-unit

# Integration tests only
make test-integration

# Lint
make lint

# Format (black + ruff --fix)
make fmt

# Type check (mypy)
make type-check

# Submit a test job
make seed

# Submit a poison-pill job (will fail all retries → DLQ)
make seed-poison

# Scale workers to N replicas
make scale-workers n=4

# Kill a worker mid-processing (crash recovery demo)
make kill-worker

# Check API health
make status

# Tail all logs
make logs

# Inspect queues (RabbitMQ management UI)
open http://localhost:15672  # guest/guest

# Inspect MinIO (MinIO console)
open http://localhost:9001   # minioadmin/minioadmin
```

**Manual job status poll:**
```bash
JOB_ID="..."
curl -s http://localhost:8000/jobs/$JOB_ID | python3 -m json.tool
```

**Inspect a DLQ in RabbitMQ:**
```
Management UI → Queues → q.tts.dlq → Get messages
```

**Force-replay from DLQ (manual):**
```
Management UI → q.tts.dlq → Move messages → target: pipeline, routing_key: job.tts
```

---

## 12. Configuration Reference

All values are **not hardcoded** — they are read at runtime via `pydantic-settings` in this priority order:
1. **Environment variable** (highest): `POSTGRES_HOST=myhost docker compose up`
2. **`.env` file**: copy `.env.example` → `.env`, set values there
3. **Python class default** (fallback only): `postgres_host: str = "postgres"` — the value shown in the table below

The defaults use Docker Compose service names (`postgres`, `rabbitmq`, etc.) so they work out-of-the-box with the compose stack. Override any value via env var for staging/production without touching code.

`alembic.ini` contains a `sqlalchemy.url` line — this is a static placeholder for CLI introspection only. `migrations/env.py` always overrides it by calling `get_settings().database_url` at runtime.

| Variable | Default | Purpose |
|---|---|---|
| `POSTGRES_HOST` | `postgres` | DB hostname |
| `POSTGRES_DB` | `pipeline` | DB name |
| `POSTGRES_USER` | `pipeline` | DB user |
| `POSTGRES_PASSWORD` | `pipeline` | DB password |
| `RABBITMQ_HOST` | `rabbitmq` | Broker hostname |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection |
| `MINIO_ENDPOINT` | `minio:9000` | Internal MinIO (Docker network) |
| `MINIO_PUBLIC_ENDPOINT` | `localhost:9000` | External MinIO (presigned URLs) |
| `WORKER_STAGES` | `parse,tts,stitch,notify` | Which stages this worker runs |
| `WORKER_PREFETCH` | `8` | RabbitMQ QoS prefetch per consumer |
| `RELAY_POLL_INTERVAL` | `0.5` | Seconds between outbox polls |
| `RELAY_BATCH_SIZE` | `50` | Max outbox rows per relay cycle |
| `JANITOR_INTERVAL` | `30` | Seconds between janitor runs |
| `JANITOR_LEASE_TIMEOUT` | `120` | Seconds before a PROCESSING task is considered stuck |
| `JANITOR_OUTBOX_PRUNE_AGE` | `86400` | Seconds (24h) — prune threshold for outbox + processed_events |
| `TTS_MAX_CONCURRENT` | `3` | Global TTS semaphore limit |
| `TTS_LEASE_SECONDS` | `60` | TTS semaphore lease duration |
| `RETRY_MAX_ATTEMPTS` | `3` | Max retries before DLQ |
| `RETRY_BASE_MS` | `2000` | First retry delay (ms) |
| `RETRY_MAX_MS` | `8000` | Max retry delay (ms) |
| `WEBHOOK_URL` | _(empty)_ | Webhook endpoint for notifications |

---

## 13. Optimization Strategy

### Database
- **Partial index on outbox**: `WHERE published_at IS NULL` — the relay scans only unpublished rows. Without this, the scan touches every historical row.
- **Composite index on tasks**: `(job_id, stage) UNIQUE` — every service handler does `get_by_job_stage(job_id, stage)`. This is always a single-row PK-equivalent lookup.
- **`(status, lock_expires_at)` index on tasks**: janitor's lease scan `WHERE status='PROCESSING' AND lock_expires_at < now()` hits the index instead of a full table scan.
- **Short transactions**: vendor I/O happens outside any DB transaction. Transactions are open for milliseconds, not seconds. Long transactions hold locks that block concurrent workers.
- **FOR UPDATE SKIP LOCKED**: relay takes a batch without blocking other relay instances. No advisory locks, no custom locking tables.

### Messaging
- **Prefetch (QoS = 8)**: workers prefetch 8 messages. This provides pipeline parallelism within a single worker while limiting per-worker memory usage. Too high and slow messages block fast ones; too low and you lose throughput.
- **Batch relay**: relay takes up to 50 rows per loop, not one-at-a-time. Amortizes round-trip overhead.
- **PERSISTENT delivery**: messages survive broker restart. Worth the write-to-disk cost for guaranteed delivery.

### Redis
- **Lua scripts registered once**: `redis.register_script()` is called at TtsSemaphore construction time and reused. No per-call script loading overhead.
- **L1 Redis + L2 DB cache**: Redis serves TTS cache hits in ~1ms. DB hits are ~5ms. Vendor calls are 500ms+ (simulated). Cache hit rate in production (repeated phrases, common characters) can be very high.

### Python async
- **All I/O is async**: `aio-pika`, `asyncpg`, `redis.asyncio`, `aioboto3`. No blocking calls on the event loop.
- **Per-stage consumers run concurrently**: the worker `__main__` creates all stage consumers and runs them as `asyncio.gather()` tasks — parse/tts/stitch/notify all run in parallel within one process.

---

## 14. Scaling to 100,000 Jobs

### Current capacity baseline (single-node)

With 1 worker container at `WORKER_PREFETCH=8` and `TTS_MAX_CONCURRENT=3`, the bottleneck is TTS (simulated vendor sleep). At 500ms per block and 3 concurrent, peak TTS throughput is ~6 blocks/sec. A job with 5 blocks takes ~1s at full concurrency.

At 100k jobs × 5 blocks = 500k TTS calls / 6 per second ≈ **23 hours**. Need to scale.

### Immediate wins (no architecture changes)

1. **Scale workers horizontally:** `make scale-workers n=20`. Each worker process runs 4 stage consumers. Queue-per-stage choreography means scale is independent per stage — run 20 parse workers, 50 TTS workers, 5 stitch workers.
   ```bash
   docker compose up -d --scale worker=20
   ```

2. **Increase `TTS_MAX_CONCURRENT`**: This is the global Redis semaphore limit. Raise from 3 to 30 to match 10 TTS worker processes × 3 concurrent each.

3. **Increase `RELAY_BATCH_SIZE`**: From 50 to 500. The relay is rarely the bottleneck, but at 100k jobs the outbox can accumulate quickly.

4. **Raise `WORKER_PREFETCH`**: From 8 to 16 for compute-light stages (stitch, notify). For TTS, keep lower to avoid starving the semaphore.

### Capacity at 20 workers + TTS_MAX_CONCURRENT=30

30 concurrent TTS × 2 calls/sec each = 60 TTS/sec. 500k blocks / 60 = ~2.3 hours for 100k jobs. Acceptable for batch workloads.

### Infrastructure scaling (beyond single machine)

#### Horizontal worker scaling
Queue-per-stage allows independent worker pools. Deploy TTS workers separately from parse workers:
```yaml
parse-worker:
  <<: *app-base
  environment:
    WORKER_STAGES: "parse"
  deploy:
    replicas: 5

tts-worker:
  <<: *app-base
  environment:
    WORKER_STAGES: "tts"
  deploy:
    replicas: 50
```

#### PostgreSQL
At 100k jobs × 4 stages = 400k task rows + 400k outbox rows + 400k processed_events rows. Postgres handles 100M+ rows comfortably. For higher write throughput:
- **PgBouncer** connection pooling (SQLAlchemy pool + asyncpg = ~10 connections per worker process; 20 workers = 200 connections, fine without PgBouncer up to ~500 workers)
- Read replicas for `GET /jobs/{id}` status queries
- Partition `outbox` by month if historical data needs to stay long-term

#### RabbitMQ
RabbitMQ single node handles 50k msg/sec easily. For 100k concurrent jobs:
- **Quorum queues** instead of classic for better replication (change `x-queue-type: quorum`)
- RabbitMQ cluster (3 nodes) for HA; replication handled by quorum consensus

#### Redis
Single Redis node at 100k jobs: semaphore ZSET rarely exceeds `TTS_MAX_CONCURRENT` entries. No issue. For HA:
- Redis Sentinel (1 primary + 2 replicas) for failover
- Or Redlock (3 independent Redis nodes) for stronger semaphore guarantees

#### MinIO
Object storage scales horizontally. For production: MinIO distributed mode (4+ nodes) or replace with S3.

#### API gateway
FastAPI is stateless — scale via load balancer (nginx, Caddy, ALB). Each API instance needs DB + MinIO access only.

### If 100k jobs must complete in under 1 hour

Required TTS throughput: 500k blocks / 3600s ≈ 139 blocks/sec.
At 500ms/block: need 70 concurrent TTS workers.
Set `TTS_MAX_CONCURRENT=70`, deploy 70 TTS worker replicas (1 concurrent each) or 35 replicas × 2 concurrent.

For sub-1-hour with real vendors: batch TTS API calls (many vendors support batch endpoints), async streaming output, or pre-warm caches for common phrases.

### 100k jobs summary

| Lever | Default | For 100k jobs |
|---|---|---|
| Worker replicas | 1 | 20–50 |
| `TTS_MAX_CONCURRENT` | 3 | 30–70 |
| `RELAY_BATCH_SIZE` | 50 | 200–500 |
| `WORKER_PREFETCH` | 8 | 16 (non-TTS) |
| Postgres connections | pool=5 | PgBouncer |
| RabbitMQ queue type | classic | quorum |

---

## 15. Future Improvements

### Observability
- Add OpenTelemetry spans: one span per stage handler, child spans for vendor calls. Export to Jaeger or Tempo.
- Prometheus metrics: `job_completed_total`, `job_failed_total`, `tts_cache_hit_ratio`, `semaphore_acquire_wait_ms`, `outbox_lag` (count of unpublished rows).
- Alerting: alert on `q.tts.dlq` depth > 0 (job failures), relay lag > 30s (relay down?), semaphore blocked > 60s.

### Exactly-once delivery
Current: at-least-once + idempotent consumers = effectively-once. True exactly-once would require transactional outbox + transactional consumers (both read/write in same DB). Not worth the complexity for this workload.

### Webhook reliability
Current: notify stage fires webhook and logs failure on error. Job is marked COMPLETED regardless. For production: fire-and-forget is correct here (pipeline completeness ≠ webhook delivery); a separate webhook retry service with its own queue and delivery tracking is the right pattern.

### Schema evolution
EventEnvelope has a `version: int` field. Currently all events are version 1. To evolve: increment version in the producer, write consumers that handle both version 1 and version 2. Never change the shape of version 1 events — add a new event type instead.

### Dead letter replay
Current: manual via RabbitMQ management UI. Add a `POST /admin/jobs/{id}/replay` endpoint that:
1. Finds the job and its stuck/failed task
2. Resets task to QUEUED
3. Emits fresh stage event via outbox

### Quorum queues
Replace `x-queue-type: classic` with `quorum` in topology.py for durability across RabbitMQ node failures. Requires RabbitMQ 3.8+.

### Database connection pooling
Add PgBouncer in transaction mode between app services and Postgres. Reduces Postgres connection count when scaling workers to 50+.

### Partial retry for TTS
If a job has 10 blocks and blocks 1-8 succeed before a failure, current retry re-processes all 10 (blocks 1-8 are cache hits so it's idempotent, just wasteful). Store `output_ref` as partial progress and resume from the first failed block.

### Priority queues
Paid/premium jobs could be routed to a separate high-priority queue with more worker replicas assigned. Add a `priority` field to jobs; route to `q.parse.high` vs `q.parse.normal`.

---

## 16. Interview Questions This Architecture Answers

### Q: How do you guarantee no messages are lost?

Three layers:
1. **Outbox pattern**: state change and event emission are one DB transaction. A crash between "update DB" and "publish to broker" is impossible.
2. **At-least-once delivery**: RabbitMQ redelivers unacked messages on worker crash. Manual ack happens only after DB commit.
3. **Janitor lease reaper**: covers the rare edge case where a worker acks before committing (post-ack crash). Janitor resets the task and re-emits the event.

### Q: How do you prevent duplicate processing?

Two-layer idempotency in every stage handler:
1. `processed_events` PK constraint — `INSERT ON CONFLICT DO NOTHING` → duplicate returns False → ack without processing.
2. State-machine guard — `UPDATE WHERE status=QUEUED RETURNING id` → if the task was already advanced by another worker, 0 rows returned → ack without processing.

Both are checked in the same DB transaction as the result write.

### Q: Why not use a workflow orchestrator like Temporal?

Hard constraint for this assessment: demonstrate mastery of distributed systems primitives. Orchestrators abstract away the coordination layer; here the coordination **is** the system. Key tradeoffs:
- Pro: finer control over retry semantics, backoff, semaphore behavior.
- Con: more code to write and maintain. For production systems with complex business logic, Temporal would be the right choice.

### Q: How does the TTS semaphore prevent exceeding 3 concurrent calls globally across all workers?

Redis ZSET-based leased semaphore with atomic Lua scripts. All workers share the same Redis key `tts:semaphore`. The Lua script atomically evicts expired holders, checks ZCARD, and either acquires or returns "full". Because Lua executes atomically on Redis's single-threaded engine, there's no race condition between check and add.

### Q: What happens if Redis crashes?

Workers making Lua calls get `ConnectionError` → `RetryableError` → task reset to QUEUED + message republished to delay queue. The ZSET is lost on restart, so all semaphore slots appear available. Workers that had already acquired and are mid-TTS continue running (they don't consult Redis during the vendor call). New arrivals all acquire freely until normal concurrency restores. For the brief window, TTS concurrency may temporarily exceed 3. Accepted behaviour for this use case.

For stronger guarantees: Redis persistence (AOF fsync) or Redlock (3 independent Redis nodes) at the cost of latency.

### Q: How do you handle a worker that crashes mid-TTS after acquiring the semaphore?

The semaphore uses leased slots — each slot has an expiry timestamp (score in the ZSET). The next `acquire()` call by any worker runs `ZREMRANGEBYSCORE` which evicts expired holders. The crashed worker's slot is automatically freed after `tts_lease_seconds`. No manual cleanup, no coordinator needed.

### Q: What's the difference between `FAILED` and `DEAD` task status?

`DEAD`: terminal state after permanent error or exhausted retries. No more recovery attempts. Job is marked FAILED.
`FAILED` (on task): intermediate state — currently unused in the state machine; the retry path resets to QUEUED, not FAILED. The distinction exists to allow future fine-grained recovery logic.

### Q: If you gave this system 100,000 jobs, what would break first?

The TTS semaphore limit is the primary throughput bottleneck. At `TTS_MAX_CONCURRENT=3` and 500ms per call: ~6 blocks/second. 500k blocks takes ~23 hours.

Fix: scale `TTS_MAX_CONCURRENT` proportionally with the number of TTS worker replicas. At 20 replicas × 3 concurrent = 60 TTS/second, 500k blocks takes ~2.3 hours.

Secondary bottleneck at very high scale: Postgres connection count. PgBouncer in transaction mode solves this.

### Q: How do you ensure that the parsing stage output feeds correctly into TTS?

The parse handler writes the `parsed.json` key as `tasks.output_ref` for the parse task AND as `tasks.input_ref` for the new TTS task created in the same result transaction. The TTS handler reads `task.input_ref` to find the parsed data. The relay dispatches the outbox event. No direct coupling between services — the handoff is through the DB task record.

### Q: What would you add to make this production-ready?

1. **Observability**: OpenTelemetry traces, Prometheus metrics, alerting on DLQ depth and relay lag.
2. **Quorum queues**: replace classic queues for RabbitMQ HA.
3. **PgBouncer**: connection pooling at scale.
4. **Webhook retry service**: separate bounded queue for delivery confirmation.
5. **Schema registry or versioning**: for EventEnvelope evolution with backward compatibility.
6. **Rate limiting on the API**: prevent one client from flooding 100k jobs at once.
7. **Job cancellation**: `DELETE /jobs/{id}` should drain in-flight tasks via outbox cancellation event.
