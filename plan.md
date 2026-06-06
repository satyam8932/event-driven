# Distributed Multi-Modal GenAI Pipeline — Implementation Plan

## Context

We are building the core asynchronous engine for a Multi-Modal Generation Platform: users upload large text manuscripts, the system outputs a produced "audio drama." This is an assessment of **architecture, state management across boundaries, edge-case handling, and reliability** — not boilerplate.

Hard constraint: **no managed workflow orchestrators** (Temporal / Airflow / Step Functions / Celery). The pipeline is choreographed using core infrastructure primitives only. Vendor calls (LLM parse, TTS) are simulated with `asyncio.sleep` + randomized failure injection.

The working directory `/Volumes/Work/event-driven` is empty — this is a from-scratch build.

### Locked decisions (from clarifying questions)
- **Fully async** stack: `aio-pika`, `asyncpg` + SQLAlchemy 2.0 async, `redis.asyncio`, `aioboto3`.
- **Queue-per-stage** choreography (isolated retries + DLQ + scaling per stage).
- **Dedicated outbox relay** service (no dual-write; survives broker downtime).
- Extras in scope: **tests** (pytest unit + integration) and **formatting/CI** (ruff + black + mypy + pre-commit + GitHub Actions). Heavy observability stack and admin UIs are out of scope (RabbitMQ/MinIO ship their own consoles for free).

---

## Target Architecture

### Services (docker-compose)
| Service | Role | Scale |
|---|---|---|
| `postgres` | State DB (jobs, tasks, outbox, processed_events, tts_cache) | 1 |
| `rabbitmq` | Broker (management plugin enabled) | 1 |
| `redis` | Distributed semaphore, idempotency locks, TTS cache | 1 |
| `minio` + `minio-init` | Object storage; init creates bucket | 1 + one-shot |
| `migrate` | One-shot Alembic migration runner (advisory-locked) | one-shot |
| `api` | FastAPI gateway (ingest + status) | 1+ |
| `worker` | Stage consumers (parse / tts / stitch / notify) | **N (the interesting one to `--scale`)** |
| `relay` | Outbox dispatcher (DB → broker) | 1–2 |
| `janitor` | Lease reaper / crash-recovery sweeper / outbox prune | 1 |

`depends_on` uses `condition: service_healthy`; every infra service gets a healthcheck. App services start only after `migrate` completes.

### Pipeline flow (choreography)
```
POST /jobs ─► [api] ingest: PUT manuscript→MinIO, INSERT job(PENDING) + outbox(JobCreated)  (1 tx)
                                   │
                            [relay] polls outbox ─► publish ─► exchange `pipeline`
                                   ▼
  job.parse ─► [worker:parse]  download txt, simulated LLM (15% 500), split into blocks,
                               PUT parsed.json, status→PARSED + outbox(ParseCompleted)
                                   ▼
  job.tts   ─► [worker:tts]    per block: Redis counting-semaphore (max 3 global) +
                               content-hash cache check; simulate TTS; PUT audio/{hash}.wav,
                               status→TTS_DONE + outbox(TtsCompleted)
                                   ▼
  job.stitch─► [worker:stitch] "combine" block audio, PUT final/{job}.wav,
                               status→STITCHED + outbox(StitchCompleted)
                                   ▼
  job.notify─► [worker:notify] fire webhook / log NOTIFIED, job status→COMPLETED
```
Every worker stage is: **consume → do I/O → commit (state + next-stage outbox row) in one short tx → ack**. Workers never publish to the broker directly — they write outbox rows; the relay publishes. This keeps "change state" and "emit event" atomic.

---

## Broker Topology (RabbitMQ)

- Durable **topic exchange** `pipeline`. Routing keys: `job.parse`, `job.tts`, `job.stitch`, `job.notify`.
- Durable work queues `q.parse`, `q.tts`, `q.stitch`, `q.notify`, each bound to its routing key. All messages persistent (`delivery_mode=2`).
- **Per-stage terminal DLQ**: `q.<stage>.dlq` bound to `dlx.<stage>`.
- **Retry via delay queues** (no plugin needed): on a retryable failure the consumer republishes the message to a delay queue `q.delay.<ttl>` with `x-message-ttl` set and a DLX pointing back at `pipeline` with the original routing key. After TTL expires, the message dead-letters back to the work queue. Backoff TTLs `2s → 4s → 8s` **plus jitter**; attempt count carried in a custom header `x-attempt`. After 3 attempts → publish to terminal `q.<stage>.dlq`, then ack the original.
  - Alternative noted: `rabbitmq-delayed-message-exchange` plugin (cleaner, but a community plugin — we stay on core primitives).
- Consumers use **manual ack** and a small **prefetch (QoS)** (e.g. 8) for fair dispatch and backpressure.

---

## Data Model (Postgres, SQLAlchemy 2.0 async)

- `jobs(id uuid pk, status, manuscript_key, final_key, correlation_id, created_at, updated_at)`
- `tasks(id uuid pk, job_id fk, stage enum, status enum, attempts int, locked_by, lock_expires_at, input_ref, output_ref, error, created_at, updated_at)` — one row per (job, stage).
- `outbox(id bigserial pk, aggregate_id, event_type, routing_key, payload jsonb, occurred_at, published_at null, attempts)` — **partial index `WHERE published_at IS NULL`**.
- `processed_events(event_id uuid pk, stage, processed_at)` — consumer-side dedup ledger.
- `tts_cache(text_hash char(64) pk, object_key, created_at)` — durable mirror of the Redis cache.

### State machines (transitions guarded by `UPDATE ... WHERE status = <expected> RETURNING` — 0 rows ⇒ already handled, skip & ack)
- **Job**: `PENDING → PARSING → TTS → STITCHING → NOTIFYING → COMPLETED`; `FAILED` terminal from any.
- **Task**: `QUEUED → PROCESSING → DONE | FAILED | DEAD`.

---

## Core Reliability Mechanisms

### 1. Idempotent consumers (effectively-once on at-least-once delivery)
Two layers, both required:
1. **Dedup ledger**: `INSERT INTO processed_events(event_id) ON CONFLICT DO NOTHING`. Conflict ⇒ duplicate ⇒ ack and return.
2. **State-machine guard**: stage work only runs via `UPDATE tasks SET status='PROCESSING' WHERE id=? AND status='QUEUED' RETURNING`. No row ⇒ another worker already advanced it.
   Both live in the **same transaction** as the result commit. S3 writes are **content-addressed** (`audio/{hash}.wav`) so re-running overwrites identically — no duplicate artifacts.

### 2. Outbox pattern (no dual-write)
State change + event are one DB tx. The **relay** loop: `SELECT ... FROM outbox WHERE published_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED LIMIT N` → publish with **publisher confirms** → `UPDATE published_at=now()`. `SKIP LOCKED` lets relays scale horizontally. Publisher confirms are mandatory — without them a relay could mark "published" on a message the broker dropped. Janitor prunes old published rows.

### 3. DLQ + exponential backoff (poison pill)
Transient errors (simulated 500, semaphore-busy) → retry via delay queues. After **3 attempts** → terminal `q.<stage>.dlq`, task→`DEAD`, job→`FAILED`. A **poison manuscript** (sentinel marker in text) deterministically fails TTS and lands in the DLQ **without blocking the queue**, because retries go through delay queues, not by holding/redelivering on the live consumer. Error taxonomy distinguishes *retryable* vs *permanent* (permanent can short-circuit straight to DLQ). A small `requeue-from-dlq` script/endpoint allows replay after a fix.

### 4. Crash recovery (`docker kill` mid-processing)
- **Primary**: manual ack happens only *after* commit. A killed worker drops its TCP connection → RabbitMQ **automatically redelivers** all its unacked messages to another consumer. Idempotency guards make redelivery safe.
- **Belt-and-suspenders**: tasks carry a **lease** (`locked_by`, `lock_expires_at`). The **janitor** periodically finds `status='PROCESSING' AND lock_expires_at < now()`, resets them to `QUEUED`, and re-emits the stage event via outbox — covering the rare "acked then crashed" or stuck-row case. Lease time comes from the **DB server clock** (`now()`), never the worker's, to avoid skew.
- Graceful path: `SIGTERM` → stop consuming, drain in-flight, close connections (docker `stop_grace_period`). `SIGKILL` falls back to redelivery + lease.

### 5. TTS concurrency — global limit of 3 (distributed counting semaphore)
Redis **ZSET-based leased semaphore**, all ops in one **Lua script** (atomic):
- *Acquire*: `ZREMRANGEBYSCORE` to evict expired holders → `ZCARD < 3`? → `ZADD token now+lease` and return token, else fail.
- *Renew*: update score while working (for longer tasks).
- *Release*: `ZREM` in a `finally`. Crashed holder ⇒ lease expiry frees the slot automatically.
- **Gotcha — don't block the consumer**: if acquire fails, **republish the message to a short delay queue (~2s) and ack** instead of busy-waiting. Blocking would deadlock prefetched messages all waiting on a full semaphore. This makes the limit a smooth global throttle.

### 6. TTS idempotency / cost cache (Constraint B)
- Key = `sha256(text_block)`. Check Redis `tts:cache:{hash}` → durable `tts_cache` table → on miss call the simulated vendor.
- **Cache stampede**: two identical blocks in flight both miss. Guard generation with a per-hash Redis lock (`SET NX PX`); the waiter re-checks the cache after acquiring. Combined with the global semaphore this guarantees the "vendor" is hit at most once per unique block. A vendor-call counter (log/metric) proves cache hits during verification.

---

## File Structure (SOLID, production-grade)

```
event-driven/
├── docker-compose.yml            # all infra + app services, healthchecks, depends_on
├── docker-compose.override.yml   # dev hot-reload, exposed ports, console UIs
├── Dockerfile                    # single multi-stage image; entrypoint selects role
├── pyproject.toml / uv.lock      # uv for fast, reproducible deps
├── Makefile                      # up/down/migrate/test/lint/fmt/seed/scale
├── .env.example  .dockerignore  ruff.toml  alembic.ini
├── .pre-commit-config.yaml       # ruff + black + mypy
├── .github/workflows/ci.yml      # lint + type + unit + integration
├── migrations/versions/          # Alembic
├── scripts/                      # init-minio, wait-for, seed-job, requeue-dlq, kill-demo
├── src/app/
│   ├── config.py                 # pydantic-settings (12-factor env)
│   ├── logging.py                # structlog JSON + correlation_id
│   ├── domain/                   # PURE: enums, event schemas, errors — no I/O (SRP)
│   │   ├── enums.py  events.py  errors.py
│   ├── db/                       # engine, async session factory, ORM models, UoW
│   ├── repositories/             # Protocol interfaces + SQLAlchemy impls (DIP/ISP)
│   │   ├── interfaces.py  job_repo.py  task_repo.py  outbox_repo.py  cache_repo.py
│   ├── infra/                    # adapters behind interfaces (DIP)
│   │   ├── broker.py             # aio-pika robust connection, topology declare, confirms
│   │   ├── storage.py            # MinIO/S3: put/get/presign (internal vs public endpoint)
│   │   ├── redis.py
│   │   ├── semaphore.py          # Lua leased counting semaphore
│   │   └── locks.py              # per-hash idempotency lock
│   ├── messaging/
│   │   ├── topology.py           # exchanges/queues/bindings/DLX/delay queues
│   │   ├── retry.py              # backoff TTL + jitter + attempt header + DLQ routing
│   │   └── consumer.py           # generic stage consumer (OCP via handler registry)
│   ├── vendors/                  # simulated externals
│   │   ├── llm.py                # parse, 15% injected 500
│   │   └── tts.py                # sleep + poison-pill detection
│   ├── services/                 # use-cases, one responsibility each (SRP)
│   │   ├── ingestion.py parsing.py tts.py stitch.py notify.py
│   ├── workers/                  # __main__ boots stage consumers (env selects stages)
│   ├── relay/__main__.py         # outbox dispatcher
│   ├── janitor/__main__.py       # lease reaper + outbox prune
│   └── api/                      # FastAPI: main.py (lifespan), routes/{jobs,health}.py, schemas.py
└── tests/{unit,integration,conftest.py}
```
SOLID mapping: repositories/adapters behind `Protocol`s (DIP); generic consumer extended by registering stage handlers, not edited (OCP); pure `domain/` with zero I/O (SRP); segregated repo interfaces (ISP).

---

## Edge Cases & Gotchas (and how each is solved)

| # | Edge case / gotcha | Mitigation |
|---|---|---|
| 1 | Dual-write (DB ok, broker publish fails) | Outbox pattern + relay |
| 2 | Broker down at publish time | Outbox accumulates; relay drains on recovery; publisher confirms |
| 3 | Duplicate delivery | `processed_events` PK + `UPDATE ... WHERE status` guard, same tx |
| 4 | Out-of-order / replayed events | State-machine rejects invalid transitions (no-op + ack) |
| 5 | Poison pill blocks queue | Retries via **delay queues**, not live redelivery; DLQ after 3 |
| 6 | Retry thundering herd | Exponential backoff **+ jitter** on delay TTL |
| 7 | RabbitMQ has no native delayed retry | TTL delay queue + DLX back to work exchange |
| 8 | Worker `docker kill` mid-task | Manual-ack-after-commit ⇒ auto redelivery; + janitor lease reaper |
| 9 | Ack-before-done ⇒ message loss | Strict order: work → commit → **then** ack |
| 10 | Semaphore holder crash leaks a slot | ZSET lease expiry auto-frees |
| 11 | Semaphore acquire/check race | Single atomic Lua script |
| 12 | Semaphore full ⇒ consumer deadlock | Republish to short delay queue + ack (never block prefetch) |
| 13 | Cache stampede (identical blocks) | Per-hash `SET NX` lock + double-check |
| 14 | Duplicate artifacts on re-run | Content-addressed S3 keys (`audio/{hash}.wav`) |
| 15 | MinIO presigned-URL host mismatch in Docker | Separate internal vs **public** S3 endpoint for presign |
| 16 | Bucket missing on first boot | `minio-init` one-shot (`mc mb`) gated by healthcheck |
| 17 | Long DB locks during vendor I/O | Claim in short tx → do I/O **outside** tx → commit result in short tx |
| 18 | Concurrent Alembic migrations | One-shot `migrate` service holding a PG advisory lock |
| 19 | Lease clock skew across workers | Use DB `now()` / Redis `TIME`, never worker wall clock |
| 20 | Outbox / processed_events table growth | Partial index + janitor prune of old rows |
| 21 | Notification reliability | `NotifyRequested` event is outbox-driven with retry/DLQ — not fire-and-forget |
| 22 | Connection drops | aio-pika robust auto-reconnect; channels re-declared on reopen |
| 23 | Multiple relays double-publishing | `FOR UPDATE SKIP LOCKED` |
| 24 | Exactly-once illusion | Documented as effectively-once = at-least-once delivery + idempotent handlers |
| 25 | Redis single-node lock SPOF | Acceptable for assessment; Redlock noted for multi-node |
| 26 | Event schema evolution | Events carry `event_id`, `occurred_at`, `version`, `correlation_id` |
| 27 | Graceful vs forced shutdown | SIGTERM drains; SIGKILL falls back to redelivery + lease |

---

## Verification Plan (end-to-end)

1. `make up` → infra healthy, `migrate` runs, app services start.
2. **Happy path**: `POST /jobs` → poll `GET /jobs/{id}` → reaches `COMPLETED`; final asset in MinIO.
3. **Idempotency cache**: submit the same manuscript twice → second run logs `tts cache hit`, vendor-call counter does **not** increment.
4. **Global concurrency**: submit ~10 jobs → assert Redis `ZCARD` of the semaphore key never exceeds **3** (log sampling + integration assertion).
5. **DLQ / poison pill**: submit the poison manuscript → 3 attempts with growing backoff → lands in `q.tts.dlq`, job `FAILED`; **other jobs keep completing** concurrently.
6. **Crash recovery**: `docker kill` a worker mid-TTS (`scripts/kill-demo.sh`) → another worker resumes via redelivery/lease → job still `COMPLETED`.
7. **Broker-down durability**: `docker compose stop rabbitmq`, submit jobs (API returns 202, outbox grows) → `start rabbitmq` → relay drains, jobs proceed.
8. **Duplicate delivery**: manually re-publish a `JobCreated` (broker UI) → no double processing (ledger + state guard).
9. **Automated**: `make test` → unit (hash idempotency, backoff math, semaphore Lua, state transitions) + integration (testcontainers/compose: crash recovery, DLQ routing, cache) → CI runs the same on GitHub Actions.

---

## Implementation Order (build sequence)
1. Scaffold: `pyproject` (uv), config, logging, Dockerfile, compose with all infra + healthchecks, Makefile, lint/CI.
2. DB layer: ORM models, Alembic migration, repositories + interfaces, UoW.
3. Infra adapters: broker (topology + confirms), storage, redis, semaphore, locks.
4. Outbox relay + messaging core (generic consumer, retry/DLQ, delay queues).
5. API gateway: ingest (outbox) + status + health.
6. Vendors (simulated) + services (parse → tts → stitch → notify) wired as stage handlers.
7. Janitor (lease reaper + prune).
8. Tests (unit + integration) and demo scripts.
9. README: architecture diagram, run instructions, failure-injection knobs, env reference.
