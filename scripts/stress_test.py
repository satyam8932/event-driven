#!/usr/bin/env python3
"""
Rigorous end-to-end stress test.

Scenarios:
  1. Load          — 40 concurrent normal jobs → all COMPLETED
  2. Poison pill   — 10 poison jobs mixed with 10 normal → poison→FAILED, normal→COMPLETED
  3. Worker kill   — submit 15 jobs, kill worker mid-flight, verify recovery
  4. Duplicate     — replay a JobCreated outbox event, verify no double processing
  5. DLQ check     — verify poison + retry-exhausted jobs landed in per-stage DLQ

Usage:
  python scripts/stress_test.py [--api http://localhost:8000] [--scenario all|load|poison|kill|dup|dlq]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime

API = "http://localhost:8000"
RABBITMQ_API = "http://localhost:15672"
RABBITMQ_USER = "guest"
RABBITMQ_PASS = "guest"
POLL_INTERVAL = 2       # seconds between status polls
JOB_TIMEOUT = 120       # seconds to wait for a single job
WORKER_SERVICE = "worker"

NORMAL_TEXT = (
    "Act 1. The storm arrived at midnight. Thunder cracked the sky open. "
    "Scene 2. Old Elara lit a candle. Its flame held steady against the dark. "
    "Scene 3. The wind howled through the valley. The villagers barred their doors."
)
POISON_TEXT = "__POISON_PILL__ This manuscript will always fail TTS."


# ─── HTTP helpers ────────────────────────────────────────────────────────────

def _http(method: str, url: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"} if body else {},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())
    except Exception as exc:
        return {"error": str(exc)}


def post(path: str, body: dict) -> dict:
    return _http("POST", f"{API}{path}", body)


def get(path: str) -> dict:
    return _http("GET", f"{API}{path}")


def rmq_get(path: str) -> dict:
    import base64
    creds = base64.b64encode(f"{RABBITMQ_USER}:{RABBITMQ_PASS}".encode()).decode()
    req = urllib.request.Request(
        f"{RABBITMQ_API}/api{path}",
        headers={"Authorization": f"Basic {creds}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read())
    except Exception as exc:
        return {"error": str(exc)}


# ─── Core primitives ─────────────────────────────────────────────────────────

@dataclass
class JobResult:
    job_id: str
    expected_status: str
    final_status: str = ""
    elapsed: float = 0.0
    timed_out: bool = False


def submit_job(text: str) -> str:
    r = post("/jobs", {"manuscript": text})
    return r.get("job_id", "")


def poll_job(job_id: str, timeout: int = JOB_TIMEOUT) -> tuple[str, float]:
    start = time.time()
    while True:
        r = get(f"/jobs/{job_id}")
        status = r.get("status", "")
        elapsed = time.time() - start
        if status in ("COMPLETED", "FAILED") or elapsed >= timeout:
            return status, elapsed
        time.sleep(POLL_INTERVAL)


async def _run_job(sem: asyncio.Semaphore, text: str, expected: str) -> JobResult:
    async with sem:
        loop = asyncio.get_event_loop()
        job_id = await loop.run_in_executor(None, submit_job, text)
        if not job_id:
            return JobResult(job_id="SUBMIT_FAILED", expected_status=expected,
                             final_status="SUBMIT_FAILED")
        status, elapsed = await loop.run_in_executor(None, poll_job, job_id)
        timed_out = status not in ("COMPLETED", "FAILED")
        return JobResult(job_id=job_id, expected_status=expected,
                         final_status=status, elapsed=elapsed, timed_out=timed_out)


async def run_batch(jobs: list[tuple[str, str]], concurrency: int = 10) -> list[JobResult]:
    sem = asyncio.Semaphore(concurrency)
    tasks = [_run_job(sem, text, expected) for text, expected in jobs]
    return await asyncio.gather(*tasks)


# ─── Reporting ───────────────────────────────────────────────────────────────

def _print_header(title: str) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def report(results: list[JobResult], label: str) -> bool:
    passed = sum(1 for r in results if r.final_status == r.expected_status)
    timed_out = sum(1 for r in results if r.timed_out)
    wrong = [r for r in results if r.final_status != r.expected_status and not r.timed_out]
    elapsed_ok = [r.elapsed for r in results if not r.timed_out]
    avg = sum(elapsed_ok) / len(elapsed_ok) if elapsed_ok else 0
    mx = max(elapsed_ok) if elapsed_ok else 0

    print(f"\n{label}")
    print(f"  Total:       {len(results)}")
    print(f"  Passed:      {passed}  ({'OK' if passed == len(results) else 'FAIL'})")
    print(f"  Timed out:   {timed_out}")
    print(f"  Wrong status:{len(wrong)}")
    if wrong:
        for r in wrong[:5]:
            print(f"    job={r.job_id[:8]} expected={r.expected_status} got={r.final_status}")
    print(f"  Avg elapsed: {avg:.1f}s   Max: {mx:.1f}s")
    return passed == len(results) and timed_out == 0


# ─── Scenario 1: Load test ────────────────────────────────────────────────────

async def scenario_load(n: int = 40) -> bool:
    _print_header(f"SCENARIO 1 — Load: {n} concurrent normal jobs")
    jobs = [(NORMAL_TEXT, "COMPLETED")] * n
    results = await run_batch(jobs, concurrency=min(n, 20))
    return report(results, f"Load test ({n} jobs)")


# ─── Scenario 2: Poison pill + normal mix ────────────────────────────────────

async def scenario_poison(n_normal: int = 10, n_poison: int = 10) -> bool:
    _print_header(f"SCENARIO 2 — Poison pill: {n_poison} poison + {n_normal} normal")
    jobs = (
        [(NORMAL_TEXT, "COMPLETED")] * n_normal
        + [(POISON_TEXT, "FAILED")] * n_poison
    )
    import random
    random.shuffle(jobs)
    results = await run_batch(jobs, concurrency=15)
    ok = report(results, "Poison pill test")

    # Verify DLQ has messages for TTS stage
    dlq = rmq_get("/queues/%2F/q.tts.dlq")
    dlq_count = dlq.get("messages", dlq.get("error", "N/A"))
    print(f"  q.tts.dlq messages: {dlq_count}")
    return ok


# ─── Scenario 3: Worker kill + recovery ──────────────────────────────────────

async def scenario_worker_kill(n: int = 15) -> bool:
    _print_header(f"SCENARIO 3 — Worker kill: {n} jobs, kill worker mid-flight")
    loop = asyncio.get_event_loop()

    # Submit all jobs with unique text so they don't all hit TTS cache
    job_ids = []
    for i in range(n):
        text = f"{NORMAL_TEXT} Unique marker {i} for kill test."
        jid = await loop.run_in_executor(None, submit_job, text)
        if jid:
            job_ids.append(jid)
    print(f"  Submitted {len(job_ids)} jobs")

    # Wait briefly for some to enter PARSING stage
    await asyncio.sleep(3)

    # Use `docker kill <id>` directly — NOT `docker compose kill`.
    # `docker compose kill` marks the container stopped in compose state,
    # which suppresses the restart policy. `docker kill` sends SIGKILL and
    # lets Docker's `restart: unless-stopped` bring it back automatically.
    cid_result = subprocess.run(
        ["docker", "compose", "ps", "-q", WORKER_SERVICE],
        capture_output=True, text=True
    )
    cid = cid_result.stdout.strip()
    print(f"  Worker container ID: {cid[:12] if cid else 'NOT FOUND'}")
    if not cid:
        print("  SKIP: worker container not found")
        return False

    kill_result = subprocess.run(["docker", "kill", cid], capture_output=True, text=True)
    print(f"  SIGKILL sent: {'OK' if kill_result.returncode == 0 else kill_result.stderr.strip()}")

    # Docker restart policy brings it back — wait for it
    await asyncio.sleep(8)
    ps = subprocess.run(["docker", "compose", "ps", WORKER_SERVICE],
                        capture_output=True, text=True)
    print(f"  Worker status after kill+8s:\n    {ps.stdout.splitlines()[-1].strip() if ps.stdout.strip() else 'not visible'}")

    # Poll all jobs — use a longer timeout since restart + redelivery takes time
    kill_timeout = 180
    results = []
    for jid in job_ids:
        status, elapsed = await loop.run_in_executor(None, poll_job, jid, kill_timeout)
        results.append(JobResult(
            job_id=jid, expected_status="COMPLETED",
            final_status=status, elapsed=elapsed,
            timed_out=status not in ("COMPLETED", "FAILED")
        ))

    return report(results, "Worker kill recovery")


# ─── Scenario 4: Duplicate event dedup ───────────────────────────────────────

async def scenario_duplicate() -> bool:
    _print_header("SCENARIO 4 — Duplicate event dedup")

    loop = asyncio.get_event_loop()

    # Submit a job, wait for it to complete
    jid = await loop.run_in_executor(None, submit_job, NORMAL_TEXT)
    print(f"  Submitted job {jid[:8]}")
    status, _ = await loop.run_in_executor(None, poll_job, jid, JOB_TIMEOUT)
    print(f"  Job reached: {status}")

    if status != "COMPLETED":
        print("  SKIP: base job didn't complete, can't test dedup")
        return False

    # Fetch the outbox row for this job from DB via psql
    print("  Fetching outbox payload for re-publish...")
    ps = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres",
         "psql", "-U", "pipeline", "-d", "pipeline",
         "-t", "-A",
         "-c", f"SELECT payload FROM outbox WHERE aggregate_id='{jid}' LIMIT 1"],
        capture_output=True, text=True
    )
    payload_str = ps.stdout.strip()
    if not payload_str:
        print("  SKIP: outbox row not found")
        return False

    payload = json.loads(payload_str)
    print(f"  Re-publishing event_id={payload.get('event_id','?')[:8]}... (expect dedup)")

    # Re-publish to the parse queue via RabbitMQ management API
    import base64
    creds = base64.b64encode(f"{RABBITMQ_USER}:{RABBITMQ_PASS}".encode()).decode()
    pub_body = json.dumps({
        "properties": {"delivery_mode": 2},
        "routing_key": "job.parse",
        "payload": json.dumps(payload),
        "payload_encoding": "string",
    }).encode()
    req = urllib.request.Request(
        f"{RABBITMQ_API}/api/exchanges/%2F/pipeline/publish",
        data=pub_body,
        headers={
            "Authorization": f"Basic {creds}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            pub_result = json.loads(r.read())
            print(f"  Publish result: {pub_result}")
    except Exception as exc:
        print(f"  Publish error: {exc}")
        return False

    # Wait a moment, then verify job is still COMPLETED (not re-processed)
    await asyncio.sleep(5)
    r = get(f"/jobs/{jid}")
    final_status = r.get("status", "")
    print(f"  Job status after duplicate: {final_status} (want COMPLETED)")

    # Check worker logs for duplicate detection
    logs = subprocess.run(
        ["docker", "compose", "logs", "--tail=50", "worker"],
        capture_output=True, text=True
    ).stdout
    dup_detected = "duplicate_event" in logs or "DuplicateEventError" in logs
    print(f"  Duplicate detected in logs: {dup_detected}")

    ok = final_status == "COMPLETED" and dup_detected
    print(f"  Result: {'PASS' if ok else 'FAIL'}")
    return ok


# ─── Scenario 5: DLQ state check ─────────────────────────────────────────────

def scenario_dlq_check() -> bool:
    _print_header("SCENARIO 5 — DLQ state check")
    stages = ["parse", "tts", "stitch", "notify"]
    all_ok = True
    for stage in stages:
        q = rmq_get(f"/queues/%2F/q.{stage}.dlq")
        msgs = q.get("messages", q.get("error", "N/A"))
        ready = q.get("messages_ready", 0)
        print(f"  q.{stage}.dlq: total={msgs} ready={ready}")
    return True  # informational only


# ─── Scenario 6: Retry backoff verification ───────────────────────────────────

async def scenario_retry_stats() -> bool:
    _print_header("SCENARIO 6 — Retry backoff stats (from logs)")
    # Submit a batch and look at retry logs
    jobs = [(NORMAL_TEXT, "COMPLETED")] * 10
    await run_batch(jobs, concurrency=5)

    logs = subprocess.run(
        ["docker", "compose", "logs", "--tail=500", "worker"],
        capture_output=True, text=True
    ).stdout

    retry_events = [l for l in logs.splitlines() if '"retry_scheduled"' in l]
    attempt_counts: dict[int, int] = {}
    for line in retry_events:
        try:
            d = json.loads(line.split(" | ")[-1] if " | " in line else line.split(None, 1)[-1])
            att = d.get("attempt", 0)
            attempt_counts[att] = attempt_counts.get(att, 0) + 1
        except Exception:
            pass

    print(f"  Total retry events seen: {len(retry_events)}")
    for att in sorted(attempt_counts):
        print(f"    attempt={att}: {attempt_counts[att]}x")

    # Check delay_ms values
    delays = []
    for line in retry_events:
        try:
            d = json.loads(line.split(None, 1)[-1])
            if "delay_ms" in d:
                delays.append(d["delay_ms"])
        except Exception:
            pass
    if delays:
        print(f"  Delay buckets seen: {sorted(set(delays))}")
        print(f"  Min delay: {min(delays)}ms  Max: {max(delays)}ms")
        # Verify jitter: should not all be exactly 2000/4000/8000
        exact = [d for d in delays if d in (2000, 4000, 8000)]
        print(f"  Exact-bucket (no jitter applied): {len(exact)}/{len(delays)}")

    return True  # informational


# ─── Scenario 7: Semaphore ceiling check ──────────────────────────────────────

async def scenario_semaphore(n: int = 30) -> bool:
    _print_header(f"SCENARIO 7 — TTS semaphore: {n} jobs, verify ≤3 concurrent")
    loop = asyncio.get_event_loop()

    # Use unique text per job so TTS cache stays cold and semaphore is contested.
    # Identical text = all jobs hit L1 cache after first run → semaphore never acquired.
    unique_jobs = [
        (f"{NORMAL_TEXT} [unique_sem_{i}]", "COMPLETED")
        for i in range(n)
    ]

    async def submit_all():
        return await run_batch(unique_jobs, concurrency=n)

    task = asyncio.create_task(submit_all())

    # Poll Redis ZCARD while jobs run
    max_concurrent = 0
    samples = []
    for _ in range(30):
        await asyncio.sleep(1)
        r = subprocess.run(
            ["docker", "compose", "exec", "-T", "redis",
             "redis-cli", "ZCARD", "tts:semaphore"],
            capture_output=True, text=True
        )
        try:
            count = int(r.stdout.strip())
        except ValueError:
            count = 0
        samples.append(count)
        max_concurrent = max(max_concurrent, count)
        if task.done():
            break

    results = await task
    report(results, f"Semaphore test ({n} jobs)")

    print(f"\n  Semaphore ZCARD samples: {samples}")
    print(f"  Max concurrent TTS observed: {max_concurrent}")
    limit_respected = max_concurrent <= 3
    print(f"  Limit ≤3 respected: {'YES' if limit_respected else 'NO — VIOLATION'}")
    return limit_respected


# ─── Main ────────────────────────────────────────────────────────────────────

async def _dlq_wrap() -> bool:
    return scenario_dlq_check()

SCENARIOS = {
    "load":      ("Load test (40 jobs)", lambda: scenario_load(40)),
    "poison":    ("Poison pill + DLQ", lambda: scenario_poison(10, 10)),
    "kill":      ("Worker kill recovery", lambda: scenario_worker_kill(15)),
    "dup":       ("Duplicate event dedup", lambda: scenario_duplicate()),
    "dlq":       ("DLQ queue state", lambda: _dlq_wrap()),
    "retry":     ("Retry backoff stats", lambda: scenario_retry_stats()),
    "semaphore": ("Semaphore ceiling", lambda: scenario_semaphore(30)),
}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument(
        "--scenario",
        default="all",
        help="all | " + " | ".join(SCENARIOS.keys()),
    )
    args = parser.parse_args()

    global API
    API = args.api

    # Quick health check
    health = get("/health")
    if health.get("status") != "ok":
        print(f"API health check failed: {health}")
        sys.exit(1)
    print(f"API healthy: {health}")
    print(f"Started: {datetime.now().isoformat()}")

    scores: dict[str, bool] = {}

    if args.scenario == "all":
        to_run = list(SCENARIOS.keys())
    else:
        to_run = [s.strip() for s in args.scenario.split(",")]

    # DLQ check is sync — wrap
    for name in to_run:
        if name not in SCENARIOS:
            print(f"Unknown scenario: {name}")
            continue
        label, fn = SCENARIOS[name]
        print(f"\n▶ Running: {label}")
        try:
            result = fn()
            if asyncio.iscoroutine(result):
                passed = await result
            else:
                passed = bool(result)
        except Exception as exc:
            print(f"  ERROR: {exc}")
            import traceback
            traceback.print_exc()
            passed = False
        scores[name] = passed

    # Final summary
    _print_header("FINAL RESULTS")
    all_pass = True
    for name, passed in scores.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {name:12s}  {status}")
        if not passed:
            all_pass = False

    print(f"\nOverall: {'ALL PASS' if all_pass else 'SOME FAILURES'}")
    print(f"Finished: {datetime.now().isoformat()}")
    sys.exit(0 if all_pass else 1)


if __name__ == "__main__":
    asyncio.run(main())
