from __future__ import annotations

import hashlib
import json
import uuid

import aio_pika

from app.config import get_settings
from app.db.models import OutboxEvent, Task
from app.db.uow import unit_of_work
from app.domain.enums import ROUTING_KEY, TaskStage
from app.domain.errors import DuplicateEventError, SemaphoreFullError, StaleTransitionError
from app.domain.events import EventEnvelope, TtsCompletedData
from app.infra import storage
from app.infra.locks import tts_generation_lock
from app.infra.redis import get_redis
from app.infra.semaphore import TtsSemaphore
from app.logging import get_logger
from app.repositories.cache_repo import ProcessedEventRepository, TtsCacheRepository
from app.repositories.job_repo import JobRepository
from app.repositories.outbox_repo import OutboxRepository
from app.repositories.task_repo import TaskRepository
from app.vendors import tts as tts_vendor

log = get_logger(__name__)

WORKER_ID = f"worker-tts-{uuid.uuid4().hex[:8]}"
REDIS_CACHE_PREFIX = "tts:cache:"


async def handle_tts(envelope: EventEnvelope, channel: aio_pika.abc.AbstractChannel) -> None:
    settings = get_settings()
    job_id = envelope.job_id
    redis = get_redis()
    semaphore = TtsSemaphore(redis, settings.tts_max_concurrent, settings.tts_lease_seconds)

    # Step 1: dedup + claim
    task_id: str
    parsed_key: str
    async with unit_of_work() as session:
        event_repo = ProcessedEventRepository(session)
        is_new = await event_repo.record(envelope.event_id, TaskStage.TTS.value)
        if not is_new:
            raise DuplicateEventError()

        task_repo = TaskRepository(session)
        task = await task_repo.get_by_job_stage(job_id, TaskStage.TTS.value)
        if task is None:
            raise StaleTransitionError()

        claimed = await task_repo.claim(task.id, WORKER_ID, settings.tts_lease_seconds + 30)
        if not claimed:
            raise StaleTransitionError()

        task_id = task.id
        parsed_key = task.input_ref or envelope.data.get("parsed_key", "")

        await JobRepository(session).transition_status(job_id, "PARSING", "TTS")

    # Step 2: download parsed blocks
    log.info("tts_downloading", parsed_key=parsed_key)
    blocks_bytes = await storage.get_object(parsed_key)
    blocks: list[str] = json.loads(blocks_bytes)

    # Step 3: process each block with semaphore + cache
    audio_keys: list[str] = []
    for block in blocks:
        audio_key = await _process_block(block, job_id, semaphore, settings.tts_lease_seconds)
        audio_keys.append(audio_key)

    # Step 4: commit result + next-stage task + outbox
    async with unit_of_work() as session:
        task_repo = TaskRepository(session)
        await task_repo.complete(task_id, output_ref=json.dumps(audio_keys))

        stitch_task = Task(
            id=str(uuid.uuid4()),
            job_id=job_id,
            stage=TaskStage.STITCH.value,
            status="QUEUED",
            input_ref=json.dumps(audio_keys),
        )
        await task_repo.create(stitch_task)

        envelope_out = EventEnvelope.build(
            event_type="TtsCompleted",
            correlation_id=envelope.correlation_id,
            job_id=job_id,
            data=TtsCompletedData(audio_keys=audio_keys).model_dump(),
        )
        await OutboxRepository(session).add(
            OutboxEvent(
                aggregate_id=job_id,
                event_type="TtsCompleted",
                routing_key=ROUTING_KEY[TaskStage.STITCH],
                payload=envelope_out.model_dump(mode="json"),
            )
        )

    log.info("tts_completed", job_id=job_id, block_count=len(blocks))


async def _process_block(
    block: str, job_id: str, semaphore: TtsSemaphore, lease_seconds: int
) -> str:
    redis = get_redis()
    text_hash = hashlib.sha256(block.encode()).hexdigest()
    cache_key = f"{REDIS_CACHE_PREFIX}{text_hash}"

    # L1: Redis cache
    cached = await redis.get(cache_key)
    if cached:
        log.info("tts_cache_hit_redis", text_hash=text_hash[:16])
        return str(cached)

    # L2: DB cache
    async with unit_of_work() as session:
        db_cached = await TtsCacheRepository(session).get(text_hash)
    if db_cached:
        await redis.set(cache_key, db_cached.object_key, ex=3600)
        log.info("tts_cache_hit_db", text_hash=text_hash[:16])
        return db_cached.object_key

    # Cache miss — acquire semaphore + per-hash lock to prevent stampede
    sem_token = await semaphore.acquire()  # raises SemaphoreFullError if full
    try:
        async with tts_generation_lock(redis, text_hash) as lock_acquired:
            if not lock_acquired:
                # Another worker holds the hash lock — re-check cache
                double_check = await redis.get(cache_key)
                if double_check:
                    log.info("tts_cache_hit_after_lock", text_hash=text_hash[:16])
                    return str(double_check)

            audio_bytes = await tts_vendor.generate_audio(block)
            object_key = f"audio/{text_hash}.wav"
            await storage.put_object(object_key, audio_bytes, content_type="audio/wav")

            async with unit_of_work() as session:
                await TtsCacheRepository(session).set(text_hash, object_key)

            await redis.set(cache_key, object_key, ex=3600)
            log.info("tts_generated", text_hash=text_hash[:16], object_key=object_key)
            return object_key
    finally:
        await semaphore.release(sem_token)
