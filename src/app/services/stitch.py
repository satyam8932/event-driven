from __future__ import annotations

import asyncio
import json
import uuid

import aio_pika

from app.db.models import OutboxEvent, Task
from app.db.uow import unit_of_work
from app.domain.enums import ROUTING_KEY, TaskStage
from app.domain.errors import DuplicateEventError, StaleTransitionError
from app.domain.events import EventEnvelope, StitchCompletedData
from app.infra import storage
from app.logging import get_logger
from app.repositories.cache_repo import ProcessedEventRepository
from app.repositories.job_repo import JobRepository
from app.repositories.outbox_repo import OutboxRepository
from app.repositories.task_repo import TaskRepository

log = get_logger(__name__)

WORKER_ID = f"worker-stitch-{uuid.uuid4().hex[:8]}"
LEASE_SECONDS = 60


async def handle_stitch(envelope: EventEnvelope, channel: aio_pika.abc.AbstractChannel) -> None:
    job_id = envelope.job_id

    # Step 1: dedup + claim
    async with unit_of_work() as session:
        event_repo = ProcessedEventRepository(session)
        if not await event_repo.record(envelope.event_id, TaskStage.STITCH.value):
            raise DuplicateEventError()

        task_repo = TaskRepository(session)
        task = await task_repo.get_by_job_stage(job_id, TaskStage.STITCH.value)
        if task is None:
            raise StaleTransitionError()

        if not await task_repo.claim(task.id, WORKER_ID, LEASE_SECONDS):
            raise StaleTransitionError()

        task_id = task.id
        audio_keys: list[str] = json.loads(task.input_ref or "[]")
        await JobRepository(session).transition_status(job_id, "TTS", "STITCHING")

    # Step 2: download + "stitch" (concatenate fake wav bytes)
    log.info("stitch_combining", block_count=len(audio_keys))
    await asyncio.sleep(0.5)  # simulate stitching work
    chunks = []
    for key in audio_keys:
        chunks.append(await storage.get_object(key))

    combined = b"".join(chunks)
    final_key = f"final/{job_id}/output.wav"
    await storage.put_object(final_key, combined, content_type="audio/wav")

    # Step 3: commit
    async with unit_of_work() as session:
        task_repo = TaskRepository(session)
        await task_repo.complete(task_id, output_ref=final_key)

        notify_task = Task(
            id=str(uuid.uuid4()),
            job_id=job_id,
            stage=TaskStage.NOTIFY.value,
            status="QUEUED",
            input_ref=final_key,
        )
        await task_repo.create(notify_task)

        envelope_out = EventEnvelope.build(
            event_type="StitchCompleted",
            correlation_id=envelope.correlation_id,
            job_id=job_id,
            data=StitchCompletedData(final_key=final_key).model_dump(),
        )
        await OutboxRepository(session).add(
            OutboxEvent(
                aggregate_id=job_id,
                event_type="StitchCompleted",
                routing_key=ROUTING_KEY[TaskStage.NOTIFY],
                payload=envelope_out.model_dump(mode="json"),
            )
        )

    log.info("stitch_completed", job_id=job_id, final_key=final_key)
