from __future__ import annotations

import json
import uuid

import aio_pika

from app.db.models import OutboxEvent, Task
from app.db.uow import unit_of_work
from app.domain.enums import ROUTING_KEY, TaskStage
from app.domain.errors import DuplicateEventError, StaleTransitionError
from app.domain.events import EventEnvelope, ParseCompletedData
from app.infra import storage
from app.logging import get_logger
from app.repositories.cache_repo import ProcessedEventRepository
from app.repositories.job_repo import JobRepository
from app.repositories.outbox_repo import OutboxRepository
from app.repositories.task_repo import TaskRepository
from app.vendors import llm

log = get_logger(__name__)

WORKER_ID = f"worker-parse-{uuid.uuid4().hex[:8]}"
LEASE_SECONDS = 120


async def handle_parse(envelope: EventEnvelope, channel: aio_pika.abc.AbstractChannel) -> None:
    job_id = envelope.job_id

    # Step 1: dedup check + claim in one short tx
    task_id: str
    manuscript_key: str
    async with unit_of_work() as session:
        event_repo = ProcessedEventRepository(session)
        is_new = await event_repo.record(envelope.event_id, TaskStage.PARSE.value)
        if not is_new:
            raise DuplicateEventError()

        task_repo = TaskRepository(session)
        task = await task_repo.get_by_job_stage(job_id, TaskStage.PARSE.value)
        if task is None:
            raise StaleTransitionError()

        claimed = await task_repo.claim(task.id, WORKER_ID, LEASE_SECONDS)
        if not claimed:
            raise StaleTransitionError()

        task_id = task.id
        manuscript_key = task.input_ref or envelope.data.get("manuscript_key", "")

        job_repo = JobRepository(session)
        await job_repo.transition_status(job_id, "PENDING", "PARSING")

    # Step 2: I/O outside any transaction
    log.info("parse_downloading", manuscript_key=manuscript_key)
    manuscript_bytes = await storage.get_object(manuscript_key)
    text = manuscript_bytes.decode("utf-8")

    blocks = await llm.parse_manuscript(text)

    parsed_key = f"parsed/{job_id}/blocks.json"
    await storage.put_object(
        parsed_key,
        json.dumps(blocks).encode("utf-8"),
        content_type="application/json",
    )

    # Step 3: commit result + next-stage task + outbox — one tx
    async with unit_of_work() as session:
        task_repo = TaskRepository(session)
        await task_repo.complete(task_id, output_ref=parsed_key)

        tts_task = Task(
            id=str(uuid.uuid4()),
            job_id=job_id,
            stage=TaskStage.TTS.value,
            status="QUEUED",
            input_ref=parsed_key,
        )
        await task_repo.create(tts_task)

        envelope_out = EventEnvelope.build(
            event_type="ParseCompleted",
            correlation_id=envelope.correlation_id,
            job_id=job_id,
            data=ParseCompletedData(
                parsed_key=parsed_key, block_count=len(blocks)
            ).model_dump(),
        )
        outbox_event = OutboxEvent(
            aggregate_id=job_id,
            event_type="ParseCompleted",
            routing_key=ROUTING_KEY[TaskStage.TTS],
            payload=envelope_out.model_dump(mode="json"),
        )
        await OutboxRepository(session).add(outbox_event)

    log.info("parse_completed", job_id=job_id, block_count=len(blocks))
