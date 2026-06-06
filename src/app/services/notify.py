from __future__ import annotations

import uuid

import aio_pika
import httpx

from app.config import get_settings
from app.db.uow import unit_of_work
from app.domain.enums import TaskStage
from app.domain.errors import DuplicateEventError, StaleTransitionError
from app.domain.events import EventEnvelope
from app.logging import get_logger
from app.repositories.cache_repo import ProcessedEventRepository
from app.repositories.job_repo import JobRepository
from app.repositories.task_repo import TaskRepository

log = get_logger(__name__)

WORKER_ID = f"worker-notify-{uuid.uuid4().hex[:8]}"
LEASE_SECONDS = 30


async def handle_notify(envelope: EventEnvelope, channel: aio_pika.abc.AbstractChannel) -> None:
    settings = get_settings()
    job_id = envelope.job_id

    # Step 1: dedup + claim — processed_events INSERT is the idempotency gate
    # If worker crashes after webhook fires but BEFORE this ack, redelivery hits
    # ON CONFLICT DO NOTHING here and skips — no duplicate webhook.
    async with unit_of_work() as session:
        event_repo = ProcessedEventRepository(session)
        if not await event_repo.record(envelope.event_id, TaskStage.NOTIFY.value):
            raise DuplicateEventError()

        task_repo = TaskRepository(session)
        task = await task_repo.get_by_job_stage(job_id, TaskStage.NOTIFY.value)
        if task is None:
            raise StaleTransitionError()

        if not await task_repo.claim(task.id, WORKER_ID, LEASE_SECONDS):
            raise StaleTransitionError()

        task_id = task.id
        final_key = task.input_ref or envelope.data.get("final_key", "")
        await JobRepository(session).transition_status(job_id, "STITCHING", "NOTIFYING")

    # Step 2: fire webhook (best-effort; failure is logged, not retried at pipeline level)
    if settings.webhook_url:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(
                    settings.webhook_url,
                    json={
                        "job_id": job_id,
                        "status": "COMPLETED",
                        "final_key": final_key,
                        "correlation_id": envelope.correlation_id,
                    },
                )
            log.info("webhook_fired", job_id=job_id, url=settings.webhook_url)
        except Exception as exc:
            log.warning("webhook_failed", job_id=job_id, error=str(exc))
    else:
        log.info("notify_no_webhook_configured", job_id=job_id, final_key=final_key)

    # Step 3: mark completed — job moves to COMPLETED
    async with unit_of_work() as session:
        task_repo = TaskRepository(session)
        await task_repo.complete(task_id, output_ref=final_key)
        await JobRepository(session).mark_completed(job_id, final_key)

    log.info("job_completed", job_id=job_id, final_key=final_key)
