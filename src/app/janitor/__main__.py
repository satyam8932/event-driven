from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from app.config import get_settings
from app.db.models import OutboxEvent
from app.db.uow import unit_of_work
from app.domain.enums import ROUTING_KEY, TaskStage
from app.domain.events import EventEnvelope
from app.infra.redis import close_redis
from app.logging import configure_logging, get_logger
from app.repositories.cache_repo import ProcessedEventRepository
from app.repositories.job_repo import JobRepository
from app.repositories.outbox_repo import OutboxRepository
from app.repositories.task_repo import TaskRepository

log = get_logger(__name__)

_STAGE_ROUTING: dict[str, str] = {
    TaskStage.PARSE.value: ROUTING_KEY[TaskStage.PARSE],
    TaskStage.TTS.value: ROUTING_KEY[TaskStage.TTS],
    TaskStage.STITCH.value: ROUTING_KEY[TaskStage.STITCH],
    TaskStage.NOTIFY.value: ROUTING_KEY[TaskStage.NOTIFY],
}

_STAGE_EVENT_TYPE: dict[str, str] = {
    TaskStage.PARSE.value: "JobCreated",
    TaskStage.TTS.value: "ParseCompleted",
    TaskStage.STITCH.value: "TtsCompleted",
    TaskStage.NOTIFY.value: "StitchCompleted",
}


async def reap_expired_leases() -> None:
    settings = get_settings()
    cutoff = datetime.now(UTC) - timedelta(seconds=settings.janitor_lease_timeout)

    async with unit_of_work() as session:
        task_repo = TaskRepository(session)
        expired = await task_repo.find_expired_leases(cutoff)

        if not expired:
            return

        log.warning("janitor_expired_leases", count=len(expired))
        outbox_repo = OutboxRepository(session)

        for task in expired:
            await task_repo.reset_to_queued(task.id)

            job = await JobRepository(session).get(task.job_id)
            if job is None:
                continue

            routing_key = _STAGE_ROUTING.get(task.stage)
            event_type = _STAGE_EVENT_TYPE.get(task.stage, "Unknown")
            if not routing_key:
                continue

            # Re-emit stage event via outbox so relay re-publishes it
            envelope = EventEnvelope.build(
                event_type=event_type,
                correlation_id=job.correlation_id,
                job_id=task.job_id,
                data={"input_ref": task.input_ref or ""},
            )
            await outbox_repo.add(
                OutboxEvent(
                    aggregate_id=task.job_id,
                    event_type=event_type,
                    routing_key=routing_key,
                    payload=envelope.model_dump(mode="json"),
                )
            )
            log.info(
                "janitor_requeued_task",
                task_id=task.id,
                job_id=task.job_id,
                stage=task.stage,
            )


async def prune_outbox() -> None:
    settings = get_settings()
    older_than = datetime.now(UTC) - timedelta(seconds=settings.janitor_outbox_prune_age)

    async with unit_of_work() as session:
        deleted = await OutboxRepository(session).prune_published(older_than)

    if deleted:
        log.info("janitor_outbox_pruned", deleted=deleted)


async def prune_processed_events() -> None:
    settings = get_settings()
    older_than = datetime.now(UTC) - timedelta(seconds=settings.janitor_outbox_prune_age)

    async with unit_of_work() as session:
        deleted = await ProcessedEventRepository(session).prune_old(older_than)

    if deleted:
        log.info("janitor_processed_events_pruned", deleted=deleted)


async def janitor_loop() -> None:
    settings = get_settings()
    log.info("janitor_started", interval=settings.janitor_interval)

    while True:
        try:
            await reap_expired_leases()
            await prune_outbox()
            await prune_processed_events()
        except Exception as exc:
            log.exception("janitor_error", error=str(exc))
        await asyncio.sleep(settings.janitor_interval)


async def main() -> None:
    configure_logging()
    try:
        await janitor_loop()
    finally:
        await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
