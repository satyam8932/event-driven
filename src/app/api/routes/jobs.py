from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException
from sqlalchemy.exc import SQLAlchemyError

from app.api.schemas import CreateJobRequest, CreateJobResponse, JobResponse
from app.db.models import Job, OutboxEvent, Task
from app.db.uow import unit_of_work
from app.domain.enums import ROUTING_KEY, TaskStage
from app.domain.errors import StorageError
from app.domain.events import EventEnvelope, JobCreatedData
from app.infra import storage
from app.logging import get_logger, set_correlation_context
from app.repositories.job_repo import JobRepository
from app.repositories.outbox_repo import OutboxRepository
from app.repositories.task_repo import TaskRepository

log = get_logger(__name__)
router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("", response_model=CreateJobResponse, status_code=202)
async def create_job(body: CreateJobRequest) -> CreateJobResponse:
    job_id = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())
    set_correlation_context(correlation_id=correlation_id, job_id=job_id)

    manuscript_key = f"manuscripts/{job_id}/manuscript.txt"

    # Store manuscript in MinIO BEFORE the DB tx — failure here is clean (no DB row)
    try:
        await storage.put_object(
            manuscript_key, body.manuscript.encode("utf-8"), content_type="text/plain"
        )
    except StorageError as exc:
        log.error("manuscript_upload_failed", error=str(exc))
        raise HTTPException(status_code=503, detail="Object storage unavailable") from exc

    # DB tx: job + parse task + outbox event — atomic
    try:
        async with unit_of_work() as session:
            job_repo = JobRepository(session)
            task_repo = TaskRepository(session)
            outbox_repo = OutboxRepository(session)

            job = Job(
                id=job_id,
                status="PENDING",
                manuscript_key=manuscript_key,
                correlation_id=correlation_id,
            )
            await job_repo.create(job)

            task = Task(
                id=str(uuid.uuid4()),
                job_id=job_id,
                stage=TaskStage.PARSE.value,
                status="QUEUED",
                input_ref=manuscript_key,
            )
            await task_repo.create(task)

            envelope = EventEnvelope.build(
                event_type="JobCreated",
                correlation_id=correlation_id,
                job_id=job_id,
                data=JobCreatedData(manuscript_key=manuscript_key).model_dump(),
            )
            outbox_event = OutboxEvent(
                aggregate_id=job_id,
                event_type="JobCreated",
                routing_key=ROUTING_KEY[TaskStage.PARSE],
                payload=envelope.model_dump(mode="json"),
            )
            await outbox_repo.add(outbox_event)

    except SQLAlchemyError as exc:
        log.error("db_unavailable", error=str(exc))
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    log.info("job_created", job_id=job_id)
    return CreateJobResponse(job_id=job_id, correlation_id=correlation_id)


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str) -> JobResponse:
    try:
        async with unit_of_work() as session:
            job_repo = JobRepository(session)
            job = await job_repo.get(job_id)
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail="Database unavailable") from exc

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobResponse.model_validate(job)
