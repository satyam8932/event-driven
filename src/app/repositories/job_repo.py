from __future__ import annotations

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job
from app.logging import get_logger

log = get_logger(__name__)


class JobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, job: Job) -> None:
        self._session.add(job)
        await self._session.flush()

    async def get(self, job_id: str) -> Job | None:
        result = await self._session.execute(select(Job).where(Job.id == job_id))
        return result.scalar_one_or_none()

    async def transition_status(self, job_id: str, from_status: str, to_status: str) -> bool:
        result = await self._session.execute(
            update(Job)
            .where(Job.id == job_id, Job.status == from_status)
            .values(status=to_status)
            .returning(Job.id)
        )
        return result.scalar_one_or_none() is not None

    async def mark_failed(self, job_id: str, error: str) -> None:
        await self._session.execute(update(Job).where(Job.id == job_id).values(status="FAILED"))
        log.warning("job_failed", job_id=job_id, error=error)

    async def mark_completed(self, job_id: str, final_key: str) -> None:
        await self._session.execute(
            update(Job).where(Job.id == job_id).values(status="COMPLETED", final_key=final_key)
        )
