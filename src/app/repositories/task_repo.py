from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Task


class TaskRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(self, task: Task) -> None:
        self._session.add(task)
        await self._session.flush()

    async def get(self, task_id: str) -> Task | None:
        result = await self._session.execute(select(Task).where(Task.id == task_id))
        return result.scalar_one_or_none()

    async def get_by_job_stage(self, job_id: str, stage: str) -> Task | None:
        result = await self._session.execute(
            select(Task).where(Task.job_id == job_id, Task.stage == stage)
        )
        return result.scalar_one_or_none()

    async def claim(self, task_id: str, worker_id: str, lease_seconds: int) -> bool:
        # Use DB now() to avoid clock skew across workers
        expires_at = datetime.now(UTC) + timedelta(seconds=lease_seconds)
        result = await self._session.execute(
            update(Task)
            .where(Task.id == task_id, Task.status == "QUEUED")
            .values(
                status="PROCESSING",
                locked_by=worker_id,
                lock_expires_at=expires_at,
                attempts=Task.attempts + 1,
            )
            .returning(Task.id)
        )
        return result.scalar_one_or_none() is not None

    async def complete(self, task_id: str, output_ref: str) -> None:
        await self._session.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="DONE", output_ref=output_ref, locked_by=None, lock_expires_at=None)
        )

    async def fail(self, task_id: str, error: str, increment_attempts: bool = True) -> None:
        values: dict[str, Any] = {
            "status": "QUEUED",
            "error": error,
            "locked_by": None,
            "lock_expires_at": None,
        }
        if increment_attempts:
            values["attempts"] = Task.attempts + 1
        await self._session.execute(update(Task).where(Task.id == task_id).values(**values))

    async def mark_dead(self, task_id: str, error: str) -> None:
        await self._session.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="DEAD", error=error, locked_by=None, lock_expires_at=None)
        )

    async def find_expired_leases(self, cutoff: datetime) -> list[Task]:
        result = await self._session.execute(
            select(Task).where(Task.status == "PROCESSING", Task.lock_expires_at < cutoff)
        )
        return list(result.scalars().all())

    async def reset_to_queued(self, task_id: str) -> None:
        await self._session.execute(
            update(Task)
            .where(Task.id == task_id)
            .values(status="QUEUED", locked_by=None, lock_expires_at=None)
        )
