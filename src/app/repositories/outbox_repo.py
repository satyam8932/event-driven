from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import delete, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, event: OutboxEvent) -> None:
        self._session.add(event)
        await self._session.flush()

    async def fetch_unpublished(self, limit: int) -> list[OutboxEvent]:
        result = await self._session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(result.scalars().all())

    async def mark_published(self, event_id: int) -> None:
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(published_at=datetime.now(UTC))
        )

    async def mark_failed(self, event_id: int) -> None:
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(attempts=OutboxEvent.attempts + 1)
        )

    async def prune_published(self, older_than: datetime) -> int:
        result: CursorResult[tuple[()]] = await self._session.execute(  # type: ignore[assignment]
            delete(OutboxEvent).where(
                OutboxEvent.published_at.is_not(None),
                OutboxEvent.published_at < older_than,
            )
        )
        return result.rowcount
