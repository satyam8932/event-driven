from __future__ import annotations

from datetime import datetime

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ProcessedEvent, TtsCache


class ProcessedEventRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record(self, event_id: str, stage: str) -> bool:
        """Insert and return True if new, False if duplicate."""
        result = await self._session.execute(
            insert(ProcessedEvent)
            .values(event_id=event_id, stage=stage)
            .on_conflict_do_nothing(index_elements=["event_id"])
            .returning(ProcessedEvent.event_id)
        )
        return result.scalar_one_or_none() is not None

    async def prune_old(self, older_than: datetime) -> int:
        result: CursorResult[tuple[()]] = await self._session.execute(  # type: ignore[assignment]
            delete(ProcessedEvent).where(ProcessedEvent.processed_at < older_than)
        )
        return result.rowcount


class TtsCacheRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, text_hash: str) -> TtsCache | None:
        result = await self._session.execute(
            select(TtsCache).where(TtsCache.text_hash == text_hash)
        )
        return result.scalar_one_or_none()

    async def set(self, text_hash: str, object_key: str) -> None:
        await self._session.execute(
            insert(TtsCache)
            .values(text_hash=text_hash, object_key=object_key)
            .on_conflict_do_nothing(index_elements=["text_hash"])
        )
