from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_session


@asynccontextmanager
async def unit_of_work() -> AsyncGenerator[AsyncSession, None]:
    """Provides a transactional session. Commits on clean exit, rolls back on exception."""
    async with get_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
