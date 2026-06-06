from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from redis.asyncio import Redis

from app.logging import get_logger

log = get_logger(__name__)

_LOCK_TTL_MS = 30_000  # 30s per-hash generation lock


@asynccontextmanager
async def tts_generation_lock(redis: Redis, text_hash: str) -> AsyncGenerator[bool, None]:
    """
    Per-hash mutex to prevent cache stampede: two workers computing the same block simultaneously.
    Yields True if lock was acquired (caller should proceed with vendor call).
    Yields False if lock was already held (caller should re-check cache and skip vendor call).
    """
    key = f"tts:gen_lock:{text_hash}"
    acquired = await redis.set(key, "1", nx=True, px=_LOCK_TTL_MS)
    try:
        yield bool(acquired)
    finally:
        if acquired:
            await redis.delete(key)
