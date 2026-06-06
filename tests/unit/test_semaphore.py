from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.domain.errors import SemaphoreFullError


class FakeRedis:
    """In-memory Redis mock with ZSET semantics for semaphore tests."""

    def __init__(self):
        self._zsets: dict[str, dict[str, float]] = {}
        self._scripts: dict[str, object] = {}

    def register_script(self, script: str):
        if "ZCARD" in script and "ZADD" in script and "ZREMRANGEBYSCORE" in script:
            return self._acquire_script
        if "ZREM" in script and "ZADD" in script and "ZSCORE" in script:
            return self._renew_script
        return self._release_script

    async def _acquire_script(self, keys, args):
        key = keys[0]
        now, limit, token, lease_until = float(args[0]), int(args[1]), args[2], float(args[3])
        zset = self._zsets.setdefault(key, {})
        expired = [k for k, v in zset.items() if v <= now]
        for k in expired:
            del zset[k]
        if len(zset) < limit:
            zset[token] = lease_until
            return 1
        return 0

    async def _renew_script(self, keys, args):
        key, token, lease_until = keys[0], args[0], float(args[1])
        zset = self._zsets.get(key, {})
        if token in zset:
            zset[token] = lease_until
            return 1
        return 0

    async def _release_script(self, keys, args):
        key, token = keys[0], args[0]
        zset = self._zsets.get(key, {})
        zset.pop(token, None)
        return 1

    async def zremrangebyscore(self, key, min_score, max_score):
        now = float(max_score)
        zset = self._zsets.get(key, {})
        expired = [k for k, v in zset.items() if v <= now]
        for k in expired:
            del zset[k]

    async def zcard(self, key):
        return len(self._zsets.get(key, {}))


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.mark.asyncio
async def test_semaphore_allows_up_to_limit(fake_redis):
    from app.infra.semaphore import TtsSemaphore

    sem = TtsSemaphore(fake_redis, limit=3, lease_seconds=60)
    tokens = []
    for _ in range(3):
        token = await sem.acquire()
        tokens.append(token)

    assert len(tokens) == 3

    with pytest.raises(SemaphoreFullError):
        await sem.acquire()


@pytest.mark.asyncio
async def test_semaphore_release_frees_slot(fake_redis):
    from app.infra.semaphore import TtsSemaphore

    sem = TtsSemaphore(fake_redis, limit=1, lease_seconds=60)
    token = await sem.acquire()

    with pytest.raises(SemaphoreFullError):
        await sem.acquire()

    await sem.release(token)
    new_token = await sem.acquire()
    assert new_token != token


@pytest.mark.asyncio
async def test_semaphore_expired_lease_frees_slot(fake_redis):
    from app.infra.semaphore import TtsSemaphore

    sem = TtsSemaphore(fake_redis, limit=1, lease_seconds=1)
    token = await sem.acquire()

    # Manually expire the lease by setting score to past time
    fake_redis._zsets["tts:semaphore"][token] = time.time() - 1

    # Should succeed now because lease expired
    new_token = await sem.acquire()
    assert new_token != token


@pytest.mark.asyncio
async def test_semaphore_concurrent_count(fake_redis):
    from app.infra.semaphore import TtsSemaphore

    sem = TtsSemaphore(fake_redis, limit=3, lease_seconds=60)
    t1 = await sem.acquire()
    t2 = await sem.acquire()

    count = await sem.current_count()
    assert count == 2

    await sem.release(t1)
    count = await sem.current_count()
    assert count == 1
