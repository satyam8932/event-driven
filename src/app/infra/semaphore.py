from __future__ import annotations

import time
import uuid

from redis.asyncio import Redis

from app.domain.errors import SemaphoreFullError
from app.logging import get_logger

log = get_logger(__name__)

SEMAPHORE_KEY = "tts:semaphore"

# Atomic Lua: evict expired holders, check capacity, acquire if room.
_ACQUIRE_SCRIPT = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local token = ARGV[3]
local lease_until = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, lease_until, token)
    return 1
end
return 0
"""

_RENEW_SCRIPT = """
local key = KEYS[1]
local token = ARGV[1]
local lease_until = tonumber(ARGV[2])
local exists = redis.call('ZSCORE', key, token)
if exists then
    redis.call('ZADD', key, lease_until, token)
    return 1
end
return 0
"""

_RELEASE_SCRIPT = """
return redis.call('ZREM', KEYS[1], ARGV[1])
"""


class TtsSemaphore:
    def __init__(self, redis: Redis[str], limit: int, lease_seconds: int) -> None:
        self._redis = redis
        self._limit = limit
        self._lease_seconds = lease_seconds
        self._acquire_fn = redis.register_script(_ACQUIRE_SCRIPT)
        self._renew_fn = redis.register_script(_RENEW_SCRIPT)
        self._release_fn = redis.register_script(_RELEASE_SCRIPT)

    async def acquire(self) -> str:
        token = str(uuid.uuid4())
        now = time.time()
        lease_until = now + self._lease_seconds
        result = await self._acquire_fn(
            keys=[SEMAPHORE_KEY],
            args=[now, self._limit, token, lease_until],
        )
        if not result:
            raise SemaphoreFullError("TTS semaphore full")
        log.debug("semaphore_acquired", token=token)
        return token

    async def renew(self, token: str) -> bool:
        lease_until = time.time() + self._lease_seconds
        result = await self._renew_fn(keys=[SEMAPHORE_KEY], args=[token, lease_until])
        return bool(result)

    async def release(self, token: str) -> None:
        await self._release_fn(keys=[SEMAPHORE_KEY], args=[token])
        log.debug("semaphore_released", token=token)

    async def current_count(self) -> int:
        now = time.time()
        await self._redis.zremrangebyscore(SEMAPHORE_KEY, "-inf", now)
        return await self._redis.zcard(SEMAPHORE_KEY)
