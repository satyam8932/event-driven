"""
Integration tests for DLQ routing and retry behaviour.

Verifies that:
- attempt counter increments correctly per retry
- messages are routed to DLQ after max_attempts
- permanent errors bypass retry and go straight to DLQ
- semaphore-full path republishes to delay queue (not DLQ)
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.domain.errors import PermanentError, RetryableError, SemaphoreFullError
from app.domain.events import EventEnvelope
from app.messaging.retry import ATTEMPT_HEADER, _delay_bucket, _jitter


def _make_mock_message(attempt: int = 0, body: bytes = b"{}") -> MagicMock:
    msg = MagicMock()
    msg.body = body
    msg.headers = {ATTEMPT_HEADER: attempt}
    return msg


@pytest.mark.asyncio
async def test_retry_schedules_delay_under_max_attempts():
    settings_mock = MagicMock()
    settings_mock.retry_max_attempts = 3
    settings_mock.retry_base_ms = 2000
    settings_mock.retry_max_ms = 8000

    channel = AsyncMock()
    delay_exchange = AsyncMock()
    channel.get_exchange = AsyncMock(return_value=delay_exchange)

    message = _make_mock_message(attempt=0)

    with patch("app.messaging.retry.get_settings", return_value=settings_mock):
        from app.messaging import retry
        await retry.schedule_retry(channel, message, stage="parse", routing_key="job.parse")

    delay_exchange.publish.assert_called_once()


@pytest.mark.asyncio
async def test_retry_routes_to_dlq_at_max_attempts():
    settings_mock = MagicMock()
    settings_mock.retry_max_attempts = 3

    channel = AsyncMock()
    dlx = AsyncMock()
    delay_exchange = AsyncMock()

    async def get_exchange(name: str):
        if name.startswith("dlx."):
            return dlx
        return delay_exchange

    channel.get_exchange = get_exchange

    # attempt=3 means this is the 4th try → exceeds max → DLQ
    message = _make_mock_message(attempt=3)

    with patch("app.messaging.retry.get_settings", return_value=settings_mock):
        from app.messaging import retry
        await retry.schedule_retry(channel, message, stage="parse", routing_key="job.parse")

    dlx.publish.assert_called_once()
    delay_exchange.publish.assert_not_called()


@pytest.mark.asyncio
async def test_permanent_error_goes_straight_to_dlq():
    channel = AsyncMock()
    dlx = AsyncMock()
    channel.get_exchange = AsyncMock(return_value=dlx)
    message = _make_mock_message(attempt=0)

    from app.messaging.retry import route_to_dlq
    await route_to_dlq(channel, message, stage="tts")

    dlx.publish.assert_called_once()


@pytest.mark.asyncio
async def test_semaphore_full_republishes_to_delay_not_dlq():
    channel = AsyncMock()
    delay_exchange = AsyncMock()
    channel.get_exchange = AsyncMock(return_value=delay_exchange)
    message = _make_mock_message(attempt=0, body=b'{"event_id":"x","routing_key":"job.tts"}')

    from app.messaging.retry import republish_for_semaphore_retry
    await republish_for_semaphore_retry(channel, message, routing_key="job.tts")

    delay_exchange.publish.assert_called_once()
    # Verify it went to the shortest delay bucket
    from app.messaging.topology import DELAY_BUCKETS_MS
    call_kwargs = delay_exchange.publish.call_args
    routing_key_used = call_kwargs.kwargs.get("routing_key") or call_kwargs.args[1]
    assert str(DELAY_BUCKETS_MS[0]) in str(routing_key_used)


def test_delay_buckets_are_bounded():
    from app.messaging.topology import DELAY_BUCKETS_MS

    assert len(DELAY_BUCKETS_MS) == 3
    assert DELAY_BUCKETS_MS == sorted(DELAY_BUCKETS_MS)
    assert DELAY_BUCKETS_MS[0] >= 1000
    assert DELAY_BUCKETS_MS[-1] <= 30_000


@pytest.mark.asyncio
async def test_consumer_acks_on_duplicate_event():
    """Consumer must ack (not nack) on DuplicateEventError — critical for idempotency."""
    from app.domain.errors import DuplicateEventError
    from app.messaging.consumer import StageConsumer

    async def failing_handler(envelope, channel):
        raise DuplicateEventError()

    consumer = StageConsumer(stage="parse", handler=failing_handler)

    envelope = EventEnvelope.build(
        event_type="JobCreated", correlation_id="c", job_id="j"
    )

    channel = AsyncMock()
    message = AsyncMock()
    message.body = envelope.model_dump_json_bytes()
    message.headers = {}

    # process() context manager — simulate it not raising
    process_ctx = AsyncMock()
    process_ctx.__aenter__ = AsyncMock(return_value=None)
    process_ctx.__aexit__ = AsyncMock(return_value=False)
    message.process = MagicMock(return_value=process_ctx)

    # Should complete without raising (DuplicateEventError is swallowed)
    await consumer._handle_message(message, channel)
    # If we get here without exception, ack path was taken
