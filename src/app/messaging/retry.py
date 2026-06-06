from __future__ import annotations

import json
import random
import uuid
from typing import Any

import aio_pika

from app.config import get_settings
from app.logging import get_logger
from app.messaging.topology import DELAY_BUCKETS_MS, DELAY_EXCHANGE_NAME

log = get_logger(__name__)

ATTEMPT_HEADER = "x-attempt"
STAGE_HEADER = "x-stage"
ORIGINAL_ROUTING_KEY_HEADER = "x-original-routing-key"


def _jitter(base_ms: int, jitter_pct: float = 0.3) -> int:
    delta = int(base_ms * jitter_pct)
    return base_ms + random.randint(-delta, delta)


def _refresh_event_id(body: bytes) -> bytes:
    """Return body with a new event_id so processed_events dedup doesn't block this retry."""
    try:
        payload = json.loads(body)
        payload["event_id"] = str(uuid.uuid4())
        return json.dumps(payload).encode()
    except Exception:
        return body


def _delay_bucket(attempt: int) -> int:
    settings = get_settings()
    buckets = [settings.retry_base_ms, settings.retry_base_ms * 2, settings.retry_max_ms]
    idx = min(attempt - 1, len(buckets) - 1)
    return buckets[idx]


async def schedule_retry(
    channel: aio_pika.abc.AbstractChannel,
    original_message: aio_pika.abc.AbstractIncomingMessage,
    stage: str,
    routing_key: str,
) -> None:
    headers: dict[str, Any] = dict(original_message.headers or {})
    attempt = int(headers.get(ATTEMPT_HEADER, 0)) + 1
    settings = get_settings()

    if attempt > settings.retry_max_attempts:
        await route_to_dlq(channel, original_message, stage)
        return

    ttl_ms = _jitter(_delay_bucket(attempt))
    bucket = min(DELAY_BUCKETS_MS, key=lambda b: abs(b - ttl_ms))

    headers[ATTEMPT_HEADER] = attempt
    headers[STAGE_HEADER] = stage
    headers[ORIGINAL_ROUTING_KEY_HEADER] = routing_key

    delay_exchange = await channel.get_exchange(DELAY_EXCHANGE_NAME)
    retry_msg = aio_pika.Message(
        body=_refresh_event_id(original_message.body),
        headers=headers,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )
    await delay_exchange.publish(retry_msg, routing_key=f"q.delay.{stage}.{bucket}")
    log.info("retry_scheduled", stage=stage, attempt=attempt, delay_ms=bucket)


async def route_to_dlq(
    channel: aio_pika.abc.AbstractChannel,
    original_message: aio_pika.abc.AbstractIncomingMessage,
    stage: str,
) -> None:
    dlx = await channel.get_exchange(f"dlx.{stage}")
    dlq_msg = aio_pika.Message(
        body=original_message.body,
        headers=dict(original_message.headers or {}),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )
    await dlx.publish(dlq_msg, routing_key=f"q.{stage}.dlq")
    log.warning("routed_to_dlq", stage=stage)


async def republish_for_semaphore_retry(
    channel: aio_pika.abc.AbstractChannel,
    original_message: aio_pika.abc.AbstractIncomingMessage,
    routing_key: str,
    stage: str = "tts",
) -> None:
    """Re-queue with a short delay when TTS semaphore is full — never block the consumer."""

    headers = dict(original_message.headers or {})
    delay_exchange = await channel.get_exchange(DELAY_EXCHANGE_NAME)
    retry_msg = aio_pika.Message(
        body=_refresh_event_id(original_message.body),
        headers=headers,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )
    await delay_exchange.publish(retry_msg, routing_key=f"q.delay.{stage}.{DELAY_BUCKETS_MS[0]}")
    log.debug("semaphore_retry_scheduled")
