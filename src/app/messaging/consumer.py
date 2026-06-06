from __future__ import annotations

import asyncio
import signal
from typing import Awaitable, Callable

import aio_pika
import aio_pika.abc

from app.domain.errors import (
    DuplicateEventError,
    PermanentError,
    RetryableError,
    SemaphoreFullError,
    StaleTransitionError,
)
from app.domain.events import EventEnvelope
from app.logging import get_logger, set_correlation_context
from app.messaging.retry import republish_for_semaphore_retry, route_to_dlq, schedule_retry
from app.messaging.topology import declare_topology

log = get_logger(__name__)

StageHandler = Callable[[EventEnvelope, aio_pika.abc.AbstractChannel], Awaitable[None]]


class StageConsumer:
    def __init__(
        self,
        stage: str,
        handler: StageHandler,
        prefetch: int = 8,
    ) -> None:
        self._stage = stage
        self._handler = handler
        self._prefetch = prefetch
        self._routing_key = f"job.{stage}"
        self._queue_name = f"q.{stage}"
        self._running = True

    async def run(self, connection: aio_pika.abc.AbstractRobustConnection) -> None:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=self._prefetch)
        exchange = await declare_topology(channel)
        queue = await channel.get_queue(self._queue_name)

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._shutdown)

        log.info("consumer_started", stage=self._stage, queue=self._queue_name)

        async with queue.iterator() as messages:
            async for message in messages:
                if not self._running:
                    break
                await self._handle_message(message, channel)

        await channel.close()
        log.info("consumer_stopped", stage=self._stage)

    async def _handle_message(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        channel: aio_pika.abc.AbstractChannel,
    ) -> None:
        async with message.process(ignore_processed=True):
            try:
                envelope = EventEnvelope.model_validate_json(message.body)
                set_correlation_context(
                    correlation_id=envelope.correlation_id,
                    job_id=envelope.job_id,
                )
                log.info("message_received", stage=self._stage, event_type=envelope.event_type)
                await self._handler(envelope, channel)

            except DuplicateEventError:
                log.info("duplicate_event_skipped", stage=self._stage)
                # ack (already handled by process context manager)

            except StaleTransitionError:
                log.info("stale_transition_skipped", stage=self._stage)
                # ack

            except SemaphoreFullError:
                log.info("semaphore_full_retry", stage=self._stage)
                await republish_for_semaphore_retry(channel, message, self._routing_key)
                # ack original

            except PermanentError as exc:
                log.error("permanent_error", stage=self._stage, error=str(exc))
                await route_to_dlq(channel, message, self._stage)
                # ack original

            except RetryableError as exc:
                log.warning("retryable_error", stage=self._stage, error=str(exc))
                await schedule_retry(channel, message, self._stage, self._routing_key)
                # ack original

            except Exception as exc:
                log.exception("unexpected_error", stage=self._stage, error=str(exc))
                await schedule_retry(channel, message, self._stage, self._routing_key)

    def _shutdown(self) -> None:
        log.info("consumer_shutdown_signal", stage=self._stage)
        self._running = False
