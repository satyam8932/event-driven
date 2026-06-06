from __future__ import annotations

import asyncio
import json

import aio_pika

from app.config import get_settings
from app.db.uow import unit_of_work
from app.infra.broker import close_connection, get_connection
from app.logging import configure_logging, get_logger
from app.messaging.topology import declare_topology
from app.repositories.outbox_repo import OutboxRepository

log = get_logger(__name__)


async def relay_loop() -> None:
    settings = get_settings()
    connection = await get_connection()
    channel = await connection.channel()
    exchange = await declare_topology(channel)

    log.info("relay_started", poll_interval=settings.relay_poll_interval)

    while True:
        try:
            await _drain_batch(channel, exchange, settings.relay_batch_size)
        except Exception as exc:
            log.exception("relay_error", error=str(exc))
        await asyncio.sleep(settings.relay_poll_interval)


async def _drain_batch(
    channel: aio_pika.abc.AbstractChannel,
    exchange: aio_pika.abc.AbstractExchange,
    limit: int,
) -> None:
    async with unit_of_work() as session:
        repo = OutboxRepository(session)
        events = await repo.fetch_unpublished(limit)
        if not events:
            return

        log.debug("relay_draining", count=len(events))
        for event in events:
            try:
                message = aio_pika.Message(
                    body=json.dumps(event.payload).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    message_id=str(event.id),
                    content_type="application/json",
                )
                # Publisher confirms block until broker acks
                await exchange.publish(message, routing_key=event.routing_key)
                await repo.mark_published(event.id)
                log.debug("relay_published", event_id=event.id, routing_key=event.routing_key)
            except Exception as exc:
                log.error("relay_publish_failed", event_id=event.id, error=str(exc))
                await repo.mark_failed(event.id)


async def main() -> None:
    configure_logging()
    try:
        await relay_loop()
    finally:
        await close_connection()


if __name__ == "__main__":
    asyncio.run(main())
