from __future__ import annotations

import aio_pika

EXCHANGE_NAME = "pipeline"
DELAY_EXCHANGE_NAME = "pipeline.delay"

STAGES = ["parse", "tts", "stitch", "notify"]
DELAY_BUCKETS_MS = [2000, 4000, 8000]


async def declare_topology(channel: aio_pika.abc.AbstractChannel) -> aio_pika.abc.AbstractExchange:
    # Main topic exchange
    exchange = await channel.declare_exchange(
        EXCHANGE_NAME,
        aio_pika.ExchangeType.TOPIC,
        durable=True,
    )

    for stage in STAGES:
        dlx_name = f"dlx.{stage}"
        dlq_name = f"q.{stage}.dlq"

        # Per-stage DLX (direct)
        dlx = await channel.declare_exchange(
            dlx_name,
            aio_pika.ExchangeType.DIRECT,
            durable=True,
        )

        # Terminal DLQ
        await channel.declare_queue(
            dlq_name,
            durable=True,
            arguments={"x-queue-type": "classic"},
        )
        dlq = await channel.get_queue(dlq_name)
        await dlq.bind(dlx, routing_key=dlq_name)

        # Work queue bound to main exchange
        work_queue = await channel.declare_queue(
            f"q.{stage}",
            durable=True,
            arguments={
                "x-dead-letter-exchange": dlx_name,
                "x-dead-letter-routing-key": dlq_name,
                "x-queue-type": "classic",
            },
        )
        await work_queue.bind(exchange, routing_key=f"job.{stage}")

    # Delay queues: one per TTL bucket, DLX back to main exchange
    delay_exchange = await channel.declare_exchange(
        DELAY_EXCHANGE_NAME,
        aio_pika.ExchangeType.DIRECT,
        durable=True,
    )

    for ttl_ms in DELAY_BUCKETS_MS:
        delay_queue_name = f"q.delay.{ttl_ms}"
        delay_queue = await channel.declare_queue(
            delay_queue_name,
            durable=True,
            arguments={
                "x-message-ttl": ttl_ms,
                "x-dead-letter-exchange": EXCHANGE_NAME,
                "x-queue-type": "classic",
            },
        )
        await delay_queue.bind(delay_exchange, routing_key=delay_queue_name)

    return exchange
