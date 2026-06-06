from __future__ import annotations

import aio_pika
import aio_pika.abc

from app.config import get_settings
from app.logging import get_logger

log = get_logger(__name__)

_connection: aio_pika.abc.AbstractRobustConnection | None = None


async def get_connection() -> aio_pika.abc.AbstractRobustConnection:
    global _connection
    if _connection is None or _connection.is_closed:
        settings = get_settings()
        _connection = await aio_pika.connect_robust(
            settings.rabbitmq_url,
            reconnect_interval=5,
            fail_fast=False,
        )
        log.info("rabbitmq_connected")
    return _connection


async def close_connection() -> None:
    global _connection
    if _connection and not _connection.is_closed:
        await _connection.close()
        _connection = None
