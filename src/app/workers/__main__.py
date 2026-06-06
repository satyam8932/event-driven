from __future__ import annotations

import asyncio

from app.config import get_settings
from app.domain.enums import TaskStage
from app.infra.broker import close_connection, get_connection
from app.infra.redis import close_redis
from app.logging import configure_logging, get_logger
from app.messaging.consumer import StageConsumer
from app.services import notify, parsing, stitch, tts

log = get_logger(__name__)

HANDLER_REGISTRY = {
    TaskStage.PARSE.value: parsing.handle_parse,
    TaskStage.TTS.value: tts.handle_tts,
    TaskStage.STITCH.value: stitch.handle_stitch,
    TaskStage.NOTIFY.value: notify.handle_notify,
}


async def main() -> None:
    configure_logging()
    settings = get_settings()

    stages = [s.strip() for s in settings.worker_stages.split(",") if s.strip()]
    log.info("worker_starting", stages=stages)

    connection = await get_connection()

    consumers = []
    for stage in stages:
        handler = HANDLER_REGISTRY.get(stage)
        if handler is None:
            log.warning("unknown_stage_skipped", stage=stage)
            continue
        consumers.append(StageConsumer(stage=stage, handler=handler, prefetch=settings.worker_prefetch))

    if not consumers:
        log.error("no_valid_stages_configured")
        return

    try:
        await asyncio.gather(*[c.run(connection) for c in consumers])
    finally:
        await close_connection()
        await close_redis()


if __name__ == "__main__":
    asyncio.run(main())
