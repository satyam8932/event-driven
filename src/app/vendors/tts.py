from __future__ import annotations

import asyncio
import random

from app.domain.errors import PoisonPillError
from app.logging import get_logger

log = get_logger(__name__)

POISON_MARKER = "__POISON_PILL__"
_vendor_call_count = 0


def get_vendor_call_count() -> int:
    return _vendor_call_count


def reset_vendor_call_count() -> None:
    global _vendor_call_count
    _vendor_call_count = 0


async def generate_audio(text_block: str) -> bytes:
    """Simulated TTS vendor. Poison pill raises PermanentError immediately."""
    global _vendor_call_count

    if POISON_MARKER in text_block:
        raise PoisonPillError("Poison pill manuscript — permanent TTS failure")

    await asyncio.sleep(random.uniform(1.0, 3.0))

    _vendor_call_count += 1
    fake_wav = b"RIFF" + text_block[:32].encode().ljust(32, b"\x00")
    log.info("tts_vendor_called", block_preview=text_block[:40])
    return fake_wav
