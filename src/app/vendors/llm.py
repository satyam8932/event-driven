from __future__ import annotations

import asyncio
import random

from app.domain.errors import VendorError
from app.logging import get_logger

log = get_logger(__name__)

FAILURE_RATE = 0.15
_vendor_call_count = 0


async def parse_manuscript(text: str) -> list[str]:
    """Simulated LLM parse. 15% chance of 500 error. Returns list of text blocks."""
    global _vendor_call_count

    await asyncio.sleep(random.uniform(0.5, 1.5))

    if random.random() < FAILURE_RATE:
        log.warning("llm_vendor_500")
        raise VendorError("LLM vendor returned 500")

    _vendor_call_count += 1
    sentences = [s.strip() for s in text.replace("\n", " ").split(".") if s.strip()]
    blocks = []
    chunk: list[str] = []
    for sentence in sentences:
        chunk.append(sentence)
        if len(chunk) >= 2:
            blocks.append(". ".join(chunk) + ".")
            chunk = []
    if chunk:
        blocks.append(". ".join(chunk) + ".")

    log.info("llm_parse_done", block_count=len(blocks))
    return blocks or [text]
