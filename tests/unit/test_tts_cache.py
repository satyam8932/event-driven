from __future__ import annotations

import hashlib

import pytest


def test_hash_is_deterministic():
    text = "The storm arrived at midnight."
    h1 = hashlib.sha256(text.encode()).hexdigest()
    h2 = hashlib.sha256(text.encode()).hexdigest()
    assert h1 == h2
    assert len(h1) == 64


def test_different_texts_different_hash():
    h1 = hashlib.sha256(b"hello").hexdigest()
    h2 = hashlib.sha256(b"world").hexdigest()
    assert h1 != h2


def test_hash_produces_valid_s3_key():
    text = "Some block of text."
    text_hash = hashlib.sha256(text.encode()).hexdigest()
    key = f"audio/{text_hash}.wav"
    assert key.startswith("audio/")
    assert key.endswith(".wav")
    assert len(text_hash) == 64


@pytest.mark.asyncio
async def test_poison_pill_raises_permanent_error():
    from app.domain.errors import PoisonPillError
    from app.vendors.tts import POISON_MARKER, generate_audio

    with pytest.raises(PoisonPillError):
        await generate_audio(f"{POISON_MARKER} This will always fail.")


@pytest.mark.asyncio
async def test_normal_tts_returns_bytes():
    from app.vendors.tts import generate_audio

    result = await generate_audio("Hello world.")
    assert isinstance(result, bytes)
    assert len(result) > 0
