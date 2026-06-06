from __future__ import annotations

from app.messaging.retry import _delay_bucket, _jitter


def test_jitter_within_bounds():
    for attempt in range(1, 4):
        base = _delay_bucket(attempt)
        for _ in range(50):
            result = _jitter(base, jitter_pct=0.3)
            assert int(base * 0.7) <= result <= int(base * 1.3) + 1


def test_delay_bucket_increases():
    b1 = _delay_bucket(1)
    b2 = _delay_bucket(2)
    b3 = _delay_bucket(3)
    assert b1 < b2 <= b3


def test_delay_bucket_clamps_at_max():
    from app.config import get_settings

    settings = get_settings()
    b_over = _delay_bucket(100)
    assert b_over == settings.retry_max_ms


def test_delay_bucket_attempt_1_is_base():
    from app.config import get_settings

    settings = get_settings()
    assert _delay_bucket(1) == settings.retry_base_ms
