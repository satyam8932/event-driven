from __future__ import annotations

import pytest

from app.domain.errors import (
    PermanentError,
    PoisonPillError,
    RetryableError,
    SemaphoreFullError,
    StorageError,
    VendorError,
)


def test_semaphore_full_is_retryable():
    assert issubclass(SemaphoreFullError, RetryableError)


def test_vendor_error_is_retryable():
    assert issubclass(VendorError, RetryableError)


def test_storage_error_is_retryable():
    assert issubclass(StorageError, RetryableError)


def test_poison_pill_is_permanent():
    assert issubclass(PoisonPillError, PermanentError)


def test_permanent_not_retryable():
    assert not issubclass(PermanentError, RetryableError)
