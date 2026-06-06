class PipelineError(Exception):
    pass


class RetryableError(PipelineError):
    """Transient failure — message should be retried with backoff."""


class PermanentError(PipelineError):
    """Non-recoverable failure — message should go straight to DLQ."""


class SemaphoreFullError(RetryableError):
    """TTS global concurrency limit reached."""


class VendorError(RetryableError):
    """Simulated vendor 5xx."""


class PoisonPillError(PermanentError):
    """Manuscript flagged as poison pill — will never succeed."""


class StorageError(RetryableError):
    """Object storage unavailable or I/O failure."""


class DuplicateEventError(Exception):
    """Event already processed — safe to ack without work."""


class StaleTransitionError(Exception):
    """State machine guard rejected the transition — job already advanced."""
