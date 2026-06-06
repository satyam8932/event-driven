from __future__ import annotations

import logging
import sys
from contextvars import ContextVar

import structlog

_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")
_job_id: ContextVar[str] = ContextVar("job_id", default="")


def set_correlation_context(*, correlation_id: str, job_id: str) -> None:
    _correlation_id.set(correlation_id)
    _job_id.set(job_id)


def _inject_context(
    logger: logging.Logger, method: str, event_dict: dict
) -> dict:
    if cid := _correlation_id.get():
        event_dict["correlation_id"] = cid
    if jid := _job_id.get():
        event_dict["job_id"] = jid
    return event_dict


def configure_logging(level: str = "INFO") -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _inject_context,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(level)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
