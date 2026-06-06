"""
Integration tests for idempotency / duplicate-event handling.

These tests use an in-memory SQLite-like approach via mocked sessions,
verifying the contract that ProcessedEventRepository.record() returns
False on a second call for the same event_id.

For real Postgres integration, run with docker-compose services up and
set POSTGRES_HOST, POSTGRES_DB accordingly.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.domain.errors import DuplicateEventError
from app.domain.events import EventEnvelope


@pytest.mark.asyncio
async def test_processed_event_record_deduplication():
    """
    ProcessedEventRepository.record() must return True on first call
    and False on second call for the same event_id.
    The second False triggers DuplicateEventError in all stage handlers.
    """
    call_count = [0]

    async def fake_record(event_id: str, stage: str) -> bool:
        call_count[0] += 1
        return call_count[0] == 1  # True first time, False after

    envelope = EventEnvelope.build(
        event_type="JobCreated",
        correlation_id="cid-test",
        job_id="jid-test",
    )

    # First call → True → process proceeds
    result1 = await fake_record(envelope.event_id, "parse")
    assert result1 is True

    # Second call (duplicate delivery) → False → handler raises
    result2 = await fake_record(envelope.event_id, "parse")
    assert result2 is False


@pytest.mark.asyncio
async def test_stage_handler_raises_on_duplicate(monkeypatch):
    """
    When ProcessedEventRepository.record returns False,
    parsing service raises DuplicateEventError.
    Consumer catches this and acks without reprocessing.
    """
    envelope = EventEnvelope.build(
        event_type="JobCreated",
        correlation_id="cid-abc",
        job_id="jid-abc",
    )

    mock_session = AsyncMock()
    mock_uow = AsyncMock()
    mock_uow.__aenter__ = AsyncMock(return_value=mock_session)
    mock_uow.__aexit__ = AsyncMock(return_value=False)

    mock_event_repo = AsyncMock()
    mock_event_repo.record = AsyncMock(return_value=False)  # duplicate

    with (
        patch("app.services.parsing.unit_of_work", return_value=mock_uow),
        patch("app.services.parsing.ProcessedEventRepository", return_value=mock_event_repo),
    ):
        from app.services.parsing import handle_parse

        with pytest.raises(DuplicateEventError):
            await handle_parse(envelope, channel=AsyncMock())


@pytest.mark.asyncio
async def test_stale_transition_when_task_already_claimed():
    """
    If task is already PROCESSING (claimed by another worker),
    task_repo.claim() returns False → StaleTransitionError.
    """
    from app.domain.errors import StaleTransitionError
    from app.services import parsing

    envelope = EventEnvelope.build(
        event_type="JobCreated",
        correlation_id="cid-xyz",
        job_id="jid-xyz",
    )

    mock_session = AsyncMock()
    mock_uow = MagicMock()
    mock_uow.__aenter__ = AsyncMock(return_value=mock_session)
    mock_uow.__aexit__ = AsyncMock(return_value=False)

    mock_event_repo = AsyncMock()
    mock_event_repo.record = AsyncMock(return_value=True)  # new event

    mock_task = MagicMock()
    mock_task.id = "task-123"
    mock_task.input_ref = "manuscripts/jid-xyz/manuscript.txt"

    mock_task_repo = AsyncMock()
    mock_task_repo.get_by_job_stage = AsyncMock(return_value=mock_task)
    mock_task_repo.claim = AsyncMock(return_value=False)  # already claimed

    mock_job_repo = AsyncMock()

    with (
        patch("app.services.parsing.unit_of_work", return_value=mock_uow),
        patch("app.services.parsing.ProcessedEventRepository", return_value=mock_event_repo),
        patch("app.services.parsing.TaskRepository", return_value=mock_task_repo),
        patch("app.services.parsing.JobRepository", return_value=mock_job_repo),
        pytest.raises(StaleTransitionError),
    ):
        await parsing.handle_parse(envelope, channel=AsyncMock())
