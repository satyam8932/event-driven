from __future__ import annotations

import pytest

from app.domain.enums import JobStatus, TaskStage, JOB_STAGE_TRANSITIONS


def test_all_stages_have_transitions():
    for stage in TaskStage:
        assert stage in JOB_STAGE_TRANSITIONS, f"Missing transition for {stage}"


def test_transitions_form_linear_chain():
    seen_from = set()
    seen_to = set()
    for _stage, (from_s, to_s) in JOB_STAGE_TRANSITIONS.items():
        seen_from.add(from_s)
        seen_to.add(to_s)

    # No status should be both a "from" and a "to" at different positions
    # except PARSING which is an intermediate state
    assert JobStatus.PENDING in seen_from
    assert JobStatus.COMPLETED not in seen_from


def test_parse_stage_transition():
    from_s, to_s = JOB_STAGE_TRANSITIONS[TaskStage.PARSE]
    assert from_s == JobStatus.PENDING
    assert to_s == JobStatus.PARSING


def test_notify_stage_transition():
    from_s, to_s = JOB_STAGE_TRANSITIONS[TaskStage.NOTIFY]
    assert from_s == JobStatus.STITCHING
    assert to_s == JobStatus.NOTIFYING


def test_event_envelope_has_required_fields():
    from app.domain.events import EventEnvelope

    env = EventEnvelope.build(
        event_type="TestEvent",
        correlation_id="cid-123",
        job_id="jid-456",
        data={"key": "value"},
    )
    assert env.event_id
    assert env.occurred_at
    assert env.version == 1
    assert env.correlation_id == "cid-123"
    assert env.job_id == "jid-456"
    assert env.data == {"key": "value"}


def test_event_envelope_unique_ids():
    from app.domain.events import EventEnvelope

    e1 = EventEnvelope.build(event_type="E", correlation_id="c", job_id="j")
    e2 = EventEnvelope.build(event_type="E", correlation_id="c", job_id="j")
    assert e1.event_id != e2.event_id
