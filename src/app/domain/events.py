from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class EventEnvelope(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str
    job_id: str
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    version: int = 1
    event_type: str
    data: dict[str, Any] = Field(default_factory=dict)

    def model_dump_json_bytes(self) -> bytes:
        return self.model_dump_json().encode()

    @classmethod
    def build(
        cls,
        *,
        event_type: str,
        correlation_id: str,
        job_id: str,
        data: dict[str, Any] | None = None,
    ) -> EventEnvelope:
        return cls(
            event_type=event_type,
            correlation_id=correlation_id,
            job_id=job_id,
            data=data or {},
        )


class JobCreatedData(BaseModel):
    manuscript_key: str


class ParseCompletedData(BaseModel):
    parsed_key: str
    block_count: int


class TtsCompletedData(BaseModel):
    audio_keys: list[str]


class StitchCompletedData(BaseModel):
    final_key: str


class NotifyCompletedData(BaseModel):
    webhook_url: str | None = None
    status: str = "notified"
