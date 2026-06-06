from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class CreateJobRequest(BaseModel):
    manuscript: str = Field(..., min_length=1, max_length=500_000)

    @field_validator("manuscript")
    @classmethod
    def not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("manuscript cannot be blank")
        return v


class TaskResponse(BaseModel):
    id: str
    stage: str
    status: str
    attempts: int
    error: str | None

    model_config = {"from_attributes": True}


class JobResponse(BaseModel):
    id: str
    status: str
    manuscript_key: str
    final_key: str | None
    correlation_id: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class CreateJobResponse(BaseModel):
    job_id: str
    correlation_id: str
    status: str = "PENDING"


class HealthResponse(BaseModel):
    status: str
    services: dict[str, str]
