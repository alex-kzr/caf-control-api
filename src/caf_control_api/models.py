"""Public P0 request and response contract."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class CreateTask(BaseModel):
    target_repository: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    base_revision: str = Field(pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
    prompt: str = Field(min_length=1, max_length=20_000)
    acceptance_criteria: list[str] = Field(min_length=1, max_length=20)
    provider: Literal["codex", "claude"]
    max_corrections: int = Field(default=1, ge=0, le=3)

    @field_validator("acceptance_criteria")
    @classmethod
    def nonempty_criteria(cls, criteria: list[str]) -> list[str]:
        if any(not item.strip() or len(item) > 2000 for item in criteria):
            raise ValueError("Each acceptance criterion must contain 1-2000 characters")
        return criteria


class SubmittedRun(BaseModel):
    task_id: UUID
    run_id: UUID
    correlation_id: UUID
    status: Literal["queued", "running", "succeeded", "failed", "cancelled"] = "queued"


class RunStatus(BaseModel):
    id: UUID
    task_id: UUID
    correlation_id: UUID
    provider: Literal["codex", "claude"]
    max_corrections: int
    status: Literal["queued", "running", "succeeded", "failed", "cancelled"]
    attempt: int
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    updated_at: datetime
    artifact: dict | None
