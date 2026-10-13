"""Public P0 request and response contract."""

from datetime import datetime
import json
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class CreateTask(BaseModel):
    target_repository: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
    workspace_id: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9-]{0,62}$")
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
    cancel_requested: bool = False
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    updated_at: datetime
    artifact: dict | None


class WorkerOwner(BaseModel):
    owner: str = Field(pattern=r"^[A-Za-z0-9._-]{8,128}$")


class WorkerClaimRequest(WorkerOwner):
    lease_seconds: int = Field(default=60, ge=15, le=3600)


class WorkerClaim(BaseModel):
    id: UUID
    task_id: UUID
    correlation_id: UUID
    target_repository: str
    workspace_id: str | None
    base_revision: str
    prompt: str
    acceptance_criteria: list[str]
    provider: Literal["codex", "claude"]
    max_corrections: int
    attempt: int


class WorkerControl(BaseModel):
    valid: bool
    cancelled: bool


class WorkerRenewed(BaseModel):
    renewed: bool


class WorkerCompleted(BaseModel):
    completed: bool


class WorkerCompletion(WorkerOwner):
    status: Literal["succeeded", "failed", "cancelled"]
    artifact: dict

    @field_validator("artifact")
    @classmethod
    def bounded_artifact(cls, artifact: dict) -> dict:
        if len(json.dumps(artifact).encode("utf-8")) > 131_072:
            raise ValueError("artifact exceeds 128 KiB")
        return artifact
