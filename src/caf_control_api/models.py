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


class RunEvent(BaseModel):
    """One persisted P0 run_journal transition, not a full event stream."""

    seq: int
    attempt: int
    event: Literal["claimed", "completed", "quarantined"]
    owner: str | None
    terminal_status: str | None
    created_at: datetime


class QualityGateSummary(BaseModel):
    suite: str
    passed: bool
    exit_code: int | None = None


class ReviewSummary(BaseModel):
    approved: bool
    findings: list[str]


class RunResults(BaseModel):
    """Typed allowlist of current artifact fields; strings are not secret-redacted."""

    run_id: UUID
    status: Literal["queued", "running", "succeeded", "failed", "cancelled"]
    artifact_present: bool
    execution_kind: str | None = None
    reason: str | None = None
    quarantined: bool
    gates: list[QualityGateSummary] | None = None
    review: ReviewSummary | None = None
    commit: str | None = None


def project_results(run: RunStatus) -> RunResults:
    artifact = run.artifact or {}
    raw_gates = artifact.get("gates")
    gates = None
    if isinstance(raw_gates, list):
        valid_gates = []
        for item in raw_gates:
            if not isinstance(item, dict) or not isinstance(item.get("suite"), str) or not isinstance(item.get("passed"), bool):
                continue
            exit_code = item.get("exit_code")
            valid_gates.append(QualityGateSummary(
                suite=item["suite"], passed=item["passed"],
                exit_code=exit_code if type(exit_code) is int else None,
            ))
        gates = valid_gates
    raw_review = artifact.get("review")
    review = None
    if (isinstance(raw_review, dict) and isinstance(raw_review.get("approved"), bool)
            and isinstance(raw_review.get("findings"), list)
            and all(isinstance(item, str) for item in raw_review["findings"])):
        review = ReviewSummary(approved=raw_review["approved"], findings=raw_review["findings"])
    reason = artifact.get("reason")
    return RunResults(
        run_id=run.id, status=run.status, artifact_present=run.artifact is not None,
        execution_kind=artifact.get("execution_kind") if isinstance(artifact.get("execution_kind"), str) else None,
        reason=reason if isinstance(reason, str) else None,
        quarantined=run.status == "failed" and reason == "orphan_unverified",
        gates=gates, review=review,
        commit=artifact.get("commit") if isinstance(artifact.get("commit"), str) else None,
    )


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
