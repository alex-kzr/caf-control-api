"""One bounded worker grant; no implicit all-queue credential scope."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from uuid import UUID


@dataclass(frozen=True)
class WorkerGrant:
    token: str
    identity: str
    database: str
    run_id: UUID
    task_id: UUID
    repository: str
    workspace_id: str
    base_revision: str
    provider: str
    expires_at: datetime

    def __post_init__(self) -> None:
        if len(self.token) < 32:
            raise ValueError("worker token must be at least 32 characters")
        if not re.fullmatch(r"[A-Za-z0-9._-]{8,128}", self.identity):
            raise ValueError("invalid worker identity")
        if not re.fullmatch(r"caf_queue_test_[A-Za-z0-9_]+", self.database):
            raise ValueError("worker database must be an explicit isolated test database")
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", self.repository):
            raise ValueError("invalid worker repository scope")
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", self.workspace_id):
            raise ValueError("invalid worker workspace scope")
        if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", self.base_revision):
            raise ValueError("invalid worker base revision scope")
        if self.provider not in ("codex", "claude"):
            raise ValueError("invalid worker provider scope")
        if self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None:
            raise ValueError("worker grant expiry must include a timezone")
        if self.expires_at - datetime.now(timezone.utc) > timedelta(hours=1):
            raise ValueError("worker grant lifetime exceeds one hour")

    def active(self) -> bool:
        return datetime.now(timezone.utc) < self.expires_at
