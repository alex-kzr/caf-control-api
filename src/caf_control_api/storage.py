"""PostgreSQL-backed task intake and durable run state."""

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID, uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .models import CreateTask, RunStatus, SubmittedRun
from .worker_grant import WorkerGrant


class IdempotencyConflict(Exception):
    """The same key was reused with different task content."""


@dataclass(frozen=True, slots=True)
class ClaimedRun:
    id: UUID
    task_id: UUID
    correlation_id: UUID
    target_repository: str
    base_revision: str
    prompt: str
    acceptance_criteria: list[str]
    provider: str
    max_corrections: int
    attempt: int
    workspace_id: str | None = None


class Store(Protocol):
    def ready(self) -> bool: ...
    def submit(self, task: CreateTask, key: str | None) -> SubmittedRun: ...
    def get_run(self, run_id: UUID) -> RunStatus | None: ...
    def request_cancel(self, run_id: UUID) -> RunStatus | None: ...
    def database_name(self) -> str: ...
    def quarantine_expired(self) -> list[UUID]: ...
    def claim_queued(self, owner: str, lease_seconds: int = 60) -> ClaimedRun | None: ...
    def claim_control(self, run_id: UUID, owner: str) -> tuple[bool, bool]: ...
    def renew_lease(self, run_id: UUID, owner: str, lease_seconds: int = 60) -> bool: ...
    def complete_claim(self, run_id: UUID, owner: str, status: str, artifact: dict) -> bool: ...
    def run_in_scope(self, grant: WorkerGrant) -> bool: ...
    def claim_scoped(self, grant: WorkerGrant, lease_seconds: int = 60) -> ClaimedRun | None: ...
    def quarantine_expired_scoped(self, grant: WorkerGrant) -> list[UUID]: ...


class PostgresStore:
    def __init__(self, dsn: str) -> None:
        self.pool = ConnectionPool(dsn, open=False, min_size=1, max_size=5, timeout=3)

    def open(self) -> None:
        self.pool.open(wait=False)

    def close(self) -> None:
        self.pool.close()

    def ready(self) -> bool:
        try:
            with self.pool.connection() as conn:
                conn.execute("SELECT id FROM runs LIMIT 0")
            return True
        except Exception:
            return False

    def database_name(self) -> str:
        """Return the actual connected database for test-only safety guards."""
        with self.pool.connection() as conn:
            return conn.execute("SELECT current_database()").fetchone()[0]

    def submit(self, task: CreateTask, key: str | None) -> SubmittedRun:
        payload = task.model_dump(mode="json")
        request_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        task_id, run_id, correlation_id = uuid4(), uuid4(), uuid4()
        with self.pool.connection() as conn, conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """INSERT INTO tasks
                    (id, target_repository, workspace_id, base_revision, prompt, acceptance_criteria,
                     idempotency_key, request_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (idempotency_key) DO NOTHING RETURNING id""",
                    (task_id, task.target_repository, task.workspace_id, task.base_revision, task.prompt,
                     Jsonb(task.acceptance_criteria), key, request_hash),
                )
                if cur.fetchone() is None:
                    cur.execute(
                        """SELECT t.id AS task_id, t.request_hash, r.id AS run_id,
                                  r.correlation_id, r.status
                           FROM tasks t JOIN runs r ON r.task_id = t.id
                           WHERE t.idempotency_key = %s""",
                        (key,),
                    )
                    existing = cur.fetchone()
                    if existing is None or existing["request_hash"] != request_hash:
                        raise IdempotencyConflict()
                    return SubmittedRun(**existing)
                cur.execute(
                    """INSERT INTO runs
                    (id, task_id, correlation_id, provider, max_corrections)
                    VALUES (%s, %s, %s, %s, %s)""",
                    (run_id, task_id, correlation_id, task.provider, task.max_corrections),
                )
        return SubmittedRun(task_id=task_id, run_id=run_id, correlation_id=correlation_id)

    def get_run(self, run_id: UUID) -> RunStatus | None:
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """SELECT id, task_id, correlation_id, provider, max_corrections,
                              status, attempt, cancel_requested, created_at, started_at, finished_at,
                              updated_at, artifact
                       FROM runs WHERE id = %s""",
                    (run_id,),
                )
                row = cur.fetchone()
        return RunStatus(**row) if row else None

    def request_cancel(self, run_id: UUID) -> RunStatus | None:
        """Cancel queued work immediately, or signal its current worker."""
        with self.pool.connection() as conn, conn.transaction():
            conn.execute(
                """UPDATE runs SET cancel_requested = true,
                          status = CASE WHEN status = 'queued' THEN 'cancelled' ELSE status END,
                          finished_at = CASE WHEN status = 'queued' THEN now() ELSE finished_at END,
                          updated_at = now()
                   WHERE id = %s AND status IN ('queued', 'running')""",
                (run_id,),
            )
        return self.get_run(run_id)

    def claim_control(self, run_id: UUID, owner: str) -> tuple[bool, bool]:
        """Return ownership and cancellation signal for a live lease."""
        self._lease_args(owner, 60)
        with self.pool.connection() as conn:
            row = conn.execute(
                """SELECT lease_expires_at > now(), cancel_requested FROM runs
                   WHERE id = %s AND status = 'running' AND lease_owner = %s""",
                (run_id, owner),
            ).fetchone()
        return (bool(row[0]), bool(row[1])) if row else (False, False)

    def quarantine_expired(self) -> list[UUID]:
        """Stop redispatch of expired work; an orphan CLI may still exist."""
        with self.pool.connection() as conn, conn.transaction():
            rows = conn.execute(
                """UPDATE runs SET status = 'failed',
                          artifact = jsonb_build_object('reason', 'orphan_unverified',
                                                        'attempt', attempt),
                          finished_at = now(), updated_at = now(),
                          lease_owner = NULL, lease_expires_at = NULL
                   WHERE status = 'running' AND lease_expires_at <= now()
                   RETURNING id, attempt"""
            ).fetchall()
            for run_id, attempt in rows:
                conn.execute(
                    """INSERT INTO run_journal (run_id, attempt, event, terminal_status)
                       VALUES (%s, %s, 'quarantined', 'failed')""",
                    (run_id, attempt),
                )
        return [row[0] for row in rows]

    def run_in_scope(self, grant: WorkerGrant) -> bool:
        with self.pool.connection() as conn:
            row = conn.execute(
                """SELECT 1 FROM runs r JOIN tasks t ON t.id = r.task_id
                   WHERE r.id = %s AND r.task_id = %s AND t.target_repository = %s
                     AND t.workspace_id = %s
                     AND t.base_revision = %s AND r.provider = %s""",
                (grant.run_id, grant.task_id, grant.repository, grant.workspace_id,
                 grant.base_revision, grant.provider),
            ).fetchone()
        return row is not None

    def claim_scoped(self, grant: WorkerGrant, lease_seconds: int = 60) -> ClaimedRun | None:
        """Claim only the configured Run; a different queued Run is untouched."""
        self._lease_args(grant.identity, lease_seconds)
        with self.pool.connection() as conn, conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """UPDATE runs AS r
                       SET status = 'running', lease_owner = %s,
                           lease_expires_at = now() + (%s * interval '1 second'),
                           started_at = COALESCE(started_at, now()),
                           updated_at = now(), attempt = attempt + 1
                       FROM tasks t
                       WHERE r.id = %s AND r.task_id = %s AND t.id = r.task_id
                         AND t.target_repository = %s AND t.workspace_id = %s
                         AND t.base_revision = %s
                         AND r.provider = %s AND r.status = 'queued'
                         AND NOT r.cancel_requested
                       RETURNING r.id, r.task_id, r.correlation_id,
                                 t.target_repository, t.workspace_id, t.base_revision, t.prompt,
                                 t.acceptance_criteria, r.provider,
                                 r.max_corrections, r.attempt""",
                    (grant.identity, lease_seconds, grant.run_id, grant.task_id,
                     grant.repository, grant.workspace_id, grant.base_revision, grant.provider),
                )
                row = cur.fetchone()
                if row is not None:
                    cur.execute(
                        """INSERT INTO run_journal (run_id, attempt, event, owner)
                           VALUES (%s, %s, 'claimed', %s)""",
                        (row["id"], row["attempt"], grant.identity),
                    )
        return ClaimedRun(**row) if row else None

    def quarantine_expired_scoped(self, grant: WorkerGrant) -> list[UUID]:
        with self.pool.connection() as conn, conn.transaction():
            rows = conn.execute(
                """UPDATE runs AS r SET status = 'failed',
                          artifact = jsonb_build_object('reason', 'orphan_unverified',
                                                        'attempt', r.attempt),
                          finished_at = now(), updated_at = now(),
                          lease_owner = NULL, lease_expires_at = NULL
                   FROM tasks t
                   WHERE r.id = %s AND r.task_id = %s AND t.id = r.task_id
                     AND t.target_repository = %s AND t.workspace_id = %s
                     AND t.base_revision = %s
                     AND r.provider = %s AND r.lease_owner = %s
                     AND r.status = 'running' AND r.lease_expires_at <= now()
                   RETURNING r.id, r.attempt""",
                (grant.run_id, grant.task_id, grant.repository, grant.workspace_id,
                 grant.base_revision, grant.provider, grant.identity),
            ).fetchall()
            for run_id, attempt in rows:
                conn.execute(
                    """INSERT INTO run_journal (run_id, attempt, event, terminal_status)
                       VALUES (%s, %s, 'quarantined', 'failed')""",
                    (run_id, attempt),
                )
        return [row[0] for row in rows]

    @staticmethod
    def _lease_args(owner: str, lease_seconds: int) -> None:
        if not re.fullmatch(r"[A-Za-z0-9._-]{8,128}", owner):
            raise ValueError("owner must be a stable 8-128 character identifier")
        if not isinstance(lease_seconds, int) or not 15 <= lease_seconds <= 3600:
            raise ValueError("lease_seconds must be an integer from 15 to 3600")

    def claim_queued(self, owner: str, lease_seconds: int = 60) -> ClaimedRun | None:
        """Claim one queued run; expired running leases are not reclaimed yet."""
        self._lease_args(owner, lease_seconds)
        with self.pool.connection() as conn, conn.transaction():
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """WITH candidate AS (
                           SELECT id FROM runs WHERE status = 'queued' AND NOT cancel_requested
                           ORDER BY created_at, id FOR UPDATE SKIP LOCKED LIMIT 1
                       )
                       UPDATE runs AS r
                       SET status = 'running', lease_owner = %s,
                           lease_expires_at = now() + (%s * interval '1 second'),
                           started_at = COALESCE(started_at, now()),
                           updated_at = now(), attempt = attempt + 1
                       FROM candidate c, tasks t
                       WHERE r.id = c.id AND t.id = r.task_id
                       RETURNING r.id, r.task_id, r.correlation_id,
                                 t.target_repository, t.workspace_id, t.base_revision, t.prompt,
                                 t.acceptance_criteria, r.provider,
                                 r.max_corrections, r.attempt""",
                    (owner, lease_seconds),
                )
                row = cur.fetchone()
                if row is not None:
                    cur.execute(
                        """INSERT INTO run_journal (run_id, attempt, event, owner)
                           VALUES (%s, %s, 'claimed', %s)""",
                        (row["id"], row["attempt"], owner),
                    )
        return ClaimedRun(**row) if row else None

    def renew_lease(self, run_id: UUID, owner: str, lease_seconds: int = 60) -> bool:
        self._lease_args(owner, lease_seconds)
        with self.pool.connection() as conn, conn.transaction():
            result = conn.execute(
                """UPDATE runs SET lease_expires_at = now() + (%s * interval '1 second'),
                          updated_at = now()
                   WHERE id = %s AND status = 'running' AND lease_owner = %s
                     AND lease_expires_at > now()""",
                (lease_seconds, run_id, owner),
            )
            return result.rowcount == 1

    def complete_claim(self, run_id: UUID, owner: str, status: str, artifact: dict) -> bool:
        if status not in {"succeeded", "failed", "cancelled"}:
            raise ValueError("status must be terminal")
        self._lease_args(owner, 60)
        json.dumps(artifact)
        with self.pool.connection() as conn, conn.transaction():
            result = conn.execute(
                """UPDATE runs SET status = %s, artifact = %s,
                          finished_at = now(), updated_at = now(),
                          lease_owner = NULL, lease_expires_at = NULL
                   WHERE id = %s AND status = 'running' AND lease_owner = %s
                     AND lease_expires_at > now() AND (NOT cancel_requested OR %s = 'cancelled')""",
                (status, Jsonb(artifact), run_id, owner, status),
            )
            if result.rowcount == 1:
                conn.execute(
                    """INSERT INTO run_journal (run_id, attempt, event, owner, terminal_status)
                       SELECT id, attempt, 'completed', %s, %s FROM runs WHERE id = %s""",
                    (owner, status, run_id),
                )
            return result.rowcount == 1
