"""PostgreSQL-backed task intake and durable run state."""

import hashlib
import json
from typing import Protocol
from uuid import UUID, uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .models import CreateTask, RunStatus, SubmittedRun


class IdempotencyConflict(Exception):
    """The same key was reused with different task content."""


class Store(Protocol):
    def ready(self) -> bool: ...
    def submit(self, task: CreateTask, key: str | None) -> SubmittedRun: ...
    def get_run(self, run_id: UUID) -> RunStatus | None: ...


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
                    (id, target_repository, base_revision, prompt, acceptance_criteria,
                     idempotency_key, request_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (idempotency_key) DO NOTHING RETURNING id""",
                    (task_id, task.target_repository, task.base_revision, task.prompt,
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
                              status, attempt, created_at, started_at, finished_at,
                              updated_at, artifact
                       FROM runs WHERE id = %s""",
                    (run_id,),
                )
                row = cur.fetchone()
        return RunStatus(**row) if row else None
