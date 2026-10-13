"""Local-only authenticated control API. No CLI processes run here."""

import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from hmac import compare_digest
from uuid import UUID

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from psycopg_pool import PoolTimeout

from .models import (
    CreateTask, RunEvent, RunResults, RunStatus, SubmittedRun, WorkerClaim, WorkerClaimRequest,
    WorkerCompleted, WorkerCompletion, WorkerControl, WorkerOwner, WorkerRenewed,
    project_results,
)
from .storage import IdempotencyConflict, PostgresStore, Store
from .worker_grant import WorkerGrant


@dataclass(frozen=True)
class Settings:
    database_url: str
    api_token: str
    worker_grant: WorkerGrant | None = None

    def __post_init__(self) -> None:
        if self.worker_grant is not None and self.worker_grant.token == self.api_token:
            raise ValueError("worker token must be distinct from API token")

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.environ["CAF_API_TOKEN"]
        if len(token) < 32:
            raise ValueError("CAF_API_TOKEN must contain at least 32 characters")
        worker_fields = ("CAF_WORKER_TOKEN", "CAF_WORKER_ID", "CAF_WORKER_DATABASE",
                         "CAF_WORKER_RUN_ID", "CAF_WORKER_TASK_ID", "CAF_WORKER_REPOSITORY",
                         "CAF_WORKER_WORKSPACE_ID",
                         "CAF_WORKER_BASE_REVISION", "CAF_WORKER_PROVIDER",
                         "CAF_WORKER_EXPIRES_AT")
        supplied = {key: os.getenv(key) for key in worker_fields}
        if any(supplied.values()) and not all(supplied.values()):
            raise ValueError("worker grant requires every scope and expiry field")
        grant = None
        if all(supplied.values()):
            try:
                grant = WorkerGrant(
                    token=supplied["CAF_WORKER_TOKEN"], identity=supplied["CAF_WORKER_ID"],
                    database=supplied["CAF_WORKER_DATABASE"],
                    run_id=UUID(supplied["CAF_WORKER_RUN_ID"]),
                    task_id=UUID(supplied["CAF_WORKER_TASK_ID"]),
                    repository=supplied["CAF_WORKER_REPOSITORY"],
                    workspace_id=supplied["CAF_WORKER_WORKSPACE_ID"],
                    base_revision=supplied["CAF_WORKER_BASE_REVISION"],
                    provider=supplied["CAF_WORKER_PROVIDER"],
                    expires_at=datetime.fromisoformat(supplied["CAF_WORKER_EXPIRES_AT"]),
                )
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid worker grant configuration") from exc
        return cls(database_url=os.environ["CAF_DATABASE_URL"], api_token=token,
                   worker_grant=grant)


def create_app(settings: Settings | None = None, store: Store | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    store = store or PostgresStore(settings.database_url)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if isinstance(store, PostgresStore):
            store.open()
        try:
            yield
        finally:
            if isinstance(store, PostgresStore):
                store.close()

    app = FastAPI(title="CAF control API", version="0.1.0", lifespan=lifespan)

    def authorized(authorization: str | None = Header(default=None)) -> None:
        scheme, _, credential = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not compare_digest(
            credential.encode("utf-8"), settings.api_token.encode("utf-8")
        ):
            raise HTTPException(status_code=401, detail="Invalid bearer token")

    def authorized_worker(authorization: str | None = Header(default=None)) -> None:
        grant = settings.worker_grant
        if grant is None or not grant.active():
            raise HTTPException(status_code=503, detail="Worker transport disabled")
        scheme, _, credential = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not compare_digest(
            credential.encode("utf-8"), grant.token.encode("utf-8")
        ):
            raise HTTPException(status_code=401, detail="Invalid worker bearer token")
        if store.database_name() != grant.database:
            raise HTTPException(status_code=503, detail="Worker database scope unavailable")

    def worker_identity(owner: str) -> WorkerGrant:
        grant = settings.worker_grant
        assert grant is not None
        if owner != grant.identity:
            raise HTTPException(status_code=403, detail="Worker identity mismatch")
        return grant

    def scoped_run(run_id: UUID) -> WorkerGrant:
        grant = settings.worker_grant
        assert grant is not None
        if run_id != grant.run_id or not store.run_in_scope(grant):
            raise HTTPException(status_code=404, detail="Run not found")
        return grant

    @app.exception_handler(psycopg.Error)
    async def database_error(_, __):
        return JSONResponse(status_code=503, content={"detail": "Database unavailable"})

    @app.exception_handler(PoolTimeout)
    async def pool_timeout(_, __):
        return JSONResponse(status_code=503, content={"detail": "Database unavailable"})

    @app.get("/health/live")
    def live():
        return {"status": "alive"}

    @app.get("/health/ready")
    def ready():
        if not store.ready():
            raise HTTPException(status_code=503, detail="Database or schema unavailable")
        return {"status": "ready"}

    @app.post("/v1/tasks", response_model=SubmittedRun, status_code=201,
              dependencies=[Depends(authorized)])
    def submit(task: CreateTask, idempotency_key: str | None = Header(default=None)):
        if idempotency_key is not None and not re.fullmatch(r"[A-Za-z0-9._-]{8,128}", idempotency_key):
            raise HTTPException(status_code=422, detail="Invalid Idempotency-Key")
        try:
            return store.submit(task, idempotency_key)
        except IdempotencyConflict:
            raise HTTPException(status_code=409, detail="Idempotency-Key content conflict") from None

    @app.get("/v1/runs/{run_id}", response_model=RunStatus,
             dependencies=[Depends(authorized)])
    def get_run(run_id: UUID):
        result = store.get_run(run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return result

    @app.get("/v1/runs/{run_id}/events", response_model=list[RunEvent],
             dependencies=[Depends(authorized)])
    def get_run_events(run_id: UUID, after_seq: int = Query(default=0, ge=0),
                       limit: int = Query(default=100, ge=1, le=100)):
        if store.get_run(run_id) is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return store.get_run_events(run_id, after_seq, limit)

    @app.get("/v1/runs/{run_id}/results", response_model=RunResults,
             dependencies=[Depends(authorized)])
    def get_run_results(run_id: UUID):
        result = store.get_run(run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return project_results(result)

    @app.post("/v1/runs/{run_id}/cancel", response_model=RunStatus,
              dependencies=[Depends(authorized)])
    def cancel_run(run_id: UUID):
        result = store.request_cancel(run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return result

    @app.get("/v1/worker/database", response_model=str,
             dependencies=[Depends(authorized_worker)])
    def worker_database():
        return settings.worker_grant.database

    @app.post("/v1/worker/quarantine-expired", response_model=list[UUID],
              dependencies=[Depends(authorized_worker)])
    def worker_quarantine():
        return store.quarantine_expired_scoped(settings.worker_grant)

    @app.post("/v1/worker/claim", response_model=WorkerClaim | None,
              dependencies=[Depends(authorized_worker)])
    def worker_claim(request: WorkerClaimRequest):
        grant = worker_identity(request.owner)
        return store.claim_scoped(grant, request.lease_seconds)

    @app.post("/v1/worker/runs/{run_id}/control", response_model=WorkerControl,
              dependencies=[Depends(authorized_worker)])
    def worker_control(run_id: UUID, request: WorkerOwner):
        worker_identity(request.owner)
        scoped_run(run_id)
        valid, cancelled = store.claim_control(run_id, request.owner)
        return WorkerControl(valid=valid, cancelled=cancelled)

    @app.post("/v1/worker/runs/{run_id}/renew", response_model=WorkerRenewed,
              dependencies=[Depends(authorized_worker)])
    def worker_renew(run_id: UUID, request: WorkerClaimRequest):
        worker_identity(request.owner)
        scoped_run(run_id)
        return WorkerRenewed(renewed=store.renew_lease(run_id, request.owner,
                                                       request.lease_seconds))

    @app.post("/v1/worker/runs/{run_id}/complete", response_model=WorkerCompleted,
              dependencies=[Depends(authorized_worker)])
    def worker_complete(run_id: UUID, request: WorkerCompletion):
        worker_identity(request.owner)
        scoped_run(run_id)
        return WorkerCompleted(completed=store.complete_claim(
            run_id, request.owner, request.status, request.artifact,
        ))

    @app.get("/v1/worker/runs/{run_id}", response_model=RunStatus,
             dependencies=[Depends(authorized_worker)])
    def worker_get_run(run_id: UUID):
        scoped_run(run_id)
        result = store.get_run(run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return result

    return app
