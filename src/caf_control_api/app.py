"""Local-only authenticated control API. No CLI processes run here."""

import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from hmac import compare_digest
from uuid import UUID

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from psycopg_pool import PoolTimeout

from .models import (
    CreateTask, RunStatus, SubmittedRun, WorkerClaim, WorkerClaimRequest,
    WorkerCompleted, WorkerCompletion, WorkerControl, WorkerOwner, WorkerRenewed,
)
from .storage import IdempotencyConflict, PostgresStore, Store


@dataclass(frozen=True)
class Settings:
    database_url: str
    api_token: str
    worker_token: str | None = None

    def __post_init__(self) -> None:
        if self.worker_token is not None and (
            len(self.worker_token) < 32 or self.worker_token == self.api_token
        ):
            raise ValueError("worker token must be distinct and at least 32 characters")

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.environ["CAF_API_TOKEN"]
        if len(token) < 32:
            raise ValueError("CAF_API_TOKEN must contain at least 32 characters")
        worker_token = os.getenv("CAF_WORKER_TOKEN") or None
        if worker_token is not None and (len(worker_token) < 32 or worker_token == token):
            raise ValueError("CAF_WORKER_TOKEN must be distinct and at least 32 characters")
        return cls(database_url=os.environ["CAF_DATABASE_URL"], api_token=token,
                   worker_token=worker_token)


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
        if settings.worker_token is None:
            raise HTTPException(status_code=503, detail="Worker transport disabled")
        scheme, _, credential = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not compare_digest(
            credential.encode("utf-8"), settings.worker_token.encode("utf-8")
        ):
            raise HTTPException(status_code=401, detail="Invalid worker bearer token")

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
        return store.database_name()

    @app.post("/v1/worker/quarantine-expired", response_model=list[UUID],
              dependencies=[Depends(authorized_worker)])
    def worker_quarantine():
        return store.quarantine_expired()

    @app.post("/v1/worker/claim", response_model=WorkerClaim | None,
              dependencies=[Depends(authorized_worker)])
    def worker_claim(request: WorkerClaimRequest):
        return store.claim_queued(request.owner, request.lease_seconds)

    @app.post("/v1/worker/runs/{run_id}/control", response_model=WorkerControl,
              dependencies=[Depends(authorized_worker)])
    def worker_control(run_id: UUID, request: WorkerOwner):
        valid, cancelled = store.claim_control(run_id, request.owner)
        return WorkerControl(valid=valid, cancelled=cancelled)

    @app.post("/v1/worker/runs/{run_id}/renew", response_model=WorkerRenewed,
              dependencies=[Depends(authorized_worker)])
    def worker_renew(run_id: UUID, request: WorkerClaimRequest):
        return WorkerRenewed(renewed=store.renew_lease(run_id, request.owner,
                                                       request.lease_seconds))

    @app.post("/v1/worker/runs/{run_id}/complete", response_model=WorkerCompleted,
              dependencies=[Depends(authorized_worker)])
    def worker_complete(run_id: UUID, request: WorkerCompletion):
        return WorkerCompleted(completed=store.complete_claim(
            run_id, request.owner, request.status, request.artifact,
        ))

    @app.get("/v1/worker/runs/{run_id}", response_model=RunStatus,
             dependencies=[Depends(authorized_worker)])
    def worker_get_run(run_id: UUID):
        result = store.get_run(run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Run not found")
        return result

    return app
