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

from .models import CreateTask, RunStatus, SubmittedRun
from .storage import IdempotencyConflict, PostgresStore, Store


@dataclass(frozen=True)
class Settings:
    database_url: str
    api_token: str

    @classmethod
    def from_env(cls) -> "Settings":
        token = os.environ["CAF_API_TOKEN"]
        if len(token) < 32:
            raise ValueError("CAF_API_TOKEN must contain at least 32 characters")
        return cls(database_url=os.environ["CAF_DATABASE_URL"], api_token=token)


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

    return app
