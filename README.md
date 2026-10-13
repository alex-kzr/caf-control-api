# CAF control API

This independent CAF service accepts one coding task and creates a durable queued run in PostgreSQL. It serves run status, liveness and database readiness. It does **not** execute CLI processes or claim that submitted runs complete without a worker. The umbrella [SPEC](https://github.com/alex-kzr/crew-ai-factory/blob/main/SPEC.md) remains authoritative.

## P0 contract

- `GET /health/live`: process liveness, no database check.
- `GET /health/ready`: verifies access to PostgreSQL and the `runs` table; returns 503 otherwise.
- `POST /v1/tasks`: bearer protected. Body: `target_repository` (slug), immutable `base_revision` (40 or 64 lowercase hex characters), `prompt`, nonempty `acceptance_criteria`, `provider` (`codex` or `claude`), `max_corrections` (0–3, default 1). Returns task, run, correlation IDs and initial status. An optional `Idempotency-Key` (8–128 ASCII letters/digits/`._-`) returns the same run for the same body; changed content returns 409. Task and run insert in one transaction.
- `GET /v1/runs/{run_id}`: bearer protected. Returns status, attempt, timestamps and nullable artifact. Unknown IDs return 404.
- `POST /v1/runs/{run_id}/cancel`: bearer protected and idempotent. A queued Run becomes `cancelled` immediately; a running Run gets `cancel_requested=true` for its owner to observe and terminate. A terminal Run is returned unchanged. The API does not kill a process.

The API never accepts arbitrary filesystem paths or commands. The worker must resolve `target_repository` against a configured allowlist, verify `base_revision` exists, and create runtime worktrees outside tracked submodules. The [worker handoff](docs/worker-contract.md) records the implemented queued-run claim, lease renewal, cancellation signal and guarded completion contract. Expired leases can be quarantined as `failed/orphan_unverified` without automatic retry. This does not prove the child process stopped. Production worker execution remains pending; a queued status means only that the database accepted the run. `blocked` worker artifacts map to API status `failed` while retaining the original artifact details.

## Configuration and local run

Python 3.12+ and uv are required. Set `CAF_DATABASE_URL` to a PostgreSQL DSN (`postgresql://...`) and `CAF_API_TOKEN` to a locally generated secret of at least 32 characters. Keep both outside Git and logs. Run `uv sync --locked`, then `uv run --locked python -m caf_control_api.start`. Startup applies the P0 schema and starts Uvicorn on `0.0.0.0:8000`; bind the host publication to `127.0.0.1` in Compose. Direct local development can use `uv run --locked uvicorn caf_control_api.server:app --host 127.0.0.1 --port 8000` after `uv run --locked python -m caf_control_api.migrate`.

Example body:

```json
{
  "target_repository": "caf-sample-app",
  "base_revision": "0123456789abcdef0123456789abcdef01234567",
  "prompt": "Implement discount code support",
  "acceptance_criteria": ["The acceptance tests pass"],
  "provider": "codex",
  "max_corrections": 1
}
```

## Verification and limits

`uv run --locked --python 3.12 --extra test pytest -q` covers authentication, request validation, idempotent responses, cancellation, status retrieval and readiness behavior with a test store. The opt-in `tests/test_queue_live.py` runs only against a dedicated `caf_queue_test_*` database and verifies concurrent claim, lease, cancellation and expired-Run quarantine. The umbrella's API/PostgreSQL Compose slice has also passed live readiness, authenticated intake and normal group stop/start. These checks do **not** prove a production worker process, CLI process-tree cleanup, resume or complete SPEC core. The P0 schema is intentionally small (`tasks`, `runs`); other SPEC entities, audit events, streaming, checkpoint recovery, and schema migration versioning remain future work.
