from datetime import datetime, timezone
from uuid import uuid4

from fastapi.testclient import TestClient
from psycopg_pool import PoolTimeout

from caf_control_api.app import Settings, create_app
from caf_control_api.models import RunStatus, SubmittedRun
from caf_control_api.storage import IdempotencyConflict


TOKEN = "x" * 32
AUTH = {"Authorization": f"Bearer {TOKEN}"}
BODY = {
    "target_repository": "caf-sample-app",
    "base_revision": "a" * 40,
    "prompt": "Implement discount codes",
    "acceptance_criteria": ["Acceptance test passes"],
    "provider": "codex",
}


class FakeStore:
    def __init__(self):
        self.available = True
        self.submissions = {}

    def ready(self):
        return self.available

    def submit(self, task, key):
        payload = task.model_dump(mode="json")
        if key in self.submissions:
            before, run = self.submissions[key]
            if before != payload:
                raise IdempotencyConflict()
            return run
        run = SubmittedRun(task_id=uuid4(), run_id=uuid4(), correlation_id=uuid4())
        self.submissions[key] = (payload, run)
        return run

    def get_run(self, run_id):
        for _, run in self.submissions.values():
            if run.run_id == run_id:
                now = datetime.now(timezone.utc)
                return RunStatus(id=run_id, task_id=run.task_id,
                                 correlation_id=run.correlation_id, provider="codex",
                                 max_corrections=1, status="queued", attempt=0,
                                 created_at=now, started_at=None, finished_at=None,
                                 updated_at=now, artifact=None)
        return None


def client():
    store = FakeStore()
    app = create_app(Settings(database_url="unused", api_token=TOKEN), store)
    return TestClient(app), store


def test_health_reports_storage_readiness():
    http, store = client()
    assert http.get("/health/live").status_code == 200
    assert http.get("/health/ready").status_code == 200
    store.available = False
    assert http.get("/health/ready").status_code == 503
    assert http.get("/health/live").status_code == 200


def test_intake_requires_auth_and_validates_target():
    http, _ = client()
    assert http.post("/v1/tasks", json=BODY).status_code == 401
    assert http.post("/v1/tasks", json=BODY, headers={"Authorization": "Bearer wrong"}).status_code == 401
    invalid = {**BODY, "target_repository": "../../host"}
    assert http.post("/v1/tasks", json=invalid, headers=AUTH).status_code == 422
    invalid = {**BODY, "base_revision": "main"}
    assert http.post("/v1/tasks", json=invalid, headers=AUTH).status_code == 422


def test_submit_replay_status_and_conflict():
    http, _ = client()
    headers = {**AUTH, "Idempotency-Key": "request-12345"}
    first = http.post("/v1/tasks", json=BODY, headers=headers)
    assert first.status_code == 201
    replay = http.post("/v1/tasks", json=BODY, headers=headers)
    assert replay.status_code == 201
    assert replay.json() == first.json()
    run_id = first.json()["run_id"]
    status = http.get(f"/v1/runs/{run_id}", headers=AUTH)
    assert status.status_code == 200
    assert status.json()["status"] == "queued"
    assert status.json()["correlation_id"] == first.json()["correlation_id"]
    assert http.get(f"/v1/runs/{run_id}").status_code == 401
    assert http.get(f"/v1/runs/{uuid4()}", headers=AUTH).status_code == 404
    changed = {**BODY, "prompt": "Different"}
    assert http.post("/v1/tasks", json=changed, headers=headers).status_code == 409


def test_rejects_invalid_idempotency_key_and_empty_criterion():
    http, _ = client()
    assert http.post("/v1/tasks", json=BODY, headers={**AUTH, "Idempotency-Key": "a"}).status_code == 422
    invalid = {**BODY, "acceptance_criteria": ["  "]}
    assert http.post("/v1/tasks", json=invalid, headers=AUTH).status_code == 422


def test_database_pool_timeout_is_service_unavailable():
    http, store = client()
    def unavailable(task, key):
        raise PoolTimeout("local diagnostic must not be exposed")
    store.submit = unavailable
    response = http.post("/v1/tasks", json=BODY, headers=AUTH)
    assert response.status_code == 503
    assert "local diagnostic" not in response.text
