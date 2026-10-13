from datetime import datetime, timedelta, timezone
from dataclasses import replace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from psycopg_pool import PoolTimeout

from caf_control_api.app import Settings, create_app
from caf_control_api.models import RunStatus, SubmittedRun
from caf_control_api.storage import ClaimedRun, IdempotencyConflict
from caf_control_api.worker_grant import WorkerGrant
from caf_control_api.models import CreateTask


TOKEN = "x" * 32
AUTH = {"Authorization": f"Bearer {TOKEN}"}
WORKER_TOKEN = "w" * 32
WORKER_AUTH = {"Authorization": f"Bearer {WORKER_TOKEN}"}
BODY = {
    "target_repository": "caf-sample-app",
    "workspace_id": "caf-sample-fixture-v1",
    "base_revision": "a" * 40,
    "prompt": "Implement discount codes",
    "acceptance_criteria": ["Acceptance test passes"],
    "provider": "codex",
}


class FakeStore:
    def __init__(self):
        self.available = True
        self.submissions = {}
        self.cancelled = set()
        self.worker_owner = None
        self.worker_completed = None

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
                                 max_corrections=1,
                                 status="cancelled" if run_id in self.cancelled else "queued",
                                 cancel_requested=run_id in self.cancelled, attempt=0,
                                 created_at=now, started_at=None, finished_at=None,
                                 updated_at=now, artifact=None)
        return None

    def request_cancel(self, run_id):
        status = self.get_run(run_id)
        if status is not None:
            self.cancelled.add(run_id)
        return self.get_run(run_id)

    def database_name(self):
        return "caf_queue_test_unit"

    def quarantine_expired(self):
        return []

    def quarantine_expired_scoped(self, grant):
        return []

    def run_in_scope(self, grant):
        return (grant.run_id == self.scoped_run.run_id
                and grant.task_id == self.scoped_run.task_id
                and grant.repository == "caf-sample-app"
                and grant.workspace_id == "caf-sample-fixture-v1"
                and grant.provider == "codex" and grant.base_revision == "a" * 40)

    def claim_scoped(self, grant, lease_seconds=60):
        if not self.run_in_scope(grant):
            return None
        return self.claim_queued(grant.identity, lease_seconds)

    def claim_queued(self, owner, lease_seconds=60):
        if self.worker_owner is not None or not self.submissions:
            return None
        self.worker_owner = owner
        _, run = next(iter(self.submissions.values()))
        return ClaimedRun(run.run_id, run.task_id, run.correlation_id,
                          "caf-sample-app", "a" * 40, "Fixture", ["Pass"],
                          "codex", 1, 1)

    def claim_control(self, run_id, owner):
        return self.worker_owner == owner, run_id in self.cancelled

    def renew_lease(self, run_id, owner, lease_seconds=60):
        return self.worker_owner == owner

    def complete_claim(self, run_id, owner, status, artifact):
        if self.worker_owner != owner:
            return False
        self.worker_completed = (run_id, status, artifact)
        return True


def client(worker_token=None):
    store = FakeStore()
    grant = None
    if worker_token is not None:
        store.scoped_run = store.submit(CreateTask(**BODY), None)
        grant = WorkerGrant(worker_token, "worker-12345", "caf_queue_test_unit",
                            store.scoped_run.run_id, store.scoped_run.task_id,
                            "caf-sample-app", "caf-sample-fixture-v1", "a" * 40, "codex",
                            datetime.now(timezone.utc) + timedelta(hours=1))
    app = create_app(Settings(database_url="unused", api_token=TOKEN,
                              worker_grant=grant), store)
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
    assert http.post("/v1/tasks", json=BODY,
                     headers={b"Authorization": b"Bearer caf\xe9"}).status_code == 401
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


def test_cancel_requires_auth_and_is_idempotent():
    http, _ = client()
    run_id = http.post("/v1/tasks", json=BODY, headers=AUTH).json()["run_id"]
    assert http.post(f"/v1/runs/{run_id}/cancel").status_code == 401
    assert http.post(f"/v1/runs/{uuid4()}/cancel", headers=AUTH).status_code == 404
    first = http.post(f"/v1/runs/{run_id}/cancel", headers=AUTH)
    second = http.post(f"/v1/runs/{run_id}/cancel", headers=AUTH)
    assert first.status_code == second.status_code == 200
    assert first.json()["status"] == second.json()["status"] == "cancelled"
    assert first.json()["cancel_requested"] is True


def test_worker_transport_reuses_auth_and_guards_owner_and_artifact():
    disabled, _ = client()
    assert disabled.get("/v1/worker/database", headers=WORKER_AUTH).status_code == 503
    http, store = client(WORKER_TOKEN)
    run_id = str(store.scoped_run.run_id)
    owner = "worker-12345"
    assert http.post("/v1/worker/claim", json={"owner": owner}).status_code == 401
    assert http.get("/v1/worker/database", headers=AUTH).status_code == 401
    assert http.post("/v1/tasks", json=BODY, headers=WORKER_AUTH).status_code == 401
    assert http.get("/v1/worker/database", headers=WORKER_AUTH).json() == "caf_queue_test_unit"
    assert http.post("/v1/worker/claim", json={"owner": "bad"}, headers=WORKER_AUTH).status_code == 422
    claimed = http.post("/v1/worker/claim", json={"owner": owner}, headers=WORKER_AUTH)
    assert claimed.status_code == 200 and claimed.json()["id"] == run_id
    assert http.post("/v1/worker/claim", json={"owner": owner}, headers=WORKER_AUTH).json() is None
    assert http.post(f"/v1/worker/runs/{run_id}/control",
                     json={"owner": "other-worker"}, headers=WORKER_AUTH).status_code == 403
    assert http.get(f"/v1/worker/runs/{uuid4()}", headers=WORKER_AUTH).status_code == 404
    assert http.post(f"/v1/worker/runs/{run_id}/renew",
                     json={"owner": owner, "lease_seconds": 60}, headers=WORKER_AUTH).json() == {
                         "renewed": True}
    assert http.post(f"/v1/worker/runs/{run_id}/complete",
                     json={"owner": owner, "status": "succeeded",
                           "artifact": {"large": "x" * 140_000}}, headers=WORKER_AUTH).status_code == 422
    completed = http.post(f"/v1/worker/runs/{run_id}/complete",
                          json={"owner": owner, "status": "succeeded",
                                "artifact": {"kind": "synthetic"}}, headers=WORKER_AUTH)
    assert completed.json() == {"completed": True}
    assert store.worker_completed[1:] == ("succeeded", {"kind": "synthetic"})
    assert http.get(f"/v1/worker/runs/{run_id}", headers=WORKER_AUTH).status_code == 200
    assert http.post("/v1/worker/quarantine-expired", headers=WORKER_AUTH).json() == []


def test_worker_grant_fails_closed_for_wrong_scope_database_and_expiry():
    http, store = client(WORKER_TOKEN)
    grant = WorkerGrant(WORKER_TOKEN, "worker-12345", "caf_queue_test_unit",
                        store.scoped_run.run_id, store.scoped_run.task_id,
                        "caf-sample-app", "caf-sample-fixture-v1", "a" * 40,
                        "codex", datetime.now(timezone.utc) + timedelta(minutes=15))
    wrong = replace(grant, workspace_id="other-workspace")
    scoped = TestClient(create_app(Settings("unused", TOKEN, wrong), store))
    assert scoped.post("/v1/worker/claim", json={"owner": grant.identity},
                       headers=WORKER_AUTH).json() is None
    assert store.worker_owner is None
    assert scoped.get(f"/v1/worker/runs/{grant.run_id}", headers=WORKER_AUTH).status_code == 404
    expired = replace(grant, expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
    disabled = TestClient(create_app(Settings("unused", TOKEN, expired), store))
    assert disabled.get("/v1/worker/database", headers=WORKER_AUTH).status_code == 503
    bad_db = TestClient(create_app(Settings("unused", TOKEN,
                                           replace(grant, database="caf_queue_test_other")), store))
    assert bad_db.get("/v1/worker/database", headers=WORKER_AUTH).status_code == 503
    with pytest.raises(ValueError, match="lifetime"):
        replace(grant, expires_at=datetime.now(timezone.utc) + timedelta(hours=2))


def test_partial_worker_grant_configuration_rejected(monkeypatch):
    monkeypatch.setenv("CAF_API_TOKEN", TOKEN)
    monkeypatch.setenv("CAF_DATABASE_URL", "unused")
    monkeypatch.setenv("CAF_WORKER_TOKEN", WORKER_TOKEN)
    with pytest.raises(ValueError, match="every scope"):
        Settings.from_env()
