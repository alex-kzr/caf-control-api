"""Opt-in in-process worker HTTP contract against an isolated PostgreSQL DB."""

import os
import unittest
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from urllib.parse import urlparse
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from caf_control_api.app import Settings, create_app
from caf_control_api.storage import PostgresStore
from caf_control_api.worker_grant import WorkerGrant


@unittest.skipUnless(os.getenv("CAF_QUEUE_TEST_LIVE") == "1", "live test DB opt-in required")
class WorkerTransportLiveTest(unittest.TestCase):
    def test_scoped_routes_preserve_owner_and_journal(self):
        dsn = os.environ["CAF_QUEUE_TEST_DATABASE_URL"]
        expected = urlparse(dsn).path.lstrip("/")
        if not expected.startswith("caf_queue_test_"):
            raise ValueError("worker transport test requires isolated database")
        store = PostgresStore(dsn)
        run_id = task_id = None
        with TestClient(create_app(Settings(dsn, "x" * 32), store)) as http:
            self.assertEqual(store.database_name(), expected)
            submitted = http.post("/v1/tasks", headers={"Authorization": "Bearer " + "x" * 32},
                                      json={"target_repository": "caf-sample-app",
                                            "workspace_id": "caf-sample-fixture-v1",
                                            "base_revision": "a" * 40,
                                            "prompt": "Scoped worker transport fixture; no CLI",
                                            "acceptance_criteria": ["One guarded ACK"],
                                            "provider": "codex"},
                                  ).json()
            run_id, task_id = submitted["run_id"], submitted["task_id"]
        grant = WorkerGrant("w" * 32, f"worker-{uuid4().hex}", expected,
                            uuid4(), uuid4(), "caf-sample-app", "caf-sample-fixture-v1",
                            "a" * 40, "codex",
                            datetime.now(timezone.utc) + timedelta(hours=1))
        # A wrong grant must not claim the queued Run or expose it by ID.
        with TestClient(create_app(Settings(dsn, "x" * 32, grant), PostgresStore(dsn))) as http:
            auth = {"Authorization": "Bearer " + "w" * 32}
            self.assertIsNone(http.post("/v1/worker/claim", headers=auth,
                                        json={"owner": grant.identity}).json())
            self.assertEqual(http.get(f"/v1/worker/runs/{run_id}", headers=auth).status_code, 404)
        grant = WorkerGrant("w" * 32, grant.identity, expected,
                            UUID(run_id), UUID(task_id),
                            "caf-sample-app", "caf-sample-fixture-v1", "a" * 40, "codex",
                            datetime.now(timezone.utc) + timedelta(hours=1))
        with TestClient(create_app(Settings(dsn, "x" * 32,
                                                replace(grant, workspace_id="wrong-workspace")),
                                        PostgresStore(dsn))) as http:
            self.assertIsNone(http.post("/v1/worker/claim", headers=auth,
                                        json={"owner": grant.identity}).json())
        store = PostgresStore(dsn)
        with TestClient(create_app(Settings(dsn, "x" * 32, grant), store)) as http:
            try:
                auth = {"Authorization": "Bearer " + "w" * 32}
                owner = grant.identity
                claim = http.post("/v1/worker/claim", headers=auth,
                                  json={"owner": owner, "lease_seconds": 60})
                self.assertEqual(claim.json()["id"], run_id)
                self.assertEqual(http.post(f"/v1/worker/runs/{run_id}/renew", headers=auth,
                                           json={"owner": "other-worker", "lease_seconds": 60}
                                           ).status_code, 403)
                self.assertTrue(http.post(f"/v1/worker/runs/{run_id}/renew", headers=auth,
                                          json={"owner": owner, "lease_seconds": 60}
                                          ).json()["renewed"])
                self.assertTrue(http.post(f"/v1/worker/runs/{run_id}/complete", headers=auth,
                                          json={"owner": owner, "status": "succeeded",
                                                "artifact": {"kind": "transport-test"}}
                                          ).json()["completed"])
                self.assertEqual(http.get(f"/v1/worker/runs/{run_id}", headers=auth
                                          ).json()["status"], "succeeded")
                with store.pool.connection() as conn:
                    journal = conn.execute(
                        "SELECT event FROM run_journal WHERE run_id = %s ORDER BY seq",
                        (run_id,),
                    ).fetchall()
                self.assertEqual(journal, [("claimed",), ("completed",)])
            finally:
                if run_id is not None:
                    with store.pool.connection() as conn, conn.transaction():
                        conn.execute("DELETE FROM runs WHERE id = %s", (run_id,))
                        conn.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
