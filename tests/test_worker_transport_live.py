"""Opt-in in-process worker HTTP contract against an isolated PostgreSQL DB."""

import os
import unittest
from urllib.parse import urlparse
from uuid import uuid4

from fastapi.testclient import TestClient

from caf_control_api.app import Settings, create_app
from caf_control_api.storage import PostgresStore


@unittest.skipUnless(os.getenv("CAF_QUEUE_TEST_LIVE") == "1", "live test DB opt-in required")
class WorkerTransportLiveTest(unittest.TestCase):
    def test_scoped_routes_preserve_owner_and_journal(self):
        dsn = os.environ["CAF_QUEUE_TEST_DATABASE_URL"]
        expected = urlparse(dsn).path.lstrip("/")
        if not expected.startswith("caf_queue_test_"):
            raise ValueError("worker transport test requires isolated database")
        store = PostgresStore(dsn)
        app = create_app(Settings(dsn, "x" * 32, "w" * 32), store)
        run_id = task_id = None
        with TestClient(app) as http:
            self.assertEqual(store.database_name(), expected)
            try:
                submitted = http.post("/v1/tasks", headers={"Authorization": "Bearer " + "x" * 32},
                                      json={"target_repository": "caf-sample-app",
                                            "base_revision": "a" * 40,
                                            "prompt": "Scoped worker transport fixture; no CLI",
                                            "acceptance_criteria": ["One guarded ACK"],
                                            "provider": "codex"},
                                      ).json()
                run_id, task_id = submitted["run_id"], submitted["task_id"]
                auth = {"Authorization": "Bearer " + "w" * 32}
                owner = f"worker-{uuid4().hex}"
                claim = http.post("/v1/worker/claim", headers=auth,
                                  json={"owner": owner, "lease_seconds": 60})
                self.assertEqual(claim.json()["id"], run_id)
                self.assertFalse(http.post(f"/v1/worker/runs/{run_id}/renew", headers=auth,
                                           json={"owner": "other-worker", "lease_seconds": 60}
                                           ).json()["renewed"])
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
