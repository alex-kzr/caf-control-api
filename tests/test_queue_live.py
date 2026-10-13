"""Opt-in PostgreSQL claim test; it never invokes an agent runtime."""

import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from uuid import uuid4

from caf_control_api.models import CreateTask
from caf_control_api.storage import PostgresStore


@unittest.skipUnless(os.getenv("CAF_QUEUE_TEST_LIVE") == "1", "live database opt-in required")
class QueueClaimTest(unittest.TestCase):
    def store(self):
        dsn = os.environ["CAF_QUEUE_TEST_DATABASE_URL"]
        expected_database = urlparse(dsn).path.lstrip("/")
        if not expected_database.startswith("caf_queue_test_"):
            raise ValueError("queue integration tests require a dedicated caf_queue_test_* database")
        store = PostgresStore(dsn)
        store.open()
        actual_database = store.database_name()
        if actual_database != expected_database:
            store.close()
            raise ValueError("connected database does not match dedicated queue test database")
        return store

    def test_competing_claims_and_owner_guard(self):
        store = self.store()
        run = None
        try:
            with store.pool.connection() as conn:
                queued = conn.execute("SELECT count(*) FROM runs WHERE status = 'queued'").fetchone()[0]
            self.assertEqual(queued, 0)

            task = CreateTask(
                target_repository="caf-sample-app", base_revision="a" * 40,
                prompt="Synthetic queue protocol check; do not execute an agent",
                acceptance_criteria=["Claim and completion persist"], provider="codex",
            )
            run = store.submit(task, f"queue-test-{uuid4().hex}")
            owner_a, owner_b = f"worker-{uuid4().hex}", f"worker-{uuid4().hex}"
            with ThreadPoolExecutor(max_workers=2) as executor:
                claims = list(executor.map(lambda owner: store.claim_queued(owner), (owner_a, owner_b)))
            self.assertEqual(sum(claim is not None for claim in claims), 1)
            claim = next(claim for claim in claims if claim is not None)
            owner = owner_a if claims[0] is not None else owner_b
            wrong_owner = owner_b if owner == owner_a else owner_a
            self.assertEqual(claim.id, run.run_id)
            self.assertEqual(claim.correlation_id, run.correlation_id)
            self.assertEqual(claim.prompt, task.prompt)
            self.assertEqual(claim.attempt, 1)
            self.assertFalse(store.renew_lease(run.run_id, wrong_owner))
            self.assertFalse(store.complete_claim(run.run_id, wrong_owner, "succeeded", {}))
            self.assertTrue(store.renew_lease(run.run_id, owner))
            self.assertTrue(store.complete_claim(run.run_id, owner, "succeeded", {"mode": "synthetic-test"}))
            self.assertFalse(store.complete_claim(run.run_id, owner, "failed", {}))
            recorded = store.get_run(run.run_id)
            self.assertEqual(recorded.status, "succeeded")
            self.assertEqual(recorded.attempt, 1)
            self.assertEqual(recorded.artifact, {"mode": "synthetic-test"})
        finally:
            if run is not None:
                with store.pool.connection() as conn, conn.transaction():
                    conn.execute("DELETE FROM runs WHERE id = %s", (run.run_id,))
                    conn.execute("DELETE FROM tasks WHERE id = %s", (run.task_id,))
            store.close()

    def test_expired_owner_cannot_complete(self):
        store = self.store()
        run = None
        try:
            with store.pool.connection() as conn:
                queued = conn.execute("SELECT count(*) FROM runs WHERE status = 'queued'").fetchone()[0]
            self.assertEqual(queued, 0)
            task = CreateTask(
                target_repository="caf-sample-app", base_revision="a" * 40,
                prompt="Synthetic expired lease check; do not execute an agent",
                acceptance_criteria=["Expired owner cannot finish"], provider="codex",
            )
            run = store.submit(task, f"queue-test-{uuid4().hex}")
            owner = f"worker-{uuid4().hex}"
            self.assertEqual(store.claim_queued(owner).id, run.run_id)
            with store.pool.connection() as conn, conn.transaction():
                conn.execute(
                    "UPDATE runs SET lease_expires_at = now() - interval '1 second' WHERE id = %s",
                    (run.run_id,),
                )
            self.assertFalse(store.renew_lease(run.run_id, owner))
            self.assertFalse(store.complete_claim(run.run_id, owner, "succeeded", {}))
            self.assertEqual(store.get_run(run.run_id).status, "running")
        finally:
            if run is not None:
                with store.pool.connection() as conn, conn.transaction():
                    conn.execute("DELETE FROM runs WHERE id = %s", (run.run_id,))
                    conn.execute("DELETE FROM tasks WHERE id = %s", (run.task_id,))
            store.close()


if __name__ == "__main__":
    unittest.main()
