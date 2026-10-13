"""Opt-in PostgreSQL claim test; it never invokes an agent runtime."""

import os
import subprocess
import sys
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
            with store.pool.connection() as conn:
                journal = conn.execute(
                    "SELECT event, attempt, owner, terminal_status FROM run_journal "
                    "WHERE run_id = %s ORDER BY seq", (run.run_id,),
                ).fetchall()
            self.assertEqual(journal, [
                ("claimed", 1, owner, None),
                ("completed", 1, owner, "succeeded"),
            ])
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

    def test_cancel_queued_and_running_and_quarantine_expired(self):
        store = self.store()
        runs = []
        try:
            for label in ("queued", "running", "expired"):
                task = CreateTask(
                    target_repository="caf-sample-app", base_revision="a" * 40,
                    prompt=f"Queue control test {label}; no agent",
                    acceptance_criteria=["State is guarded"], provider="codex",
                )
                runs.append(store.submit(task, f"queue-control-{uuid4().hex}"))
            queued, running, expired = runs
            assert store.request_cancel(queued.run_id).status == "cancelled"
            assert store.request_cancel(queued.run_id).status == "cancelled"
            owner = f"worker-{uuid4().hex}"
            assert store.claim_queued(owner).id == running.run_id
            assert store.claim_control(running.run_id, owner) == (True, False)
            assert store.request_cancel(running.run_id).cancel_requested is True
            assert store.claim_control(running.run_id, owner) == (True, True)
            assert store.renew_lease(running.run_id, owner)
            assert not store.complete_claim(running.run_id, owner, "succeeded", {})
            assert store.complete_claim(running.run_id, owner, "cancelled", {"reason": "requested"})
            assert store.claim_queued(owner).id == expired.run_id
            with store.pool.connection() as conn, conn.transaction():
                conn.execute(
                    "UPDATE runs SET lease_expires_at = now() - interval '1 second' WHERE id = %s",
                    (expired.run_id,),
                )
            assert store.quarantine_expired() == [expired.run_id]
            assert store.quarantine_expired() == []
            assert store.get_run(expired.run_id).artifact["reason"] == "orphan_unverified"
            assert store.claim_queued(owner) is None
            with store.pool.connection() as conn:
                journal = conn.execute(
                    "SELECT event, attempt FROM run_journal WHERE run_id = %s ORDER BY seq",
                    (expired.run_id,),
                ).fetchall()
            assert journal == [("claimed", 1), ("quarantined", 1)]
        finally:
            for run in runs:
                with store.pool.connection() as conn, conn.transaction():
                    conn.execute("DELETE FROM runs WHERE id = %s", (run.run_id,))
                    conn.execute("DELETE FROM tasks WHERE id = %s", (run.task_id,))
            store.close()

    def test_worker_process_exit_preserves_lease_and_quarantines_without_replay(self):
        store = self.store()
        run = None
        try:
            task = CreateTask(
                target_repository="caf-sample-app", base_revision="a" * 40,
                prompt="Controlled worker exit; no coding CLI",
                acceptance_criteria=["No replay after unknown execution"], provider="codex",
            )
            run = store.submit(task, f"crash-test-{uuid4().hex}")
            owner = f"worker-{uuid4().hex}"
            fixture = (
                "import os;from caf_control_api.storage import PostgresStore;"
                "s=PostgresStore(os.environ['CAF_QUEUE_TEST_DATABASE_URL']);s.open();"
                "assert s.database_name().startswith('caf_queue_test_');"
                "assert s.claim_queued(os.environ['CAF_CRASH_TEST_OWNER']) is not None;"
                "os._exit(23)"
            )
            exited = subprocess.run(
                [sys.executable, "-c", fixture],
                env={**os.environ, "CAF_CRASH_TEST_OWNER": owner},
                capture_output=True, timeout=10,
            )
            self.assertEqual(exited.returncode, 23)
            with store.pool.connection() as conn:
                row = conn.execute(
                    "SELECT status, attempt, lease_owner, lease_expires_at FROM runs WHERE id = %s",
                    (run.run_id,),
                ).fetchone()
                journal = conn.execute(
                    "SELECT event FROM run_journal WHERE run_id = %s ORDER BY seq",
                    (run.run_id,),
                ).fetchall()
            self.assertEqual(row[0:3], ("running", 1, owner))
            self.assertIsNotNone(row[3])
            self.assertEqual(journal, [("claimed",)])
            self.assertIsNone(store.claim_queued(f"worker-{uuid4().hex}"))
            with store.pool.connection() as conn, conn.transaction():
                conn.execute(
                    "UPDATE runs SET lease_expires_at = now() - interval '1 second' WHERE id = %s",
                    (run.run_id,),
                )
            self.assertEqual(store.quarantine_expired(), [run.run_id])
            self.assertIsNone(store.claim_queued(f"worker-{uuid4().hex}"))
            self.assertEqual(store.get_run(run.run_id).artifact["reason"], "orphan_unverified")
            with store.pool.connection() as conn:
                journal = conn.execute(
                    "SELECT event FROM run_journal WHERE run_id = %s ORDER BY seq",
                    (run.run_id,),
                ).fetchall()
            self.assertEqual(journal, [("claimed",), ("quarantined",)])
        finally:
            if run is not None:
                with store.pool.connection() as conn, conn.transaction():
                    conn.execute("DELETE FROM runs WHERE id = %s", (run.run_id,))
                    conn.execute("DELETE FROM tasks WHERE id = %s", (run.task_id,))
            store.close()


if __name__ == "__main__":
    unittest.main()
