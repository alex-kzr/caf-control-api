CREATE TABLE IF NOT EXISTS tasks (
    id uuid PRIMARY KEY,
    target_repository text NOT NULL,
    workspace_id text,
    base_revision text NOT NULL,
    prompt text NOT NULL,
    acceptance_criteria jsonb NOT NULL,
    idempotency_key text UNIQUE,
    request_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS runs (
    id uuid PRIMARY KEY,
    task_id uuid NOT NULL UNIQUE REFERENCES tasks(id),
    correlation_id uuid NOT NULL,
    provider text NOT NULL CHECK (provider IN ('codex', 'claude')),
    max_corrections integer NOT NULL CHECK (max_corrections BETWEEN 0 AND 3),
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    lease_owner text,
    lease_expires_at timestamptz,
    attempt integer NOT NULL DEFAULT 0,
    cancel_requested boolean NOT NULL DEFAULT false,
    artifact jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL))
);

CREATE INDEX IF NOT EXISTS runs_claim_idx ON runs (created_at)
WHERE status = 'queued';

ALTER TABLE runs ADD COLUMN IF NOT EXISTS cancel_requested boolean NOT NULL DEFAULT false;
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS workspace_id text;

CREATE TABLE IF NOT EXISTS run_journal (
    seq bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    attempt integer NOT NULL,
    event text NOT NULL CHECK (event IN ('claimed', 'completed', 'quarantined')),
    owner text,
    terminal_status text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS run_journal_run_idx ON run_journal (run_id, seq);
