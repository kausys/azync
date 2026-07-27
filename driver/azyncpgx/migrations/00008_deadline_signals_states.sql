-- +goose Up

-- Per-task snooze deadline (dag.Deadline). Two nullable columns, no default:
-- catalog-only, instant on a hot azync_jobs. snooze_budget is the duration
-- (seconds) declared at insert; deadline_at is stamped lazily by the FIRST
-- Snooze (now() + snooze_budget, backend clock) so the budget measures time
-- spent waiting, not time since the workflow was created. Read only on rows
-- already leased (the Snooze settlement) — no index needed.
ALTER TABLE azync_jobs ADD COLUMN IF NOT EXISTS snooze_budget double precision NULL;
ALTER TABLE azync_jobs ADD COLUMN IF NOT EXISTS deadline_at timestamptz NULL;

-- Buffered DAG signals — the per-DAG inbox, mirroring azync_workflow_signals
-- (00003). A signal that arrives before its WaitSignal/Sleep task is
-- deliverable is stored here and handed over by the scheduler's
-- DeliverBufferedSignals pass; rows cascade with their DAG, so VacuumDAGs'
-- retention sweep covers them. The table is created empty, so plain (in-tx)
-- index builds are safe here.
CREATE TABLE IF NOT EXISTS azync_dag_signals (
    id         uuid PRIMARY KEY,
    dag_id     uuid NOT NULL REFERENCES azync_dags (id) ON DELETE CASCADE,
    name       text NOT NULL,
    message_id text NULL,
    payload    jsonb NULL,
    consumed   boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- At-least-once webhook dedupe: one inbox row per (dag, name, message_id).
CREATE UNIQUE INDEX IF NOT EXISTS azync_dag_signals_dedupe_idx
    ON azync_dag_signals (dag_id, name, message_id)
    WHERE message_id IS NOT NULL;

-- DeliverBufferedSignals scans unconsumed rows, oldest first.
CREATE INDEX IF NOT EXISTS azync_dag_signals_inbox_idx
    ON azync_dag_signals (dag_id, name, created_at)
    WHERE NOT consumed;

-- New job state 'skipped' (dag.Skip): terminal, satisfies dependencies like
-- succeeded, carries no result and never compensates. NOT VALID + VALIDATE
-- per this directory's README: the catalog swap is instant and the
-- validation scan runs without blocking writes. DROP IF EXISTS + ADD keeps
-- the whole file idempotent (an upgrade-convergence re-run is a no-op-safe
-- replace).
ALTER TABLE azync_jobs DROP CONSTRAINT IF EXISTS azync_jobs_state_check;
ALTER TABLE azync_jobs ADD CONSTRAINT azync_jobs_state_check
    CHECK (state IN (
        'pending', 'scheduled', 'active', 'dead', 'paused', 'succeeded',
        'blocked', 'waiting', 'cancelled', 'uncertain', 'skipped'
    )) NOT VALID;
ALTER TABLE azync_jobs VALIDATE CONSTRAINT azync_jobs_state_check;

-- New DAG header state 'paused' (dag.Manager.Pause): operator-initiated
-- freeze, non-terminal, resumed by Manager.Retry. Same NOT VALID + VALIDATE
-- treatment.
ALTER TABLE azync_dags DROP CONSTRAINT IF EXISTS azync_dags_state_check;
ALTER TABLE azync_dags ADD CONSTRAINT azync_dags_state_check
    CHECK (state IN (
        'running', 'suspended', 'compensating', 'paused',
        'succeeded', 'failed', 'cancelled'
    )) NOT VALID;
ALTER TABLE azync_dags VALIDATE CONSTRAINT azync_dags_state_check;

-- +goose Down
ALTER TABLE azync_dags DROP CONSTRAINT azync_dags_state_check;
ALTER TABLE azync_dags ADD CONSTRAINT azync_dags_state_check
    CHECK (state IN ('running', 'suspended', 'compensating', 'succeeded', 'failed', 'cancelled')) NOT VALID;
ALTER TABLE azync_dags VALIDATE CONSTRAINT azync_dags_state_check;

ALTER TABLE azync_jobs DROP CONSTRAINT azync_jobs_state_check;
ALTER TABLE azync_jobs ADD CONSTRAINT azync_jobs_state_check
    CHECK (state IN (
        'pending', 'scheduled', 'active', 'dead', 'paused', 'succeeded',
        'blocked', 'waiting', 'cancelled', 'uncertain'
    )) NOT VALID;
ALTER TABLE azync_jobs VALIDATE CONSTRAINT azync_jobs_state_check;

DROP TABLE azync_dag_signals;

ALTER TABLE azync_jobs DROP COLUMN deadline_at;
ALTER TABLE azync_jobs DROP COLUMN snooze_budget;
