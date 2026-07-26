-- +goose Up

-- azync_dags is the workflow header: one row per execution of a workflow
-- definition. The DAG's tasks and dependency edges live in azync_jobs
-- (source='dag') and azync_dag_deps; this row carries the lifecycle
-- state, the declared failure policy and the dedupe/idempotency scope.
CREATE TABLE azync_dags (
    id               uuid PRIMARY KEY,
    name             text NOT NULL,
    state            text NOT NULL DEFAULT 'running'
                         CHECK (state IN ('running', 'suspended', 'compensating', 'succeeded', 'failed', 'cancelled')),
    on_failure       text NOT NULL
                         CHECK (on_failure IN ('cancel', 'suspend')),
    idempotency_key  text NULL,
    failure_reason   text NULL,
    -- cancel_requested records that CancelDAG was called, so a compensation
    -- that settles afterwards lands the workflow on cancelled instead of failed.
    cancel_requested boolean NOT NULL DEFAULT false,
    meta             jsonb NOT NULL DEFAULT '{}',
    trace_id         text NULL,
    span_id          text NULL,
    trace_flags      smallint NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    completed_at     timestamptz NULL
);
-- Admin browsing: live executions newest-first, and per-definition history.
CREATE INDEX azync_dags_live_created_idx ON azync_dags (created_at)
    WHERE state IN ('running', 'suspended', 'compensating');
CREATE INDEX azync_dags_name_created_idx ON azync_dags (name, created_at);
-- Live-execution dedupe: a duplicate (name, idempotency_key) is rejected only
-- while a prior execution is still live; a terminal workflow frees the key.
CREATE UNIQUE INDEX azync_dags_idempotency_idx ON azync_dags (name, idempotency_key)
    WHERE idempotency_key IS NOT NULL AND state IN ('running', 'suspended', 'compensating');
-- VacuumDAGs' retention sweep: terminal dags by completion time.
CREATE INDEX azync_dags_terminal_completed_idx ON azync_dags (completed_at)
    WHERE state IN ('succeeded', 'failed', 'cancelled');

-- azync_dag_deps holds the static DAG edges plus the compensation chain
-- links inserted at compensation time: task_key waits for depends_on_key.
CREATE TABLE azync_dag_deps (
    dag_id    uuid NOT NULL REFERENCES azync_dags (id) ON DELETE CASCADE,
    task_key       text NOT NULL,
    depends_on_key text NOT NULL,
    PRIMARY KEY (dag_id, task_key, depends_on_key)
);

-- azync_jobs gains the workflow-task columns (all NULL/defaulted so existing
-- queue and event rows are untouched), and its source/state CHECKs are swapped
-- to admit the 'dag' source and the workflow-only states.
ALTER TABLE azync_jobs
    ADD COLUMN dag_id          uuid NULL REFERENCES azync_dags (id) ON DELETE CASCADE,
    ADD COLUMN task_key             text NULL,
    ADD COLUMN result               jsonb NULL,
    ADD COLUMN compensation_kind    text NULL,
    ADD COLUMN compensation_payload jsonb NULL,
    ADD COLUMN signal_name          text NULL,
    ADD COLUMN ignore_dead_deps     boolean NOT NULL DEFAULT false;

ALTER TABLE azync_jobs DROP CONSTRAINT azync_jobs_source_check;
ALTER TABLE azync_jobs ADD CONSTRAINT azync_jobs_source_check CHECK (source IN ('queue', 'event', 'dag'));

ALTER TABLE azync_jobs DROP CONSTRAINT azync_jobs_state_check;
ALTER TABLE azync_jobs ADD CONSTRAINT azync_jobs_state_check
    CHECK (state IN ('pending', 'scheduled', 'active', 'dead', 'paused', 'succeeded', 'blocked', 'waiting', 'cancelled'));

-- One task per (workflow, key); the compensation chain uses 'comp:<key>' keys.
CREATE UNIQUE INDEX azync_jobs_dag_task_idx ON azync_jobs (dag_id, task_key)
    WHERE dag_id IS NOT NULL;
-- The scheduler's PromoteUnblocked scans blocked tasks; Signal scans waiting ones.
CREATE INDEX azync_jobs_dag_blocked_idx ON azync_jobs (dag_id) WHERE state = 'blocked';
CREATE INDEX azync_jobs_dag_waiting_idx ON azync_jobs (dag_id, signal_name) WHERE state = 'waiting';

-- +goose Down
-- Safe only on a schema with no workflow data: re-adding the narrowed CHECKs
-- below fails while any workflow row exists (see README, "Migrations").
DROP INDEX azync_jobs_dag_waiting_idx;
DROP INDEX azync_jobs_dag_blocked_idx;
DROP INDEX azync_jobs_dag_task_idx;

ALTER TABLE azync_jobs DROP CONSTRAINT azync_jobs_state_check;
ALTER TABLE azync_jobs ADD CONSTRAINT azync_jobs_state_check
    CHECK (state IN ('pending', 'scheduled', 'active', 'dead', 'paused', 'succeeded'));

ALTER TABLE azync_jobs DROP CONSTRAINT azync_jobs_source_check;
ALTER TABLE azync_jobs ADD CONSTRAINT azync_jobs_source_check CHECK (source IN ('queue', 'event'));

DROP INDEX azync_dags_terminal_completed_idx;

ALTER TABLE azync_jobs
    DROP COLUMN ignore_dead_deps,
    DROP COLUMN signal_name,
    DROP COLUMN compensation_payload,
    DROP COLUMN compensation_kind,
    DROP COLUMN result,
    DROP COLUMN task_key,
    DROP COLUMN dag_id;

DROP TABLE azync_dag_deps;
DROP TABLE azync_dags;
