-- +goose Up

-- Workflow-as-code control plane (see docs/workflow-v1-spec.md).
-- Reuses azync_jobs with source='workflow' and run_id linkage; history is
-- append-only and retained by default (no automatic vacuum in MVP).

CREATE TABLE azync_workflow_executions (
    id                        uuid PRIMARY KEY,
    name                      text NOT NULL,
    version                   text NOT NULL,
    state                     text NOT NULL DEFAULT 'running'
                                  CHECK (state IN ('running', 'suspended', 'succeeded', 'failed', 'cancelled')),
    business_idempotency_key  text NULL,
    task_queue                text NOT NULL DEFAULT 'default',
    input                     jsonb NULL,
    result                    jsonb NULL,
    failure_reason            text NULL,
    meta                      jsonb NOT NULL DEFAULT '{}',
    created_at                timestamptz NOT NULL DEFAULT now(),
    updated_at                timestamptz NOT NULL DEFAULT now(),
    completed_at              timestamptz NULL
);

CREATE UNIQUE INDEX azync_workflow_executions_live_biz_idx
    ON azync_workflow_executions (name, business_idempotency_key)
    WHERE business_idempotency_key IS NOT NULL
      AND state IN ('running', 'suspended');

CREATE INDEX azync_workflow_executions_name_created_idx
    ON azync_workflow_executions (name, created_at);

CREATE TABLE azync_workflow_history (
    workflow_id uuid NOT NULL REFERENCES azync_workflow_executions (id) ON DELETE CASCADE,
    event_seq   bigint NOT NULL,
    event_type  text NOT NULL,
    payload     jsonb NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, event_seq)
);

CREATE TABLE azync_workflow_signals (
    id          uuid PRIMARY KEY,
    workflow_id uuid NOT NULL REFERENCES azync_workflow_executions (id) ON DELETE CASCADE,
    name        text NOT NULL,
    message_id  text NULL,
    payload     jsonb NULL,
    consumed    boolean NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX azync_workflow_signals_dedupe_idx
    ON azync_workflow_signals (workflow_id, name, message_id)
    WHERE message_id IS NOT NULL;

CREATE INDEX azync_workflow_signals_inbox_idx
    ON azync_workflow_signals (workflow_id, name, created_at)
    WHERE NOT consumed;

CREATE TABLE azync_workflow_timers (
    id          uuid PRIMARY KEY,
    workflow_id uuid NOT NULL REFERENCES azync_workflow_executions (id) ON DELETE CASCADE,
    fire_at     timestamptz NOT NULL,
    fired       boolean NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX azync_workflow_timers_due_idx
    ON azync_workflow_timers (fire_at)
    WHERE NOT fired;

ALTER TABLE azync_jobs
    ADD COLUMN run_id uuid NULL REFERENCES azync_workflow_executions (id) ON DELETE CASCADE;

ALTER TABLE azync_jobs DROP CONSTRAINT azync_jobs_source_check;
ALTER TABLE azync_jobs ADD CONSTRAINT azync_jobs_source_check
    CHECK (source IN ('queue', 'event', 'dag', 'workflow'));

CREATE INDEX azync_jobs_run_idx ON azync_jobs (run_id) WHERE run_id IS NOT NULL;

-- +goose Down

DROP INDEX azync_jobs_run_idx;

ALTER TABLE azync_jobs DROP CONSTRAINT azync_jobs_source_check;
ALTER TABLE azync_jobs ADD CONSTRAINT azync_jobs_source_check
    CHECK (source IN ('queue', 'event', 'dag'));

ALTER TABLE azync_jobs DROP COLUMN run_id;

DROP TABLE azync_workflow_timers;
DROP TABLE azync_workflow_signals;
DROP TABLE azync_workflow_history;
DROP TABLE azync_workflow_executions;
