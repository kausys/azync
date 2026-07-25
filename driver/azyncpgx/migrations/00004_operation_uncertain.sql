-- +goose Up
-- Durable uncertain Operation jobs (docs/workflow-v1-spec.md §7–8).

ALTER TABLE azync_jobs DROP CONSTRAINT azync_jobs_state_check;
ALTER TABLE azync_jobs ADD CONSTRAINT azync_jobs_state_check
    CHECK (state IN (
        'pending', 'scheduled', 'active', 'dead', 'paused', 'succeeded',
        'blocked', 'waiting', 'cancelled', 'uncertain'
    ));

-- Idempotent ScheduleOperation: one live Operation per execution_key.
CREATE UNIQUE INDEX azync_jobs_workflow_exec_key_idx
    ON azync_jobs ((meta->>'execution_key'))
    WHERE source = 'workflow'
      AND meta ? 'execution_key'
      AND state IN ('pending', 'scheduled', 'active', 'uncertain');

-- +goose Down

DROP INDEX IF EXISTS azync_jobs_workflow_exec_key_idx;

ALTER TABLE azync_jobs DROP CONSTRAINT azync_jobs_state_check;
ALTER TABLE azync_jobs ADD CONSTRAINT azync_jobs_state_check
    CHECK (state IN (
        'pending', 'scheduled', 'active', 'dead', 'paused', 'succeeded',
        'blocked', 'waiting', 'cancelled'
    ));
