-- +goose NO TRANSACTION
-- +goose Up
-- VacuumCompleted's predicate (source, state='succeeded', dag_id IS NULL,
-- run_id IS NULL, completed_at < cutoff) has no matching index: the existing
-- azync_jobs_succeeded_idx (00001) is (source, kind, completed_at), and
-- VacuumCompleted has no kind to bind, so completed_at can't be used as a
-- range condition through it. CONCURRENTLY + NO TRANSACTION (idempotent via
-- IF NOT EXISTS) so building this on a populated azync_jobs never blocks
-- reads or writes.
CREATE INDEX CONCURRENTLY IF NOT EXISTS azync_jobs_succeeded_vacuum_idx
    ON azync_jobs (source, completed_at)
    WHERE state = 'succeeded' AND dag_id IS NULL AND run_id IS NULL;

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS azync_jobs_succeeded_vacuum_idx;
