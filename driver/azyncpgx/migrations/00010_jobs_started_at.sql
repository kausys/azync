-- +goose Up

-- started_at records when the current attempt was leased, so an admin surface
-- can report how long a job actually ran. No existing column answers that:
-- run_at is the *due* time and is rewritten on every promotion, retry backoff
-- and snooze, so completed_at - run_at is not a duration; enqueued_at includes
-- however long the job waited to be picked up. The dequeue claim stamps this
-- alongside attempt = attempt + 1, so for a settled job
-- completed_at - started_at is the duration of the LAST attempt — earlier ones
-- are timestamped individually in azync_job_attempts.
--
-- Nullable with no default, so this is a catalog-only ALTER (no table rewrite,
-- no long lock) even on a large azync_jobs. Rows written before this migration
-- stay NULL until their next lease; readers must treat NULL as "unknown", not
-- as zero elapsed.
--
-- IF NOT EXISTS because goose's version table is not checksummed: an
-- environment whose records were rebuilt (see 00005) replays this file against
-- a schema that already has the column, and a bare ADD COLUMN would abort the
-- whole convergence. Pinned by TestMigrateConvergesFromDriftedTenantTraceSchema.
ALTER TABLE azync_jobs ADD COLUMN IF NOT EXISTS started_at timestamptz NULL;

-- DAGNameStateCounts groups every dag by (name, state) to feed the admin's
-- definition navigator and its state tabs from one read. Every index from
-- 00002/00009 is partial on the live states, so an unrestricted GROUP BY falls
-- back to a sequential scan over the full table including terminal history.
-- The column order is (name, state) rather than (state) because the grouping
-- is by both and the leading column is the one a caller also filters on.
CREATE INDEX IF NOT EXISTS azync_dags_name_state_idx ON azync_dags (name, state);

-- +goose Down
DROP INDEX IF EXISTS azync_dags_name_state_idx;
ALTER TABLE azync_jobs DROP COLUMN IF EXISTS started_at;
