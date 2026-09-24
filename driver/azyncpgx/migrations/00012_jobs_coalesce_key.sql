-- +goose NO TRANSACTION
-- +goose Up

-- coalesce_key backs queue.CoalesceKey. An enqueue carrying a key is dropped
-- when a job of the same kind and key exists that has not started yet
-- (pending or scheduled), and inserted otherwise — in particular when the
-- existing one is already running, so a signal arriving mid-run is never
-- absorbed by work that has already looked.
--
-- The check runs inside the INSERT (enqueueInsertSQL), not as a unique index.
-- A unique index would also bind every later transition back into pending or
-- scheduled — a retry backoff, Release, a manual retry — and one of those
-- colliding with a newer job would fail the transition itself. Without it two
-- concurrent enqueues can both insert: a redundant run, never a lost one.
--
-- Nullable with no default, so the ALTER is catalog-only. The index is partial
-- on exactly the states the check reads and is built CONCURRENTLY (hence NO
-- TRANSACTION), with 00009's recovery for an interrupted build: a same-name
-- leftover is dropped only when pg_index marks it invalid, and only the one
-- visible on this search_path, so another schema's index is never touched.
ALTER TABLE azync_jobs ADD COLUMN IF NOT EXISTS coalesce_key text NULL;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname = 'azync_jobs_coalesce_idx' AND NOT i.indisvalid
            AND pg_catalog.pg_table_is_visible(c.oid)
    ) THEN
        EXECUTE 'DROP INDEX azync_jobs_coalesce_idx';
    END IF;
END $$;
-- +goose StatementEnd
CREATE INDEX CONCURRENTLY IF NOT EXISTS azync_jobs_coalesce_idx
    ON azync_jobs (source, kind, coalesce_key)
    WHERE coalesce_key IS NOT NULL AND state IN ('pending', 'scheduled');

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS azync_jobs_coalesce_idx;
ALTER TABLE azync_jobs DROP COLUMN IF EXISTS coalesce_key;
