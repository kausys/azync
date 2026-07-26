-- +goose Up
-- Re-expresses the tenant/trace-into-meta change (originally shipped as an
-- in-place edit to 00001/00002 in v0.0.3, which goose's non-checksummed
-- version tracking cannot detect) as a proper forward migration.
--
-- 00001/00002 have been restored to their v0.0.1/v0.0.2 released content, so
-- every upgrade path converges here regardless of which schema an
-- environment actually ran:
--   - v0.0.1/v0.0.2 installs already applied 00001/00002 with the legacy
--     columns; this migration drops them.
--   - v0.0.3 installs already applied the edited 00001/00002 (no legacy
--     columns); every DROP COLUMN IF EXISTS below is then a no-op.
--   - fresh installs from this version on run the restored 00001/00002
--     (legacy columns present) and this migration drops them immediately.
--
-- All operations are metadata-only catalog changes (no column default
-- requiring a rewrite), so each ALTER TABLE takes a brief ACCESS EXCLUSIVE
-- lock without scanning or rewriting the table.
DROP INDEX IF EXISTS azync_events_tenant_occurred_idx;

ALTER TABLE azync_events
    DROP COLUMN IF EXISTS tenant_id,
    DROP COLUMN IF EXISTS trace_id,
    DROP COLUMN IF EXISTS span_id,
    DROP COLUMN IF EXISTS trace_flags;

ALTER TABLE azync_jobs
    DROP COLUMN IF EXISTS trace_id,
    DROP COLUMN IF EXISTS span_id,
    DROP COLUMN IF EXISTS trace_flags;

ALTER TABLE azync_job_attempts
    DROP COLUMN IF EXISTS trace;

ALTER TABLE azync_dags
    DROP COLUMN IF EXISTS trace_id,
    DROP COLUMN IF EXISTS span_id,
    DROP COLUMN IF EXISTS trace_flags;

-- +goose Down
-- Intentionally a no-op: no released runtime code reads these columns, so
-- there is nothing to restore into.
