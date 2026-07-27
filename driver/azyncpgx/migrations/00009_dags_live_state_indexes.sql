-- +goose NO TRANSACTION
-- +goose Up

-- 00008 added the 'paused' DAG header state. Two partial indexes from 00002
-- enumerate the live states in their predicate and must include it:
--
--   * azync_dags_idempotency_idx is the live-execution dedupe barrier. If
--     'paused' were missing, pausing a DAG would free its idempotency key
--     and a duplicate run could start while the original is merely frozen.
--   * azync_dags_live_created_idx backs admin browsing of live executions;
--     without 'paused', paused DAGs would vanish from that listing.
--
-- Indexes on the potentially large azync_dags are rebuilt CONCURRENTLY
-- (hence NO TRANSACTION). The new unique index is built BEFORE the old one
-- is dropped, so the dedupe barrier never has a gap. CreateDAG's ON
-- CONFLICT infers the arbiter from the predicate — the SQL in dag_store.go
-- carries the matching new WHERE, and the old 3-state WHERE of a
-- not-yet-redeployed replica still infers against the 4-state index
-- (subset-IN implication; pinned by an integration test), so a rolling
-- deploy where new replicas migrate first keeps old replicas working.
--
-- Failure recovery makes the file safely re-runnable: an interrupted
-- CONCURRENTLY build leaves the index created but INVALID — and an invalid
-- UNIQUE index does not enforce uniqueness, the worst possible silent
-- failure for a dedupe barrier — while a bare IF NOT EXISTS on the re-run
-- would see the name and skip it, reporting success. Each DO block below
-- (its own implicit transaction) drops a same-name leftover ONLY when
-- pg_index marks it invalid, so the following CREATE rebuilds it; a valid
-- index is never touched.

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname = 'azync_dags_idempotency_live_idx' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX azync_dags_idempotency_live_idx';
    END IF;
END $$;
-- +goose StatementEnd
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS azync_dags_idempotency_live_idx
    ON azync_dags (name, idempotency_key)
    WHERE idempotency_key IS NOT NULL
        AND state IN ('running', 'suspended', 'compensating', 'paused');
DROP INDEX CONCURRENTLY IF EXISTS azync_dags_idempotency_idx;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname = 'azync_dags_live_state_created_idx' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX azync_dags_live_state_created_idx';
    END IF;
END $$;
-- +goose StatementEnd
CREATE INDEX CONCURRENTLY IF NOT EXISTS azync_dags_live_state_created_idx
    ON azync_dags (created_at)
    WHERE state IN ('running', 'suspended', 'compensating', 'paused');
DROP INDEX CONCURRENTLY IF EXISTS azync_dags_live_created_idx;

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname = 'azync_dags_idempotency_idx' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX azync_dags_idempotency_idx';
    END IF;
END $$;
-- +goose StatementEnd
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS azync_dags_idempotency_idx
    ON azync_dags (name, idempotency_key)
    WHERE idempotency_key IS NOT NULL
        AND state IN ('running', 'suspended', 'compensating');
DROP INDEX CONCURRENTLY IF EXISTS azync_dags_idempotency_live_idx;

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname = 'azync_dags_live_created_idx' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX azync_dags_live_created_idx';
    END IF;
END $$;
-- +goose StatementEnd
CREATE INDEX CONCURRENTLY IF NOT EXISTS azync_dags_live_created_idx
    ON azync_dags (created_at)
    WHERE state IN ('running', 'suspended', 'compensating');
DROP INDEX CONCURRENTLY IF EXISTS azync_dags_live_state_created_idx;
