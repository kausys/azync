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
-- (hence NO TRANSACTION), each statement idempotent on its own. The new
-- unique index is built BEFORE the old one is dropped, so the dedupe
-- barrier never has a gap. CreateDAG's ON CONFLICT infers the arbiter from
-- the predicate — the SQL in dag_store.go carries the matching new WHERE.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS azync_dags_idempotency_live_idx
    ON azync_dags (name, idempotency_key)
    WHERE idempotency_key IS NOT NULL
        AND state IN ('running', 'suspended', 'compensating', 'paused');
DROP INDEX CONCURRENTLY IF EXISTS azync_dags_idempotency_idx;

CREATE INDEX CONCURRENTLY IF NOT EXISTS azync_dags_live_state_created_idx
    ON azync_dags (created_at)
    WHERE state IN ('running', 'suspended', 'compensating', 'paused');
DROP INDEX CONCURRENTLY IF EXISTS azync_dags_live_created_idx;

-- +goose Down
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS azync_dags_idempotency_idx
    ON azync_dags (name, idempotency_key)
    WHERE idempotency_key IS NOT NULL
        AND state IN ('running', 'suspended', 'compensating');
DROP INDEX CONCURRENTLY IF EXISTS azync_dags_idempotency_live_idx;

CREATE INDEX CONCURRENTLY IF NOT EXISTS azync_dags_live_created_idx
    ON azync_dags (created_at)
    WHERE state IN ('running', 'suspended', 'compensating');
DROP INDEX CONCURRENTLY IF EXISTS azync_dags_live_state_created_idx;
