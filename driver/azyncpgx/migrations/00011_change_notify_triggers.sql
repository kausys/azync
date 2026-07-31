-- +goose Up

-- Change-hint triggers backing the driver.ChangeNotifier capability: every
-- job/DAG insert and state transition, and every event-ledger append, emits
-- one pg_notify on the fixed 'azync_changes' channel so external observers
-- (ops UIs, SSE bridges) can react without polling. Hints are best-effort
-- and at-most-once by contract — LISTEN/NOTIFY has no replay — so consumers
-- treat them as refetch signals, never as a durable feed.
--
-- * PII rule: payloads carry ONLY schema/entity/source/ids/kind/task_key/
--   state/atMs — never the payload, result or meta columns, which routinely
--   carry PII. This is a contract, not a convenience: the Go surface gates
--   full-row reads behind the Managers and the caller's own authorization.
--
-- * Fixed channel + TG_TABLE_SCHEMA in the payload (the Go listener filters
--   by schema): migration SQL is untemplated (schema isolation rides
--   search_path), so the trigger cannot know the configured wakeup channel,
--   and mirroring defaultChannel()'s 63-byte truncation fallback in PL/pgSQL
--   would be a silent-divergence hazard. One shared channel with the schema
--   in the payload keeps both sides trivially in agreement.
--
-- * Statement-level with transition tables, per-row notifies capped at 50:
--   batch statements here touch up to the maintenance batch (1000 rows) per
--   statement, and the NOTIFY queue is a shared 8GB resource any stalled
--   listener can wedge for the whole database. Above the cap one coalesced
--   {bulk:true,count:N} payload per partition replaces the per-row hints;
--   consumers treat it as "refetch the partition". The UPDATE functions
--   consider only rows whose state really changed (statement triggers cannot
--   carry a row-level WHEN), so the lease-heartbeat UPDATE path exits after
--   one cheap count over the transition tuplestore and emits nothing.
--
-- * Cost: this adds transition-table capture plus one PL/pgSQL call to every
--   INSERT/UPDATE statement on the three tables, for every install, watcher
--   or not. The no-op path is deliberately cheap, but it is not zero — an
--   accepted, deliberate trade for uniform coverage (set-based scheduler
--   transitions, the reaper and admin verbs all announce themselves with no
--   Go-side call-site auditing).
--
-- * atMs is epoch milliseconds computed from now() (transaction time): a
--   jsonb-rendered timestamptz would depend on the session TimeZone.
--
-- * PG13 floor: CREATE OR REPLACE TRIGGER is PG14+, so idempotency (goose's
--   version table is not checksummed; convergence replays this file) uses
--   DROP TRIGGER IF EXISTS + CREATE TRIGGER, and CREATE OR REPLACE FUNCTION.
--   CREATE/DROP TRIGGER takes SHARE ROW EXCLUSIVE on its table — catalog
--   only, but it queues behind long-running writes and briefly blocks new
--   ones; on a busy install run this migration in a calm window.
--
-- * Scope: no DELETE triggers (vacuums and admin deletes emit nothing — a
--   refetch reconciles removals) and no trigger on azync_workflow_executions
--   (workflow-as-code JOB rows are covered via source='workflow'; execution
--   headers are a future migration).

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION azync_changes_jobs_ins_fn() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    r record;
    total bigint;
    at_ms bigint := (extract(epoch FROM now()) * 1000)::bigint;
BEGIN
    SELECT count(*) INTO total FROM new_rows;
    IF total = 0 THEN
        RETURN NULL;
    END IF;
    IF total > 50 THEN
        FOR r IN SELECT n.source, count(*) AS cnt FROM new_rows n GROUP BY n.source LOOP
            PERFORM pg_notify('azync_changes', jsonb_build_object(
                'schema', TG_TABLE_SCHEMA,
                'entity', 'job',
                'source', r.source,
                'bulk', true,
                'count', r.cnt,
                'atMs', at_ms)::text);
        END LOOP;
        RETURN NULL;
    END IF;
    FOR r IN SELECT * FROM new_rows LOOP
        PERFORM pg_notify('azync_changes', jsonb_strip_nulls(jsonb_build_object(
            'schema', TG_TABLE_SCHEMA,
            'entity', 'job',
            'source', r.source,
            'id', r.id,
            'dagId', r.dag_id,
            'kind', r.kind,
            'taskKey', r.task_key,
            'state', r.state,
            'atMs', at_ms))::text);
    END LOOP;
    RETURN NULL;
END $$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS azync_changes_jobs_ins ON azync_jobs;
CREATE TRIGGER azync_changes_jobs_ins
    AFTER INSERT ON azync_jobs
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION azync_changes_jobs_ins_fn();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION azync_changes_jobs_upd_fn() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    r record;
    total bigint;
    at_ms bigint := (extract(epoch FROM now()) * 1000)::bigint;
BEGIN
    SELECT count(*) INTO total
    FROM new_rows n JOIN old_rows o ON o.id = n.id
    WHERE o.state IS DISTINCT FROM n.state;
    IF total = 0 THEN
        RETURN NULL;
    END IF;
    IF total > 50 THEN
        FOR r IN
            SELECT n.source, count(*) AS cnt
            FROM new_rows n JOIN old_rows o ON o.id = n.id
            WHERE o.state IS DISTINCT FROM n.state
            GROUP BY n.source
        LOOP
            PERFORM pg_notify('azync_changes', jsonb_build_object(
                'schema', TG_TABLE_SCHEMA,
                'entity', 'job',
                'source', r.source,
                'bulk', true,
                'count', r.cnt,
                'atMs', at_ms)::text);
        END LOOP;
        RETURN NULL;
    END IF;
    FOR r IN
        SELECT n.*
        FROM new_rows n JOIN old_rows o ON o.id = n.id
        WHERE o.state IS DISTINCT FROM n.state
    LOOP
        PERFORM pg_notify('azync_changes', jsonb_strip_nulls(jsonb_build_object(
            'schema', TG_TABLE_SCHEMA,
            'entity', 'job',
            'source', r.source,
            'id', r.id,
            'dagId', r.dag_id,
            'kind', r.kind,
            'taskKey', r.task_key,
            'state', r.state,
            'atMs', at_ms))::text);
    END LOOP;
    RETURN NULL;
END $$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS azync_changes_jobs_upd ON azync_jobs;
CREATE TRIGGER azync_changes_jobs_upd
    AFTER UPDATE ON azync_jobs
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION azync_changes_jobs_upd_fn();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION azync_changes_dags_ins_fn() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    r record;
    total bigint;
    at_ms bigint := (extract(epoch FROM now()) * 1000)::bigint;
BEGIN
    SELECT count(*) INTO total FROM new_rows;
    IF total = 0 THEN
        RETURN NULL;
    END IF;
    IF total > 50 THEN
        PERFORM pg_notify('azync_changes', jsonb_build_object(
            'schema', TG_TABLE_SCHEMA,
            'entity', 'dag',
            'bulk', true,
            'count', total,
            'atMs', at_ms)::text);
        RETURN NULL;
    END IF;
    FOR r IN SELECT * FROM new_rows LOOP
        PERFORM pg_notify('azync_changes', jsonb_strip_nulls(jsonb_build_object(
            'schema', TG_TABLE_SCHEMA,
            'entity', 'dag',
            'id', r.id,
            'kind', r.name,
            'state', r.state,
            'atMs', at_ms))::text);
    END LOOP;
    RETURN NULL;
END $$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS azync_changes_dags_ins ON azync_dags;
CREATE TRIGGER azync_changes_dags_ins
    AFTER INSERT ON azync_dags
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION azync_changes_dags_ins_fn();

-- The DAG UPDATE filter also fires on a cancel_requested flip: CancelDAG on
-- an already-compensating DAG changes no state, but the requested cancel is
-- exactly what an operator watching the run needs to see.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION azync_changes_dags_upd_fn() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    r record;
    total bigint;
    at_ms bigint := (extract(epoch FROM now()) * 1000)::bigint;
BEGIN
    SELECT count(*) INTO total
    FROM new_rows n JOIN old_rows o ON o.id = n.id
    WHERE o.state IS DISTINCT FROM n.state
        OR o.cancel_requested IS DISTINCT FROM n.cancel_requested;
    IF total = 0 THEN
        RETURN NULL;
    END IF;
    IF total > 50 THEN
        PERFORM pg_notify('azync_changes', jsonb_build_object(
            'schema', TG_TABLE_SCHEMA,
            'entity', 'dag',
            'bulk', true,
            'count', total,
            'atMs', at_ms)::text);
        RETURN NULL;
    END IF;
    FOR r IN
        SELECT n.*
        FROM new_rows n JOIN old_rows o ON o.id = n.id
        WHERE o.state IS DISTINCT FROM n.state
            OR o.cancel_requested IS DISTINCT FROM n.cancel_requested
    LOOP
        PERFORM pg_notify('azync_changes', jsonb_strip_nulls(jsonb_build_object(
            'schema', TG_TABLE_SCHEMA,
            'entity', 'dag',
            'id', r.id,
            'kind', r.name,
            'state', r.state,
            'atMs', at_ms))::text);
    END LOOP;
    RETURN NULL;
END $$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS azync_changes_dags_upd ON azync_dags;
CREATE TRIGGER azync_changes_dags_upd
    AFTER UPDATE ON azync_dags
    REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION azync_changes_dags_upd_fn();

-- The ledger is append-only (its only other mutation is the retention
-- DELETE), so INSERT is the only meaningful trigger on azync_events.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION azync_changes_events_ins_fn() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    r record;
    total bigint;
    at_ms bigint := (extract(epoch FROM now()) * 1000)::bigint;
BEGIN
    SELECT count(*) INTO total FROM new_rows;
    IF total = 0 THEN
        RETURN NULL;
    END IF;
    IF total > 50 THEN
        PERFORM pg_notify('azync_changes', jsonb_build_object(
            'schema', TG_TABLE_SCHEMA,
            'entity', 'event',
            'bulk', true,
            'count', total,
            'atMs', at_ms)::text);
        RETURN NULL;
    END IF;
    FOR r IN SELECT * FROM new_rows LOOP
        PERFORM pg_notify('azync_changes', jsonb_strip_nulls(jsonb_build_object(
            'schema', TG_TABLE_SCHEMA,
            'entity', 'event',
            'id', r.id,
            'kind', r.type,
            'atMs', at_ms))::text);
    END LOOP;
    RETURN NULL;
END $$;
-- +goose StatementEnd

DROP TRIGGER IF EXISTS azync_changes_events_ins ON azync_events;
CREATE TRIGGER azync_changes_events_ins
    AFTER INSERT ON azync_events
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION azync_changes_events_ins_fn();

-- +goose Down
DROP TRIGGER IF EXISTS azync_changes_jobs_ins ON azync_jobs;
DROP TRIGGER IF EXISTS azync_changes_jobs_upd ON azync_jobs;
DROP TRIGGER IF EXISTS azync_changes_dags_ins ON azync_dags;
DROP TRIGGER IF EXISTS azync_changes_dags_upd ON azync_dags;
DROP TRIGGER IF EXISTS azync_changes_events_ins ON azync_events;
DROP FUNCTION IF EXISTS azync_changes_jobs_ins_fn();
DROP FUNCTION IF EXISTS azync_changes_jobs_upd_fn();
DROP FUNCTION IF EXISTS azync_changes_dags_ins_fn();
DROP FUNCTION IF EXISTS azync_changes_dags_upd_fn();
DROP FUNCTION IF EXISTS azync_changes_events_ins_fn();
