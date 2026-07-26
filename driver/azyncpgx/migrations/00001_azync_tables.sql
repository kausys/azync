-- +goose Up

-- azync_events is the append-only event ledger: the single source of truth for
-- an event's body. Event delivery jobs carry no payload; the Envelope is
-- rehydrated by joining this table on dequeue, so Replay reconstructs deliveries
-- without the original publish call.
CREATE TABLE azync_events (
    id             uuid PRIMARY KEY,
    type           text NOT NULL,
    tenant_id      uuid NULL,
    aggregate_type text NOT NULL DEFAULT '',
    aggregate_id   text NOT NULL DEFAULT '',
    version        bigint NOT NULL DEFAULT 0,
    occurred_at    timestamptz NOT NULL,
    payload        jsonb NOT NULL,
    meta           jsonb NOT NULL DEFAULT '{}',
    trace_id       text NULL,
    span_id        text NULL,
    trace_flags    smallint NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX azync_events_type_occurred_idx ON azync_events (type, occurred_at, id);
CREATE INDEX azync_events_occurred_idx ON azync_events (occurred_at, id);
CREATE INDEX azync_events_tenant_occurred_idx ON azync_events (tenant_id, occurred_at, id)
    WHERE tenant_id IS NOT NULL;

-- azync_subscribers binds a named consumer to an event type with its own retry
-- budget. Publish fans out one delivery per matching registration.
CREATE TABLE azync_subscribers (
    name         text NOT NULL,
    event_type   text NOT NULL,
    max_attempts integer NOT NULL CHECK (max_attempts > 0),
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (name, event_type)
);

-- azync_jobs is the unified job table ("everything is a job"). The source
-- discriminator partitions it: 'queue' rows are durable background jobs (payload
-- required); 'event' rows are per-subscriber deliveries (payload NULL, event_id
-- set, kind = subscriber name so fetch routing is identical to queue).
CREATE TABLE azync_jobs (
    id                    uuid PRIMARY KEY,
    source                text NOT NULL CHECK (source IN ('queue', 'event')),
    kind                  text NOT NULL,
    state                 text NOT NULL CHECK (state IN ('pending', 'scheduled', 'active', 'dead', 'paused', 'succeeded')),
    run_at                timestamptz NOT NULL,
    lease_until           timestamptz NULL,
    lease_token           uuid NULL,
    attempt               integer NOT NULL DEFAULT 0,
    max_attempts          integer NOT NULL,
    max_attempts_explicit boolean NOT NULL DEFAULT false,
    reap_count            integer NOT NULL DEFAULT 0,
    payload               jsonb NULL,
    meta                  jsonb NOT NULL DEFAULT '{}',
    trace_id              text NULL,
    span_id               text NULL,
    trace_flags           smallint NULL,
    idempotency_key       text NULL,
    event_id              uuid NULL REFERENCES azync_events (id) ON DELETE CASCADE,
    replay                boolean NOT NULL DEFAULT false,
    last_error            text NULL,
    enqueued_at           timestamptz NOT NULL,
    failed_at             timestamptz NULL,
    completed_at          timestamptz NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),
    -- Queue jobs must carry a payload; event deliveries rehydrate theirs from
    -- the ledger, so their payload is NULL.
    CONSTRAINT azync_jobs_payload_check CHECK (source <> 'queue' OR payload IS NOT NULL),
    -- event_id is set for exactly the event deliveries.
    CONSTRAINT azync_jobs_event_id_check CHECK ((source = 'event') = (event_id IS NOT NULL))
);
-- Partial per-state indexes with (source, kind) at the front so each fetch loop
-- and admin query hits a lean, state-scoped index.
CREATE INDEX azync_jobs_pending_idx ON azync_jobs (source, kind, run_at, id) WHERE state = 'pending';
CREATE INDEX azync_jobs_scheduled_idx ON azync_jobs (source, kind, run_at, id) WHERE state = 'scheduled';
CREATE INDEX azync_jobs_active_idx ON azync_jobs (source, kind, lease_until, id) WHERE state = 'active';
CREATE INDEX azync_jobs_dead_idx ON azync_jobs (source, kind, enqueued_at) WHERE state = 'dead';
CREATE INDEX azync_jobs_succeeded_idx ON azync_jobs (source, kind, completed_at) WHERE state = 'succeeded';
-- Live-job idempotency: a duplicate is rejected only while a prior job with the
-- key is still alive (excludes the terminal dead/succeeded states).
CREATE UNIQUE INDEX azync_jobs_idempotency_idx ON azync_jobs (source, kind, idempotency_key)
    WHERE idempotency_key IS NOT NULL AND state <> ALL (ARRAY['dead'::text, 'succeeded'::text]);
CREATE INDEX azync_jobs_event_idx ON azync_jobs (event_id) WHERE event_id IS NOT NULL;

-- azync_job_attempts is the normalized per-attempt failure history (one row per
-- failed reschedule, exhaustion or reap). The source is reachable by joining
-- azync_jobs, so no source column is duplicated here.
CREATE TABLE azync_job_attempts (
    id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id    uuid NOT NULL REFERENCES azync_jobs (id) ON DELETE CASCADE,
    attempt   integer NOT NULL,
    error     text NOT NULL,
    trace     text NULL,
    failed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX azync_job_attempts_job_idx ON azync_job_attempts (job_id, attempt);

-- azync_idempotency_keys holds time-window dedupe reservations (IdempotencyTTL):
-- they survive job completion, unlike the live-job unique index, and are
-- vacuumed by the maintenance loop.
CREATE TABLE azync_idempotency_keys (
    source     text NOT NULL,
    kind       text NOT NULL,
    key        text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (source, kind, key)
);
CREATE INDEX azync_idempotency_expires_idx ON azync_idempotency_keys (expires_at);

-- azync_stats_daily holds per-kind daily throughput counters, slot-sharded
-- (0..7) so concurrent business transactions do not serialize on a single hot
-- (source, kind, day) row; readers SUM across slots.
CREATE TABLE azync_stats_daily (
    source    text NOT NULL,
    kind      text NOT NULL,
    day       date NOT NULL,
    slot      smallint NOT NULL DEFAULT 0,
    enqueued  bigint NOT NULL DEFAULT 0,
    processed bigint NOT NULL DEFAULT 0,
    failed    bigint NOT NULL DEFAULT 0,
    reaped    bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (source, kind, day, slot)
);

-- +goose Down
DROP TABLE azync_stats_daily;
DROP TABLE azync_idempotency_keys;
DROP TABLE azync_job_attempts;
DROP TABLE azync_jobs;
DROP TABLE azync_subscribers;
DROP TABLE azync_events;
