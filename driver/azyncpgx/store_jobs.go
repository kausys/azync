package azyncpgx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// ---- statistics -----------------------------------------------------------

// statField selects one of the four daily counter columns.
type statField int

const (
	statEnqueued statField = iota
	statProcessed
	statFailed
	statReaped
)

// column returns the counter column name for the field. The value is one of a
// fixed, closed set, never user input.
func (f statField) column() string {
	switch f {
	case statProcessed:
		return "processed"
	case statFailed:
		return "failed"
	case statReaped:
		return "reaped"
	case statEnqueued:
		return "enqueued"
	default:
		return "enqueued"
	}
}

// bumpStat increments one daily counter for (source, kind). The row is sharded
// by a random slot (0..7) so concurrent business transactions do not serialize
// on a single hot (source, kind, day) row; readers SUM across slots. It runs on
// the caller's querier so the bump commits atomically with the operation.
func (s *Store) bumpStat(ctx context.Context, q querier, source driver.Source, kind string, field statField, n int64) error {
	column := field.column()
	//nolint:gosec // column is one of four hardcoded counter names, never user input
	sql := fmt.Sprintf(`INSERT INTO azync_stats_daily (source, kind, day, slot, %[1]s)
		VALUES ($1, $2, CURRENT_DATE, $3, $4)
		ON CONFLICT (source, kind, day, slot) DO UPDATE SET %[1]s = azync_stats_daily.%[1]s + EXCLUDED.%[1]s`, column)
	//nolint:gosec // G404: slot only spreads write contention; it is not security-sensitive
	slot := rand.IntN(8)
	if _, err := q.Exec(ctx, sql, string(source), kind, slot, n); err != nil {
		return fmt.Errorf("azyncpgx: bump stat %s: %w", column, err)
	}
	return nil
}

// ---- producer -------------------------------------------------------------

const enqueueClaimSQL = `
INSERT INTO azync_idempotency_keys (source, kind, key, expires_at)
VALUES ('queue', $1, $2, now() + make_interval(secs => $3))
ON CONFLICT (source, kind, key) DO UPDATE SET expires_at = EXCLUDED.expires_at
WHERE azync_idempotency_keys.expires_at < now()
RETURNING kind`

// enqueueInsertSQL resolves run_at and the pending/scheduled split against the
// backend clock: a client clock skewed fast could otherwise stamp a "run now"
// job that sits invisible to dequeue. The partial-index predicate on ON CONFLICT
// matches azync_jobs_idempotency_idx exactly (live-job dedupe).
const enqueueInsertSQL = `
INSERT INTO azync_jobs
	(id, source, kind, state, run_at, max_attempts, max_attempts_explicit, payload, meta,
	 trace_id, span_id, trace_flags, idempotency_key, enqueued_at)
SELECT $1, 'queue', $2,
	CASE WHEN r.run_at > now() THEN 'scheduled' ELSE 'pending' END,
	r.run_at, $3, $4, $5::jsonb, $6::jsonb,
	NULLIF($7, ''), NULLIF($8, ''), $9, $10, now()
FROM (SELECT COALESCE($11::timestamptz, now() + make_interval(secs => $12)) AS run_at) r
ON CONFLICT (source, kind, idempotency_key)
	WHERE idempotency_key IS NOT NULL AND state <> ALL (ARRAY['dead'::text, 'succeeded'::text])
DO NOTHING
RETURNING id`

// Enqueue durably inserts one queue job in its own short transaction and signals
// workers after commit.
func (s *Store) Enqueue(ctx context.Context, p driver.EnqueueParams) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("azyncpgx: enqueue begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	inserted, err := s.enqueue(ctx, tx, p)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("azyncpgx: enqueue commit: %w", err)
	}
	return inserted, nil
}

// EnqueueTx performs Enqueue within the caller's transaction so the outbox
// commits atomically with the caller's own writes.
func (s *Store) EnqueueTx(ctx context.Context, tx pgx.Tx, p driver.EnqueueParams) (bool, error) {
	return s.enqueue(ctx, tx, p)
}

func (s *Store) enqueue(ctx context.Context, q querier, p driver.EnqueueParams) (bool, error) {
	metaJSON, err := json.Marshal(orEmptyMeta(p.Meta))
	if err != nil {
		return false, fmt.Errorf("azyncpgx: marshal meta: %w", err)
	}

	// Time-window dedupe: claim the key first; a live, unexpired claim is a
	// duplicate. The claim value expiry is computed DB-side.
	if p.IdempotencyKey != "" && p.IdempotencyTTL > 0 {
		var claimed string
		err := q.QueryRow(ctx, enqueueClaimSQL, p.Kind, p.IdempotencyKey, p.IdempotencyTTL.Seconds()).Scan(&claimed)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil // deduplicated by the time window
		}
		if err != nil {
			return false, fmt.Errorf("azyncpgx: idempotency claim: %w", err)
		}
	}

	var idem any
	if p.IdempotencyKey != "" {
		idem = p.IdempotencyKey
	}

	var id pgtype.UUID
	err = q.QueryRow(ctx, enqueueInsertSQL,
		p.ID, p.Kind, p.MaxAttempts, p.MaxAttemptsExplicit,
		string(p.Payload), string(metaJSON),
		p.TraceID, p.SpanID, nullableTraceFlags(p.TraceID, p.TraceFlags), idem,
		nullableTime(p.RunAt), p.Delay.Seconds(),
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // deduplicated by the live-job unique index
	}
	if err != nil {
		return false, fmt.Errorf("azyncpgx: enqueue insert: %w", err)
	}

	if err := s.bumpStat(ctx, q, driver.SourceQueue, p.Kind, statEnqueued, 1); err != nil {
		return false, err
	}
	// Outbox: NOTIFY inside the transaction fires only after commit.
	if _, err := q.Exec(ctx, `SELECT pg_notify($1, $2)`, s.notifyChannel, notifyPayload(driver.SourceQueue, p.Kind)); err != nil {
		return false, fmt.Errorf("azyncpgx: enqueue notify: %w", err)
	}
	return true, nil
}

// ---- fetch ----------------------------------------------------------------

// dequeueClaimBody is the shared SET/pick clause of the two dequeue statements.
// The first lease resolves the retry budget durably (max_attempts_explicit=true)
// so later workers with divergent defaults cannot overwrite it. SKIP LOCKED lets
// many workers claim disjoint batches without blocking.
const dequeueClaimBody = `
UPDATE azync_jobs j SET
	state = 'active',
	lease_until = now() + make_interval(secs => $1),
	lease_token = gen_random_uuid(),
	max_attempts = CASE WHEN j.max_attempts_explicit OR NOT $2 THEN j.max_attempts ELSE $3 END,
	max_attempts_explicit = true,
	attempt = j.attempt + 1,
	updated_at = now()
FROM (
	SELECT id FROM azync_jobs
	WHERE source = $4 AND kind = $5 AND state = 'pending' AND run_at <= now()
	ORDER BY run_at, id
	FOR UPDATE SKIP LOCKED
	LIMIT $6
) picked`

var (
	dequeueQueueSQL = dequeueClaimBody + `
WHERE j.id = picked.id
RETURNING ` + jobColumns("j")

	// The event path joins the ledger so each delivery's Envelope is rehydrated
	// in the same claim (payload lives in azync_events, not the delivery row).
	dequeueEventSQL = dequeueClaimBody + `, azync_events e
WHERE j.id = picked.id AND e.id = j.event_id
RETURNING ` + jobColumns("j") + `, ` + eventColumns("e")
)

// DequeueBatch leases up to p.Limit due pending jobs of (source, p.Kind).
func (s *Store) DequeueBatch(ctx context.Context, source driver.Source, p driver.DequeueParams) ([]driver.Job, error) {
	if p.Limit <= 0 {
		return nil, nil
	}
	if source == driver.SourceEvent {
		return s.dequeueEvents(ctx, p)
	}
	rows, err := s.pool.Query(ctx, dequeueQueueSQL,
		p.Lease.Seconds(), p.OverrideDefault, p.DefaultMaxAttempts, string(source), p.Kind, p.Limit)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: dequeue: %w", err)
	}
	return collectJobs(rows)
}

func (s *Store) dequeueEvents(ctx context.Context, p driver.DequeueParams) ([]driver.Job, error) {
	rows, err := s.pool.Query(ctx, dequeueEventSQL,
		p.Lease.Seconds(), p.OverrideDefault, p.DefaultMaxAttempts, string(driver.SourceEvent), p.Kind, p.Limit)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: dequeue events: %w", err)
	}
	defer rows.Close()
	var out []driver.Job
	for rows.Next() {
		var (
			sj scannedJob
			se scannedEvent
		)
		if err := scanJobEventRow(rows, &sj, &se); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan event delivery: %w", err)
		}
		job, err := sj.toJob()
		if err != nil {
			return nil, err
		}
		rec, err := se.toRecord(job.EventID)
		if err != nil {
			return nil, err
		}
		job.Event = &rec
		out = append(out, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate event deliveries: %w", err)
	}
	return out, nil
}

// ---- settlement (lease-token fenced) --------------------------------------

const ackSQL = `
UPDATE azync_jobs
SET state = 'succeeded', lease_until = NULL, lease_token = NULL, completed_at = now(), updated_at = now()
WHERE id = $1 AND state = 'active' AND lease_token = $2
RETURNING source, kind`

// Ack completes an active job, retaining it as StateSucceeded. Clearing the
// lease and the partial idempotency index excluding 'succeeded' frees the
// live-job idempotency key exactly as a delete would.
func (s *Store) Ack(ctx context.Context, id, leaseToken uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("azyncpgx: ack begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var source, kind string
	err = tx.QueryRow(ctx, ackSQL, id, leaseToken).Scan(&source, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.NewNotFound("ack")
	}
	if err != nil {
		return fmt.Errorf("azyncpgx: ack: %w", err)
	}
	if err := s.bumpStat(ctx, tx, driver.Source(source), kind, statProcessed, 1); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("azyncpgx: ack commit: %w", err)
	}
	return nil
}

// rescheduleSQL records the failed attempt atomically with the transition: the
// UPDATE's RETURNING feeds the attempts INSERT, so a row is written only if the
// fenced transition actually applied.
const rescheduleSQL = `
WITH upd AS (
	UPDATE azync_jobs SET
		state = 'scheduled', run_at = now() + make_interval(secs => $3),
		lease_until = NULL, lease_token = NULL,
		last_error = $4, failed_at = now(), updated_at = now()
	WHERE id = $1 AND state = 'active' AND lease_token = $2
	RETURNING id, source, kind, attempt, trace_id
),
ins AS (
	INSERT INTO azync_job_attempts (job_id, attempt, error, trace)
	SELECT id, attempt, $4, trace_id FROM upd
)
SELECT source, kind FROM upd`

// Reschedule parks a failed active job as StateScheduled and records the attempt.
func (s *Store) Reschedule(ctx context.Context, id, leaseToken uuid.UUID, delay time.Duration, lastError string) error {
	return s.failTransition(ctx, "reschedule", rescheduleSQL, id, leaseToken, delay.Seconds(), lastError)
}

const deadSQL = `
WITH upd AS (
	UPDATE azync_jobs SET
		state = 'dead', lease_until = NULL, lease_token = NULL,
		last_error = $3, failed_at = now(), updated_at = now()
	WHERE id = $1 AND state = 'active' AND lease_token = $2
	RETURNING id, source, kind, attempt, trace_id
),
ins AS (
	INSERT INTO azync_job_attempts (job_id, attempt, error, trace)
	SELECT id, attempt, $3, trace_id FROM upd
)
SELECT source, kind FROM upd`

// Dead moves a failed active job to StateDead and records the final attempt.
func (s *Store) Dead(ctx context.Context, id, leaseToken uuid.UUID, lastError string) error {
	return s.failTransition(ctx, "dead", deadSQL, id, leaseToken, lastError)
}

// failTransition runs a fenced state change that counts as a failure and bumps
// the daily 'failed' counter for the returned (source, kind) in the same tx.
func (s *Store) failTransition(ctx context.Context, op, sql string, args ...any) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("azyncpgx: %s begin: %w", op, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var source, kind string
	err = tx.QueryRow(ctx, sql, args...).Scan(&source, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.NewNotFound(op)
	}
	if err != nil {
		return fmt.Errorf("azyncpgx: %s: %w", op, err)
	}
	if err := s.bumpStat(ctx, tx, driver.Source(source), kind, statFailed, 1); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("azyncpgx: %s commit: %w", op, err)
	}
	return nil
}

const releaseSQL = `
UPDATE azync_jobs SET
	state = 'pending', attempt = GREATEST(attempt - 1, 0),
	lease_until = NULL, lease_token = NULL, run_at = now(), updated_at = now()
WHERE id = $1 AND state = 'active' AND lease_token = $2`

// Release returns a leased job to StatePending, decrementing the attempt it did
// not really spend, without recording an attempt. Fenced by lease token.
func (s *Store) Release(ctx context.Context, id, leaseToken uuid.UUID) error {
	return s.requireRow(ctx, "release", releaseSQL, id, leaseToken)
}

const extendLeaseSQL = `
UPDATE azync_jobs SET lease_until = now() + make_interval(secs => $3), updated_at = now()
WHERE id = $1 AND state = 'active' AND lease_token = $2`

// ExtendLease renews an active job's lease. Fenced by lease token.
func (s *Store) ExtendLease(ctx context.Context, id, leaseToken uuid.UUID, lease time.Duration) error {
	return s.requireRow(ctx, "extend lease", extendLeaseSQL, id, leaseToken, lease.Seconds())
}

// requireRow runs a single-statement update/delete and maps a zero-rows result
// to the contract's not-found error.
func (s *Store) requireRow(ctx context.Context, op, sql string, args ...any) error {
	tag, err := s.pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("azyncpgx: %s: %w", op, err)
	}
	if tag.RowsAffected() == 0 {
		return driver.NewNotFound(op)
	}
	return nil
}

// ---- row scanning ---------------------------------------------------------

// jobColumns is the projected column list for a job row under the given alias.
// Nullable text/flag columns are COALESCE'd so scan targets stay simple; the
// nullable payload and timestamps keep their NULLs for pgtype scanning.
func jobColumns(alias string) string {
	a := alias + "."
	return a + `id, ` + a + `source, ` + a + `kind, ` + a + `state, ` + a + `attempt, ` +
		a + `max_attempts, ` + a + `reap_count, ` + a + `payload::text, ` + a + `meta::text, ` +
		`COALESCE(` + a + `trace_id, ''), COALESCE(` + a + `span_id, ''), COALESCE(` + a + `trace_flags, 0), ` +
		a + `run_at, ` + a + `lease_until, ` + a + `lease_token, COALESCE(` + a + `last_error, ''), ` +
		a + `event_id, ` + a + `replay, ` + a + `enqueued_at, ` + a + `failed_at, ` + a + `completed_at`
}

// eventColumns is the projected ledger column list under the given alias, used
// when a dequeue rehydrates an event delivery.
func eventColumns(alias string) string {
	a := alias + "."
	return a + `type, ` + a + `tenant_id, ` + a + `aggregate_type, ` + a + `aggregate_id, ` +
		a + `version, ` + a + `occurred_at, ` + a + `payload::text, ` + a + `meta::text, ` +
		`COALESCE(` + a + `trace_id, ''), COALESCE(` + a + `span_id, ''), COALESCE(` + a + `trace_flags, 0)`
}

// scannedJob is the raw scan target for jobColumns. Nullable columns land as
// pgtype wrappers (or a nil *string for payload); the rest as plain values.
type scannedJob struct {
	id          pgtype.UUID
	source      string
	kind        string
	state       string
	attempt     int
	maxAttempts int
	reapCount   int
	payload     *string
	meta        string
	traceID     string
	spanID      string
	traceFlags  int16
	runAt       time.Time
	leaseUntil  pgtype.Timestamptz
	leaseToken  pgtype.UUID
	lastError   string
	eventID     pgtype.UUID
	replay      bool
	enqueuedAt  time.Time
	failedAt    pgtype.Timestamptz
	completedAt pgtype.Timestamptz
}

// scannedEvent is the raw scan target for eventColumns.
type scannedEvent struct {
	eventType     string
	tenantID      pgtype.UUID
	aggregateType string
	aggregateID   string
	version       int64
	occurredAt    time.Time
	payload       string
	meta          string
	traceID       string
	spanID        string
	traceFlags    int16
}

func (sj *scannedJob) scanArgs() []any {
	return []any{
		&sj.id, &sj.source, &sj.kind, &sj.state, &sj.attempt, &sj.maxAttempts, &sj.reapCount,
		&sj.payload, &sj.meta, &sj.traceID, &sj.spanID, &sj.traceFlags, &sj.runAt, &sj.leaseUntil,
		&sj.leaseToken, &sj.lastError, &sj.eventID, &sj.replay, &sj.enqueuedAt, &sj.failedAt, &sj.completedAt,
	}
}

func (se *scannedEvent) scanArgs() []any {
	return []any{
		&se.eventType, &se.tenantID, &se.aggregateType, &se.aggregateID, &se.version,
		&se.occurredAt, &se.payload, &se.meta, &se.traceID, &se.spanID, &se.traceFlags,
	}
}

func scanJobRow(rows pgx.Rows, sj *scannedJob) error {
	return rows.Scan(sj.scanArgs()...)
}

func scanJobEventRow(rows pgx.Rows, sj *scannedJob, se *scannedEvent) error {
	return rows.Scan(append(sj.scanArgs(), se.scanArgs()...)...)
}

func (sj *scannedJob) toJob() (driver.Job, error) {
	meta, err := decodeMeta(sj.meta)
	if err != nil {
		return driver.Job{}, fmt.Errorf("azyncpgx: decode job meta for %s: %w", toUUID(sj.id), err)
	}
	var payload json.RawMessage
	if sj.payload != nil {
		payload = json.RawMessage(*sj.payload)
	}
	return driver.Job{
		ID:          toUUID(sj.id),
		Source:      driver.Source(sj.source),
		Kind:        sj.kind,
		State:       driver.JobState(sj.state),
		Attempt:     sj.attempt,
		MaxAttempts: sj.maxAttempts,
		ReapCount:   sj.reapCount,
		Payload:     payload,
		Meta:        meta,
		TraceID:     sj.traceID,
		SpanID:      sj.spanID,
		TraceFlags:  sj.traceFlags,
		RunAt:       sj.runAt,
		LeaseUntil:  sj.leaseUntil.Time,
		LeaseToken:  toUUID(sj.leaseToken),
		LastError:   sj.lastError,
		EventID:     toUUID(sj.eventID),
		Replay:      sj.replay,
		EnqueuedAt:  sj.enqueuedAt,
		FailedAt:    sj.failedAt.Time,
		CompletedAt: sj.completedAt.Time,
	}, nil
}

func (se *scannedEvent) toRecord(id uuid.UUID) (driver.EventRecord, error) {
	meta, err := decodeMeta(se.meta)
	if err != nil {
		return driver.EventRecord{}, fmt.Errorf("azyncpgx: decode event meta for %s: %w", id, err)
	}
	return driver.EventRecord{
		ID:            id,
		Type:          se.eventType,
		TenantID:      toUUID(se.tenantID),
		AggregateType: se.aggregateType,
		AggregateID:   se.aggregateID,
		Version:       se.version,
		OccurredAt:    se.occurredAt,
		Payload:       json.RawMessage(se.payload),
		Meta:          meta,
		TraceID:       se.traceID,
		SpanID:        se.spanID,
		TraceFlags:    se.traceFlags,
	}, nil
}

func collectJobs(rows pgx.Rows) ([]driver.Job, error) {
	defer rows.Close()
	var out []driver.Job
	for rows.Next() {
		var sj scannedJob
		if err := scanJobRow(rows, &sj); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan job: %w", err)
		}
		job, err := sj.toJob()
		if err != nil {
			return nil, err
		}
		out = append(out, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate jobs: %w", err)
	}
	return out, nil
}

// ---- small helpers --------------------------------------------------------

func decodeMeta(raw string) (map[string]string, error) {
	meta := map[string]string{}
	if raw == "" || raw == "null" {
		return meta, nil
	}
	if err := json.Unmarshal([]byte(raw), &meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func notifyPayload(source driver.Source, kind string) string {
	return string(source) + ":" + kind
}

// toUUID converts a pgtype.UUID to a uuid.UUID, mapping SQL NULL to uuid.Nil.
func toUUID(p pgtype.UUID) uuid.UUID {
	if !p.Valid {
		return uuid.Nil
	}
	return p.Bytes
}

// nullableUUID maps uuid.Nil to a SQL NULL argument and any other value through.
func nullableUUID(id uuid.UUID) any {
	if id == uuid.Nil {
		return nil
	}
	return id
}

// nullableTime maps a zero time to a SQL NULL argument.
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// nullableString maps an empty string to a SQL NULL argument.
func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

// nullableTraceFlags carries flags only alongside a trace id: flags 0 with a
// trace id is a valid unsampled trace, so presence keys off the id.
func nullableTraceFlags(traceID string, flags int16) any {
	if traceID == "" {
		return nil
	}
	return flags
}

func orEmptyMeta(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
