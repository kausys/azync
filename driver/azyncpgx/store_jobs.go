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
// by a random slot (0..StatsSlots-1) so concurrent business transactions do
// not serialize on a single hot (source, kind, day) row; readers SUM across
// slots. It runs on the caller's querier so the bump commits atomically with
// the operation. Used for the maintenance paths that touch many (source,
// kind) pairs at once (reap, dag); the single-job settlement paths
// (Ack/Reschedule/Dead) fuse their own bump into one statement instead — see
// ackSQL/rescheduleSQL/deadSQL.
func (s *Store) bumpStat(ctx context.Context, q querier, source driver.Source, kind string, field statField, n int64) error {
	column := field.column()
	//nolint:gosec // column is one of four hardcoded counter names, never user input
	sql := fmt.Sprintf(`INSERT INTO azync_stats_daily (source, kind, day, slot, %[1]s)
		VALUES ($1, $2, CURRENT_DATE, $3, $4)
		ON CONFLICT (source, kind, day, slot) DO UPDATE SET %[1]s = azync_stats_daily.%[1]s + EXCLUDED.%[1]s`, column)
	if _, err := q.Exec(ctx, sql, string(source), kind, s.randSlot(), n); err != nil {
		return fmt.Errorf("azyncpgx: bump stat %s: %w", column, err)
	}
	return nil
}

// randSlot picks a random stats shard in [0, StatsSlots).
func (s *Store) randSlot() int {
	//nolint:gosec // G404: slot only spreads write contention; it is not security-sensitive
	return rand.IntN(s.statsSlots)
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
	 idempotency_key, enqueued_at)
SELECT $1, 'queue', $2,
	CASE WHEN r.run_at > now() THEN 'scheduled' ELSE 'pending' END,
	r.run_at, $3, $4, $5::jsonb, $6::jsonb,
	$7, now()
FROM (SELECT COALESCE($8::timestamptz, now() + make_interval(secs => $9)) AS run_at) r
ON CONFLICT (source, kind, idempotency_key)
	WHERE idempotency_key IS NOT NULL AND state <> ALL (ARRAY['dead'::text, 'succeeded'::text])
DO NOTHING
RETURNING id`

// Enqueue durably inserts one queue job in its own short transaction and
// signals workers with a best-effort notify after commit (see
// notifyAfterCommit): a slow or failing notify can never abort the enqueue.
func (s *Store) Enqueue(ctx context.Context, p driver.EnqueueParams) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("azyncpgx: enqueue begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	inserted, err := s.enqueue(ctx, tx, p, false)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("azyncpgx: enqueue commit: %w", err)
	}
	if inserted {
		s.notifyAfterCommit(driver.SourceQueue, p.Kind) //nolint:contextcheck // deliberately independent of ctx, see notifyAfterCommit doc
	}
	return inserted, nil
}

// EnqueueTx performs Enqueue within the caller's transaction so the outbox
// commits atomically with the caller's own writes. The notify stays inside
// the caller's transaction here — that is the outbox contract: a wakeup is
// only ever sent if the caller's own transaction commits, and Postgres fires
// it exactly on that commit.
func (s *Store) EnqueueTx(ctx context.Context, tx pgx.Tx, p driver.EnqueueParams) (bool, error) {
	return s.enqueue(ctx, tx, p, true)
}

// enqueue inserts the job. notifyInTx selects which of the two outbox
// contracts above applies: true fires the notify inside q (EnqueueTx's own
// transaction); false leaves it to the caller to notify post-commit
// (Enqueue).
func (s *Store) enqueue(ctx context.Context, q querier, p driver.EnqueueParams, notifyInTx bool) (bool, error) {
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
		string(p.Payload), string(metaJSON), idem,
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
	if notifyInTx {
		if _, err := q.Exec(ctx, `SELECT pg_notify($1, $2)`, s.notifyChannel, notifyPayload(driver.SourceQueue, p.Kind)); err != nil {
			return false, fmt.Errorf("azyncpgx: enqueue notify: %w", err)
		}
	}
	return true, nil
}

// ---- fetch ----------------------------------------------------------------

// dequeueClaimBody is the shared SET/pick clause of the two dequeue statements.
// The first lease resolves the retry budget durably (max_attempts_explicit=true)
// so later workers with divergent defaults cannot overwrite it. SKIP LOCKED lets
// many workers claim disjoint batches without blocking. started_at moves with
// attempt: each lease begins a new attempt, so the column always describes the
// attempt currently reflected by state and completed_at.
const dequeueClaimBody = `
UPDATE azync_jobs j SET
	state = 'active',
	lease_until = now() + make_interval(secs => $1),
	lease_token = gen_random_uuid(),
	max_attempts = CASE WHEN j.max_attempts_explicit OR NOT $2 THEN j.max_attempts ELSE $3 END,
	max_attempts_explicit = true,
	attempt = j.attempt + 1,
	started_at = now(),
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

// ackSQL settles the job and bumps its daily 'processed' counter in one
// statement (one implicit, single-statement transaction) instead of an
// explicit BEGIN/UPDATE/INSERT/COMMIT round trip: the bump CTE runs only for
// the row(s) upd actually produced, so a fencing miss (zero rows) bumps
// nothing.
const ackSQL = `
WITH upd AS (
	UPDATE azync_jobs
	SET state = 'succeeded', lease_until = NULL, lease_token = NULL, completed_at = now(), updated_at = now()
	WHERE id = $1 AND state = 'active' AND lease_token = $2
	RETURNING source, kind
),
bump AS (
	INSERT INTO azync_stats_daily (source, kind, day, slot, processed)
	SELECT source, kind, CURRENT_DATE, $3, 1 FROM upd
	ON CONFLICT (source, kind, day, slot) DO UPDATE SET processed = azync_stats_daily.processed + 1
)
SELECT source, kind FROM upd`

// Ack completes an active job, retaining it as StateSucceeded. Clearing the
// lease and the partial idempotency index excluding 'succeeded' frees the
// live-job idempotency key exactly as a delete would.
func (s *Store) Ack(ctx context.Context, id, leaseToken uuid.UUID) error {
	var source, kind string
	err := s.pool.QueryRow(ctx, ackSQL, id, leaseToken, s.randSlot()).Scan(&source, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.NewNotFound("ack")
	}
	if err != nil {
		return fmt.Errorf("azyncpgx: ack: %w", err)
	}
	return nil
}

// rescheduleSQL records the failed attempt and bumps the daily 'failed'
// counter atomically with the transition, all in one statement: each CTE's
// RETURNING/SELECT chains off upd, so a fencing miss (zero rows) writes
// nothing anywhere.
const rescheduleSQL = `
WITH upd AS (
	UPDATE azync_jobs SET
		state = 'scheduled', run_at = now() + make_interval(secs => $3),
		lease_until = NULL, lease_token = NULL,
		last_error = $4, failed_at = now(), updated_at = now()
	WHERE id = $1 AND state = 'active' AND lease_token = $2
	RETURNING id, source, kind, attempt
),
ins AS (
	INSERT INTO azync_job_attempts (job_id, attempt, error)
	SELECT id, attempt, $4 FROM upd
),
bump AS (
	INSERT INTO azync_stats_daily (source, kind, day, slot, failed)
	SELECT source, kind, CURRENT_DATE, $5, 1 FROM upd
	ON CONFLICT (source, kind, day, slot) DO UPDATE SET failed = azync_stats_daily.failed + 1
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
	RETURNING id, source, kind, attempt
),
ins AS (
	INSERT INTO azync_job_attempts (job_id, attempt, error)
	SELECT id, attempt, $3 FROM upd
),
bump AS (
	INSERT INTO azync_stats_daily (source, kind, day, slot, failed)
	SELECT source, kind, CURRENT_DATE, $4, 1 FROM upd
	ON CONFLICT (source, kind, day, slot) DO UPDATE SET failed = azync_stats_daily.failed + 1
)
SELECT source, kind FROM upd`

// Dead moves a failed active job to StateDead and records the final attempt.
func (s *Store) Dead(ctx context.Context, id, leaseToken uuid.UUID, lastError string) error {
	return s.failTransition(ctx, "dead", deadSQL, id, leaseToken, lastError)
}

// skipSQL settles a deliberate no-op: terminal skipped, the reason retained
// as last_error for the ops surfaces, counted as processed (the task
// completed — it just had nothing to do), no attempts row (nothing failed).
const skipSQL = `
WITH upd AS (
	UPDATE azync_jobs SET
		state = 'skipped', lease_until = NULL, lease_token = NULL,
		last_error = $3, completed_at = now(), updated_at = now()
	WHERE id = $1 AND state = 'active' AND lease_token = $2
	RETURNING source, kind
),
bump AS (
	INSERT INTO azync_stats_daily (source, kind, day, slot, processed)
	SELECT source, kind, CURRENT_DATE, $4, 1 FROM upd
	ON CONFLICT (source, kind, day, slot) DO UPDATE SET processed = azync_stats_daily.processed + 1
)
SELECT source, kind FROM upd`

// Skip settles an active job as StateSkipped with the reason retained.
// Fenced by lease token.
func (s *Store) Skip(ctx context.Context, id, leaseToken uuid.UUID, reason string) error {
	return s.failTransition(ctx, "skip", skipSQL, id, leaseToken, reason)
}

// failTransition runs a fenced state change that counts as a failure,
// appending the caller's own randomly chosen stats slot so the statement's
// fused stats bump (see rescheduleSQL/deadSQL) lands on it.
func (s *Store) failTransition(ctx context.Context, op, sql string, args ...any) error {
	var source, kind string
	err := s.pool.QueryRow(ctx, sql, append(args, s.randSlot())...).Scan(&source, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.NewNotFound(op)
	}
	if err != nil {
		return fmt.Errorf("azyncpgx: %s: %w", op, err)
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

// snoozeSQL implements the polling-wait primitive: back to scheduled on the
// backend clock, handing back the attempt this lease charged (floored at zero)
// so snoozing never consumes the retry budget, with no attempts row.
//
// The snooze budget is enforced here, atomically, on the backend clock: the
// first snooze of a budgeted job stamps deadline_at (SET clauses read
// pre-update values, so the stamping snooze itself never escalates), and a
// snooze settled past a stamped deadline dead-letters the job instead —
// recording the final attempt and bumping the 'failed' stat exactly like
// deadSQL, fused so a fencing miss writes nothing anywhere.
const snoozeSQL = `
WITH upd AS (
	UPDATE azync_jobs SET
		state = CASE WHEN deadline_at IS NOT NULL AND now() >= deadline_at
			THEN 'dead' ELSE 'scheduled' END,
		run_at = CASE WHEN deadline_at IS NOT NULL AND now() >= deadline_at
			THEN run_at ELSE now() + make_interval(secs => $3) END,
		attempt = CASE WHEN deadline_at IS NOT NULL AND now() >= deadline_at
			THEN attempt ELSE GREATEST(attempt - 1, 0) END,
		last_error = CASE WHEN deadline_at IS NOT NULL AND now() >= deadline_at
			THEN $4 ELSE last_error END,
		failed_at = CASE WHEN deadline_at IS NOT NULL AND now() >= deadline_at
			THEN now() ELSE failed_at END,
		deadline_at = COALESCE(deadline_at, CASE WHEN snooze_budget IS NOT NULL
			THEN now() + make_interval(secs => snooze_budget) END),
		lease_until = NULL, lease_token = NULL, updated_at = now()
	WHERE id = $1 AND state = 'active' AND lease_token = $2
	RETURNING id, source, kind, attempt, (state = 'dead') AS deadlined
),
ins AS (
	INSERT INTO azync_job_attempts (job_id, attempt, error)
	SELECT id, attempt, $4 FROM upd WHERE deadlined
),
bump AS (
	INSERT INTO azync_stats_daily (source, kind, day, slot, failed)
	SELECT source, kind, CURRENT_DATE, $5, 1 FROM upd WHERE deadlined
	ON CONFLICT (source, kind, day, slot) DO UPDATE SET failed = azync_stats_daily.failed + 1
)
SELECT deadlined FROM upd`

// Snooze parks an active job as StateScheduled with run_at now()+delay without
// consuming the retry budget and without recording an attempt — unless the
// job's stamped snooze deadline has passed, in which case it dead-letters the
// job atomically (deadlined=true) with deadlineError as its final attempt.
// Fenced by lease token.
func (s *Store) Snooze(ctx context.Context, id, leaseToken uuid.UUID, delay time.Duration, deadlineError string) (bool, error) {
	var deadlined bool
	err := s.pool.QueryRow(ctx, snoozeSQL, id, leaseToken, delay.Seconds(), deadlineError, s.randSlot()).Scan(&deadlined)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, driver.NewNotFound("snooze")
	}
	if err != nil {
		return false, fmt.Errorf("azyncpgx: snooze: %w", err)
	}
	return deadlined, nil
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
// Nullable text columns are COALESCE'd so scan targets stay simple; the
// nullable payload and timestamps keep their NULLs for pgtype scanning.
func jobColumns(alias string) string {
	a := alias + "."
	return a + `id, ` + a + `source, ` + a + `kind, ` + a + `state, ` + a + `attempt, ` +
		a + `max_attempts, ` + a + `reap_count, ` + a + `payload::text, ` + a + `meta::text, ` +
		a + `run_at, ` + a + `lease_until, ` + a + `lease_token, COALESCE(` + a + `last_error, ''), ` +
		a + `event_id, ` + a + `replay, ` + a + `enqueued_at, ` + a + `failed_at, ` + a + `completed_at, ` +
		a + `dag_id, ` + a + `run_id, COALESCE(` + a + `task_key, ''), ` + a + `result::text, ` +
		`COALESCE(` + a + `signal_name, ''), COALESCE(` + a + `compensation_kind, ''), ` + a + `ignore_dead_deps, ` +
		`COALESCE(` + a + `snooze_budget, 0), ` + a + `deadline_at, ` + a + `started_at`
}

// eventColumns is the projected ledger column list under the given alias, used
// when a dequeue rehydrates an event delivery.
func eventColumns(alias string) string {
	a := alias + "."
	return a + `type, ` + a + `aggregate_type, ` + a + `aggregate_id, ` +
		a + `version, ` + a + `occurred_at, ` + a + `payload::text, ` + a + `meta::text`
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
	runAt       time.Time
	leaseUntil  pgtype.Timestamptz
	leaseToken  pgtype.UUID
	lastError   string
	eventID     pgtype.UUID
	replay      bool
	enqueuedAt  time.Time
	failedAt    pgtype.Timestamptz
	completedAt pgtype.Timestamptz
	// DAG / workflow-as-code columns.
	dagID            pgtype.UUID
	runID            pgtype.UUID
	taskKey          string
	result           *string
	signalName       string
	compensationKind string
	ignoreDeadDeps   bool
	snoozeBudget     float64
	deadlineAt       pgtype.Timestamptz
	startedAt        pgtype.Timestamptz
}

// scannedEvent is the raw scan target for eventColumns.
type scannedEvent struct {
	eventType     string
	aggregateType string
	aggregateID   string
	version       int64
	occurredAt    time.Time
	payload       string
	meta          string
}

func (sj *scannedJob) scanArgs() []any {
	return []any{
		&sj.id, &sj.source, &sj.kind, &sj.state, &sj.attempt, &sj.maxAttempts, &sj.reapCount,
		&sj.payload, &sj.meta, &sj.runAt, &sj.leaseUntil,
		&sj.leaseToken, &sj.lastError, &sj.eventID, &sj.replay, &sj.enqueuedAt, &sj.failedAt, &sj.completedAt,
		&sj.dagID, &sj.runID, &sj.taskKey, &sj.result, &sj.signalName, &sj.compensationKind, &sj.ignoreDeadDeps,
		&sj.snoozeBudget, &sj.deadlineAt, &sj.startedAt,
	}
}

func (se *scannedEvent) scanArgs() []any {
	return []any{
		&se.eventType, &se.aggregateType, &se.aggregateID, &se.version,
		&se.occurredAt, &se.payload, &se.meta,
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
	var result json.RawMessage
	if sj.result != nil {
		result = json.RawMessage(*sj.result)
	}
	return driver.Job{
		ID:               toUUID(sj.id),
		Source:           driver.Source(sj.source),
		Kind:             sj.kind,
		State:            driver.JobState(sj.state),
		Attempt:          sj.attempt,
		MaxAttempts:      sj.maxAttempts,
		ReapCount:        sj.reapCount,
		Payload:          payload,
		Meta:             meta,
		RunAt:            sj.runAt,
		LeaseUntil:       sj.leaseUntil.Time,
		LeaseToken:       toUUID(sj.leaseToken),
		LastError:        sj.lastError,
		EventID:          toUUID(sj.eventID),
		Replay:           sj.replay,
		EnqueuedAt:       sj.enqueuedAt,
		FailedAt:         sj.failedAt.Time,
		CompletedAt:      sj.completedAt.Time,
		DAGID:            toUUID(sj.dagID),
		RunID:            toUUID(sj.runID),
		TaskKey:          sj.taskKey,
		Result:           result,
		SignalName:       sj.signalName,
		CompensationKind: sj.compensationKind,
		IgnoreDeadDeps:   sj.ignoreDeadDeps,
		SnoozeBudget:     secondsToDuration(sj.snoozeBudget),
		DeadlineAt:       sj.deadlineAt.Time,
		StartedAt:        sj.startedAt.Time,
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
		AggregateType: se.aggregateType,
		AggregateID:   se.aggregateID,
		Version:       se.version,
		OccurredAt:    se.occurredAt,
		Payload:       json.RawMessage(se.payload),
		Meta:          meta,
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

// notifyTimeout bounds a post-commit notify so a stuck pool acquire or a slow
// backend cannot hang the caller indefinitely; NOTIFY itself is normally
// near-instant.
const notifyTimeout = 2 * time.Second

// notifyAfterCommit fires a best-effort wakeup after the caller's own
// transaction has already committed. Running it outside that transaction
// (rather than NOTIFY-inside-tx, fired only on commit by Postgres itself)
// means a slow or failing notify can never abort or add lock time to the
// caller's write. Wakeups are lossy by contract — driver.Notifier's polling
// fallback is the correctness path — so a failure here only delays pickup by
// one poll interval; it is logged, not returned.
func (s *Store) notifyAfterCommit(source driver.Source, kind string) {
	ctx, cancel := context.WithTimeout(context.Background(), notifyTimeout)
	defer cancel()
	if _, err := s.pool.Exec(ctx, `SELECT pg_notify($1, $2)`, s.notifyChannel, notifyPayload(source, kind)); err != nil {
		s.logger.Warn("post-commit notify failed; workers will still pick this up by polling",
			"source", string(source), "kind", kind, "error", err)
	}
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

func orEmptyMeta(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
