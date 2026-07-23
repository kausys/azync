package azyncpgx

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// ---- job admin: kinds and depths ------------------------------------------

const listKindsSQL = `
SELECT kind FROM (
	SELECT DISTINCT kind FROM azync_jobs WHERE source = $1
	UNION
	SELECT DISTINCT kind FROM azync_stats_daily WHERE source = $1
) k ORDER BY kind`

// ListKinds returns the distinct kinds of the source (from live jobs and stat
// history), sorted.
func (s *Store) ListKinds(ctx context.Context, source driver.Source) ([]string, error) {
	rows, err := s.pool.Query(ctx, listKindsSQL, string(source))
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: list kinds: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan kind: %w", err)
		}
		out = append(out, kind)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate kinds: %w", err)
	}
	return out, nil
}

const kindDepthsSQL = `SELECT kind, state, COUNT(*)::bigint FROM azync_jobs WHERE source = $1 GROUP BY kind, state`

// KindDepths returns per-kind instantaneous state counters of the source.
func (s *Store) KindDepths(ctx context.Context, source driver.Source) (map[string]driver.Depths, error) {
	rows, err := s.pool.Query(ctx, kindDepthsSQL, string(source))
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: kind depths: %w", err)
	}
	defer rows.Close()
	out := map[string]driver.Depths{}
	for rows.Next() {
		var (
			kind, state string
			count       int64
		)
		if err := rows.Scan(&kind, &state, &count); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan kind depth: %w", err)
		}
		depths := out[kind]
		addDepth(&depths, driver.JobState(state), count)
		out[kind] = depths
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate kind depths: %w", err)
	}
	return out, nil
}

const statsDepthsSQL = `SELECT state, COUNT(*)::bigint FROM azync_jobs WHERE source = $1 AND kind = $2 GROUP BY state`

const statsDailySQL = `
SELECT day, SUM(enqueued)::bigint, SUM(processed)::bigint, SUM(failed)::bigint, SUM(reaped)::bigint
FROM azync_stats_daily WHERE source = $1 AND kind = $2 GROUP BY day ORDER BY day`

// Stats returns one kind's instantaneous depths and its daily throughput window,
// oldest day first.
func (s *Store) Stats(ctx context.Context, source driver.Source, kind string) (driver.Depths, []driver.DailyCount, error) {
	rows, err := s.pool.Query(ctx, statsDepthsSQL, string(source), kind)
	if err != nil {
		return driver.Depths{}, nil, fmt.Errorf("azyncpgx: stats depths: %w", err)
	}
	depths, err := scanDepths(rows)
	if err != nil {
		return driver.Depths{}, nil, fmt.Errorf("azyncpgx: scan stats depths: %w", err)
	}
	daily, err := s.queryDaily(ctx, statsDailySQL, string(source), kind)
	if err != nil {
		return driver.Depths{}, nil, err
	}
	return depths, daily, nil
}

const allDailySQL = `
SELECT day, SUM(enqueued)::bigint, SUM(processed)::bigint, SUM(failed)::bigint, SUM(reaped)::bigint
FROM azync_stats_daily WHERE source = $1 GROUP BY day ORDER BY day`

// AllDaily returns the daily throughput window summed across every kind of the
// source, oldest day first.
func (s *Store) AllDaily(ctx context.Context, source driver.Source) ([]driver.DailyCount, error) {
	return s.queryDaily(ctx, allDailySQL, string(source))
}

func (s *Store) queryDaily(ctx context.Context, sql string, args ...any) ([]driver.DailyCount, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: daily stats: %w", err)
	}
	defer rows.Close()
	var out []driver.DailyCount
	for rows.Next() {
		var dc driver.DailyCount
		if err := rows.Scan(&dc.Date, &dc.Enqueued, &dc.Processed, &dc.Failed, &dc.Reaped); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan daily stat: %w", err)
		}
		out = append(out, dc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate daily stats: %w", err)
	}
	return out, nil
}

func scanDepths(rows pgx.Rows) (driver.Depths, error) {
	defer rows.Close()
	var d driver.Depths
	for rows.Next() {
		var (
			state string
			count int64
		)
		if err := rows.Scan(&state, &count); err != nil {
			return driver.Depths{}, err
		}
		addDepth(&d, driver.JobState(state), count)
	}
	return d, rows.Err()
}

func addDepth(d *driver.Depths, state driver.JobState, count int64) {
	switch state {
	case driver.StatePending:
		d.Pending = count
	case driver.StateScheduled:
		d.Scheduled = count
	case driver.StateActive:
		d.Active = count
	case driver.StateDead:
		d.Dead = count
	case driver.StatePaused:
		d.Paused = count
	case driver.StateSucceeded:
		d.Succeeded = count
	default:
	}
}

// ---- job admin: listing and detail ----------------------------------------

// ListJobs lists jobs of the source matching filter, paginated. Ordering depends
// on filter.State (see the driver.Store contract); a limit <= 0 means unbounded.
func (s *Store) ListJobs(ctx context.Context, source driver.Source, filter driver.JobFilter, offset, limit int) ([]driver.Job, int64, error) {
	where := "source = $1"
	args := []any{string(source)}
	if filter.Kind != "" {
		args = append(args, filter.Kind)
		where += " AND kind = $" + strconv.Itoa(len(args))
	}
	if filter.State != "" {
		args = append(args, string(filter.State))
		where += " AND state = $" + strconv.Itoa(len(args))
	}

	var total int64
	// where is built from fixed fragments and bound parameters only.
	//nolint:gosec // G202: no user-controlled SQL identifier is interpolated
	if err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM azync_jobs WHERE "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("azyncpgx: list jobs count: %w", err)
	}

	if offset < 0 {
		offset = 0
	}
	limitIdx := len(args) + 1
	offsetIdx := len(args) + 2
	args = append(args, limitArg(limit), offset)
	// where and order are fixed fragments; only bound parameters vary.
	//nolint:gosec // G202: no user-controlled SQL identifier is interpolated
	sql := "SELECT " + jobColumns("azync_jobs") + " FROM azync_jobs WHERE " + where +
		" ORDER BY " + orderForState(filter.State) +
		" LIMIT $" + strconv.Itoa(limitIdx) + " OFFSET $" + strconv.Itoa(offsetIdx)
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("azyncpgx: list jobs: %w", err)
	}
	jobs, err := collectJobs(rows)
	if err != nil {
		return nil, 0, err
	}
	return jobs, total, nil
}

// orderForState returns the ORDER BY clause the contract mandates for a state
// filter (broken by id for stability).
func orderForState(state driver.JobState) string {
	switch state {
	case driver.StateScheduled, driver.StatePaused:
		return "run_at, id"
	case driver.StateActive:
		return "lease_until, id"
	case driver.StateSucceeded:
		return "completed_at DESC, id"
	case driver.StatePending, driver.StateDead:
		return "enqueued_at, id"
	default: // no state filter: newest first for admin browsing
		return "enqueued_at DESC, id"
	}
}

// limitArg maps a non-positive limit to a NULL argument (LIMIT NULL is
// unbounded) and any positive limit through.
func limitArg(limit int) any {
	if limit <= 0 {
		return nil
	}
	return limit
}

var getJobSQL = `SELECT ` + jobColumns("azync_jobs") + ` FROM azync_jobs WHERE id = $1 AND source = $2`

// GetJob returns a single job of the source by id, or a not-found error.
func (s *Store) GetJob(ctx context.Context, source driver.Source, id uuid.UUID) (*driver.Job, error) {
	rows, err := s.pool.Query(ctx, getJobSQL, id, string(source))
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: get job: %w", err)
	}
	jobs, err := collectJobs(rows)
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, driver.NewNotFound("get job")
	}
	return &jobs[0], nil
}

const jobAttemptsSQL = `
SELECT a.attempt, a.error, a.failed_at, COALESCE(a.trace, '')
FROM azync_job_attempts a JOIN azync_jobs j ON j.id = a.job_id
WHERE a.job_id = $1 AND j.source = $2
ORDER BY a.attempt, a.id`

// JobAttempts returns a job's failure history, oldest attempt first.
func (s *Store) JobAttempts(ctx context.Context, source driver.Source, id uuid.UUID) ([]driver.AttemptError, error) {
	rows, err := s.pool.Query(ctx, jobAttemptsSQL, id, string(source))
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: job attempts: %w", err)
	}
	defer rows.Close()
	var out []driver.AttemptError
	for rows.Next() {
		var a driver.AttemptError
		if err := rows.Scan(&a.Attempt, &a.Error, &a.At, &a.Trace); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan attempt: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate attempts: %w", err)
	}
	return out, nil
}

// ---- job admin: mutations -------------------------------------------------

const retryJobSQL = `
UPDATE azync_jobs SET
	state = 'pending', run_at = now(), attempt = 0, reap_count = 0,
	last_error = NULL, failed_at = NULL, updated_at = now()
WHERE id = $1 AND source = $2 AND state = 'dead'`

// RetryJob resets a dead job of the source to pending for immediate retry.
func (s *Store) RetryJob(ctx context.Context, source driver.Source, id uuid.UUID) error {
	return s.requireRow(ctx, "retry", retryJobSQL, id, string(source))
}

const retryAllDeadSQL = `
UPDATE azync_jobs SET
	state = 'pending', run_at = now(), attempt = 0, reap_count = 0,
	last_error = NULL, failed_at = NULL, updated_at = now()
WHERE source = $1 AND ($2 = '' OR kind = $2) AND state = 'dead'`

// RetryAllDead resets every dead job of (source, kind) to pending. An empty kind
// targets all kinds of the source.
func (s *Store) RetryAllDead(ctx context.Context, source driver.Source, kind string) (int64, error) {
	tag, err := s.pool.Exec(ctx, retryAllDeadSQL, string(source), kind)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: retry all dead: %w", err)
	}
	return tag.RowsAffected(), nil
}

const archiveJobSQL = `
UPDATE azync_jobs SET state = 'dead', lease_until = NULL, updated_at = now()
WHERE id = $1 AND source = $2 AND state IN ('pending', 'scheduled')`

// ArchiveJob force-fails a pending or scheduled job of the source to dead.
func (s *Store) ArchiveJob(ctx context.Context, source driver.Source, id uuid.UUID) error {
	return s.requireRow(ctx, "archive", archiveJobSQL, id, string(source))
}

const pauseJobSQL = `
UPDATE azync_jobs SET state = 'paused', updated_at = now()
WHERE id = $1 AND source = $2 AND state IN ('pending', 'scheduled')`

// PauseJob holds a pending or scheduled job of the source out of the ready set.
func (s *Store) PauseJob(ctx context.Context, source driver.Source, id uuid.UUID) error {
	return s.requireRow(ctx, "pause", pauseJobSQL, id, string(source))
}

const resumeJobSQL = `
UPDATE azync_jobs SET
	state = CASE WHEN run_at <= now() THEN 'pending' ELSE 'scheduled' END,
	updated_at = now()
WHERE id = $1 AND source = $2 AND state = 'paused'`

// ResumeJob returns a paused job of the source to pending or scheduled per its
// run_at.
func (s *Store) ResumeJob(ctx context.Context, source driver.Source, id uuid.UUID) error {
	return s.requireRow(ctx, "resume", resumeJobSQL, id, string(source))
}

const deleteJobSQL = `DELETE FROM azync_jobs WHERE id = $1 AND source = $2 AND state = $3`

// DeleteJob deletes a job of the source in the given state.
func (s *Store) DeleteJob(ctx context.Context, source driver.Source, id uuid.UUID, state driver.JobState) error {
	return s.requireRow(ctx, "delete", deleteJobSQL, id, string(source), string(state))
}

const deleteAllSQL = `DELETE FROM azync_jobs WHERE source = $1 AND ($2 = '' OR kind = $2) AND state = $3`

// DeleteAll deletes every job of (source, kind) in the given state and returns
// the count. An empty kind targets all kinds of the source.
func (s *Store) DeleteAll(ctx context.Context, source driver.Source, kind string, state driver.JobState) (int64, error) {
	tag, err := s.pool.Exec(ctx, deleteAllSQL, string(source), kind, string(state))
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: delete all: %w", err)
	}
	return tag.RowsAffected(), nil
}

const vacuumDeadSQL = `
DELETE FROM azync_jobs
WHERE source = $1 AND ($2 = '' OR kind = $2) AND state = 'dead' AND enqueued_at < now() - make_interval(secs => $3)`

// VacuumDead deletes dead jobs of (source, kind) enqueued before olderThan ago
// and returns the count. An empty kind targets all kinds of the source.
func (s *Store) VacuumDead(ctx context.Context, source driver.Source, kind string, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, vacuumDeadSQL, string(source), kind, olderThan.Seconds())
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: vacuum dead: %w", err)
	}
	return tag.RowsAffected(), nil
}

// NukeAll deletes all jobs, stats and idempotency keys of the source (a dev
// reset) and reports the counts. The event ledger is left intact; deleting the
// jobs cascades to their attempt history.
func (s *Store) NukeAll(ctx context.Context, source driver.Source) (driver.NukeReport, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return driver.NukeReport{}, fmt.Errorf("azyncpgx: nuke begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var report driver.NukeReport
	jobsTag, err := tx.Exec(ctx, `DELETE FROM azync_jobs WHERE source = $1`, string(source))
	if err != nil {
		return driver.NukeReport{}, fmt.Errorf("azyncpgx: nuke jobs: %w", err)
	}
	report.Jobs = jobsTag.RowsAffected()

	statsTag, err := tx.Exec(ctx, `DELETE FROM azync_stats_daily WHERE source = $1`, string(source))
	if err != nil {
		return driver.NukeReport{}, fmt.Errorf("azyncpgx: nuke stats: %w", err)
	}
	report.Stats = statsTag.RowsAffected()

	keysTag, err := tx.Exec(ctx, `DELETE FROM azync_idempotency_keys WHERE source = $1`, string(source))
	if err != nil {
		return driver.NukeReport{}, fmt.Errorf("azyncpgx: nuke keys: %w", err)
	}
	report.Keys = keysTag.RowsAffected()

	if err := tx.Commit(ctx); err != nil {
		return driver.NukeReport{}, fmt.Errorf("azyncpgx: nuke commit: %w", err)
	}
	return report, nil
}

// ---- event ledger admin ---------------------------------------------------

// eventAdminColumns projects a ledger row plus its delivery count for the admin
// views. Deliveries are the source='event' jobs linked by event_id.
const eventAdminColumns = `
	e.id, e.type, e.tenant_id, e.aggregate_type, e.aggregate_id, e.version, e.occurred_at,
	COALESCE(e.trace_id, ''), COALESCE(e.span_id, ''), COALESCE(e.trace_flags, 0),
	e.meta::text, e.payload::text,
	(SELECT COUNT(*) FROM azync_jobs j WHERE j.event_id = e.id AND j.source = 'event')`

// ListEvents lists ledger events matching filter, newest first, paginated.
func (s *Store) ListEvents(ctx context.Context, filter driver.EventFilter, offset, limit int) ([]driver.EventAdminRow, int64, error) {
	where, args := eventWhere(filter)
	var total int64
	// where is built from fixed fragments and bound parameters only.
	//nolint:gosec // G202: no user-controlled SQL identifier is interpolated
	if err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM azync_events e WHERE "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("azyncpgx: list events count: %w", err)
	}
	if offset < 0 {
		offset = 0
	}
	limitIdx := len(args) + 1
	offsetIdx := len(args) + 2
	args = append(args, limitArg(limit), offset)
	//nolint:gosec // G202: no user-controlled SQL identifier is interpolated
	sql := "SELECT " + eventAdminColumns + " FROM azync_events e WHERE " + where +
		" ORDER BY e.occurred_at DESC, e.id DESC" +
		" LIMIT $" + strconv.Itoa(limitIdx) + " OFFSET $" + strconv.Itoa(offsetIdx)
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("azyncpgx: list events: %w", err)
	}
	events, err := scanEventAdminRows(rows)
	if err != nil {
		return nil, 0, err
	}
	return events, total, nil
}

const getEventSQL = `SELECT ` + eventAdminColumns + ` FROM azync_events e WHERE e.id = $1`

// GetEvent returns a single ledger event by id, or a not-found error.
func (s *Store) GetEvent(ctx context.Context, id uuid.UUID) (*driver.EventAdminRow, error) {
	rows, err := s.pool.Query(ctx, getEventSQL, id)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: get event: %w", err)
	}
	events, err := scanEventAdminRows(rows)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, driver.NewNotFound("get event")
	}
	return &events[0], nil
}

func eventWhere(filter driver.EventFilter) (string, []any) {
	where := "TRUE"
	args := []any{}
	add := func(clause string, value any) {
		args = append(args, value)
		where += " AND " + clause + " $" + strconv.Itoa(len(args))
	}
	if filter.Type != "" {
		add("e.type =", filter.Type)
	}
	if filter.TenantID != uuid.Nil {
		add("e.tenant_id =", filter.TenantID)
	}
	if !filter.Since.IsZero() {
		add("e.occurred_at >=", filter.Since)
	}
	if !filter.Until.IsZero() {
		add("e.occurred_at <=", filter.Until)
	}
	if filter.Undispatched != nil {
		if *filter.Undispatched {
			where += ` AND NOT EXISTS (SELECT 1 FROM azync_jobs j WHERE j.event_id = e.id AND j.source = 'event')`
		} else {
			where += ` AND EXISTS (SELECT 1 FROM azync_jobs j WHERE j.event_id = e.id AND j.source = 'event')`
		}
	}
	return where, args
}

func scanEventAdminRows(rows pgx.Rows) ([]driver.EventAdminRow, error) {
	defer rows.Close()
	var out []driver.EventAdminRow
	for rows.Next() {
		var (
			row        driver.EventAdminRow
			id, tenant pgtype.UUID
			meta, body string
		)
		if err := rows.Scan(
			&id, &row.Type, &tenant, &row.AggregateType, &row.AggregateID, &row.Version, &row.OccurredAt,
			&row.TraceID, &row.SpanID, &row.TraceFlags, &meta, &body, &row.Deliveries,
		); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan event: %w", err)
		}
		row.ID = toUUID(id)
		row.TenantID = toUUID(tenant)
		m, err := decodeMeta(meta)
		if err != nil {
			return nil, fmt.Errorf("azyncpgx: decode event meta for %s: %w", row.ID, err)
		}
		row.Meta = m
		row.Payload = json.RawMessage(body)
		if row.Deliveries > 0 {
			row.DispatchedAt = row.OccurredAt
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate events: %w", err)
	}
	return out, nil
}

const listSubscriberViewsSQL = `
SELECT event_type, name, max_attempts, created_at, updated_at FROM azync_subscribers
WHERE ($1 = '' OR event_type = $1)
ORDER BY event_type, name`

// ListSubscriberViews returns subscriber registrations, ordered by event type
// then name. An empty eventType returns all.
func (s *Store) ListSubscriberViews(ctx context.Context, eventType string) ([]driver.SubscriberView, error) {
	rows, err := s.pool.Query(ctx, listSubscriberViewsSQL, eventType)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: list subscriber views: %w", err)
	}
	defer rows.Close()
	var out []driver.SubscriberView
	for rows.Next() {
		var view driver.SubscriberView
		if err := rows.Scan(&view.EventType, &view.Subscriber, &view.MaxAttempts, &view.CreatedAt, &view.UpdatedAt); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan subscriber view: %w", err)
		}
		out = append(out, view)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate subscriber views: %w", err)
	}
	return out, nil
}

const opsStatsSQL = `
SELECT
	(SELECT COUNT(*) FROM azync_events e WHERE NOT EXISTS (SELECT 1 FROM azync_jobs j WHERE j.event_id = e.id AND j.source = 'event')),
	(SELECT COUNT(*) FROM azync_events WHERE occurred_at >= now() - interval '24 hours'),
	(SELECT COUNT(DISTINCT type) FROM azync_events WHERE occurred_at >= now() - interval '24 hours'),
	(SELECT COUNT(*) FROM azync_subscribers)`

// OpsStats returns the event ledger admin summary.
func (s *Store) OpsStats(ctx context.Context) (driver.OpsStats, error) {
	var out driver.OpsStats
	if err := s.pool.QueryRow(ctx, opsStatsSQL).Scan(&out.Undispatched, &out.Total24h, &out.Types24h, &out.Subscribers); err != nil {
		return driver.OpsStats{}, fmt.Errorf("azyncpgx: ops stats: %w", err)
	}
	return out, nil
}
