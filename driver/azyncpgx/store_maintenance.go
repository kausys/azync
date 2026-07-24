package azyncpgx

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/jackc/pgx/v5"
)

const promoteDueSQL = `
UPDATE azync_jobs SET state = 'pending', updated_at = now()
WHERE source = $1 AND kind = ANY($2::text[]) AND state = 'scheduled' AND run_at <= now()`

// PromoteDue moves due scheduled jobs of the given kinds to pending.
func (s *Store) PromoteDue(ctx context.Context, source driver.Source, kinds []string) (int64, error) {
	if len(kinds) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, promoteDueSQL, string(source), kinds)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: promote due: %w", err)
	}
	return tag.RowsAffected(), nil
}

// reapExpiredSQL reclaims active jobs whose lease expired. The `e` CTE locks the
// candidates with SKIP LOCKED; the UPDATE re-checks state and fences on
// lease_token IS NOT DISTINCT FROM so a job re-leased between the two steps is
// left alone. A job crossing the reap budget dies and records an attempt (the
// `ins` CTE) atomically with the transition.
const reapExpiredSQL = `
WITH e AS (
	SELECT id, lease_token FROM azync_jobs
	WHERE source = $1 AND kind = ANY($2::text[]) AND state = 'active' AND lease_until < now()
	FOR UPDATE SKIP LOCKED
),
upd AS (
	UPDATE azync_jobs j SET
		state       = CASE WHEN j.reap_count + 1 >= $3 THEN 'dead' ELSE 'pending' END,
		reap_count  = j.reap_count + 1,
		lease_until = NULL,
		lease_token = NULL,
		last_error  = CASE WHEN j.reap_count + 1 >= $3
			THEN 'lease expired ' || (j.reap_count + 1) || ' times'
			ELSE j.last_error END,
		failed_at   = CASE WHEN j.reap_count + 1 >= $3 THEN now() ELSE j.failed_at END,
		updated_at  = now()
	FROM e
	WHERE j.id = e.id AND j.state = 'active'
		AND j.lease_token IS NOT DISTINCT FROM e.lease_token
	RETURNING j.id, j.kind, j.attempt, j.last_error, j.trace_id, (j.state = 'dead') AS died
),
ins AS (
	INSERT INTO azync_job_attempts (job_id, attempt, error, trace)
	SELECT id, attempt, last_error, trace_id FROM upd WHERE died
)
SELECT kind, died FROM upd`

// ReapExpired reclaims active jobs of the given kinds whose lease expired,
// returning the number reaped and the subset killed.
func (s *Store) ReapExpired(ctx context.Context, source driver.Source, kinds []string, maxReaps int) (int64, int64, error) {
	if len(kinds) == 0 {
		return 0, 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("azyncpgx: reap begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, reapExpiredSQL, string(source), kinds, maxReaps)
	if err != nil {
		return 0, 0, fmt.Errorf("azyncpgx: reap: %w", err)
	}
	perKind, reaped, killed, err := collectReap(rows)
	if err != nil {
		return 0, 0, fmt.Errorf("azyncpgx: reap scan: %w", err)
	}
	// Bump in sorted kind order: two concurrent reapers touching the same stat
	// rows in opposite (map-random) order could otherwise deadlock each other.
	for _, kind := range slices.Sorted(maps.Keys(perKind)) {
		if err := s.bumpStat(ctx, tx, source, kind, statReaped, perKind[kind]); err != nil {
			return 0, 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("azyncpgx: reap commit: %w", err)
	}
	return reaped, killed, nil
}

// collectReap drains the reap RETURNING stream into per-kind reap counts and the
// reaped/killed totals, closing the rows before returning so the transaction's
// connection is free for the follow-up stat bumps.
func collectReap(rows pgx.Rows) (map[string]int64, int64, int64, error) {
	defer rows.Close()
	perKind := map[string]int64{}
	var reaped, killed int64
	for rows.Next() {
		var (
			kind string
			died bool
		)
		if err := rows.Scan(&kind, &died); err != nil {
			return nil, 0, 0, err
		}
		reaped++
		if died {
			killed++
		}
		perKind[kind]++
	}
	return perKind, reaped, killed, rows.Err()
}

const vacuumStatsSQL = `DELETE FROM azync_stats_daily WHERE source = $1 AND day < CURRENT_DATE - $2::integer`

// statsRetentionDays converts a retention to whole days, rounding UP: the
// vacuum works at day granularity, so rounding down could trim a day whose
// newest counters are still within the retention window.
func statsRetentionDays(retention time.Duration) int {
	const day = 24 * time.Hour
	return int((retention + day - 1) / day)
}

// VacuumStats trims daily stat counters of the source older than retention. A
// retention <= 0 removes nothing.
func (s *Store) VacuumStats(ctx context.Context, source driver.Source, retention time.Duration) (int64, error) {
	if retention <= 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, vacuumStatsSQL, string(source), statsRetentionDays(retention))
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: vacuum stats: %w", err)
	}
	return tag.RowsAffected(), nil
}

const vacuumIdempotencySQL = `DELETE FROM azync_idempotency_keys WHERE source = $1 AND expires_at < now()`

// VacuumIdempotency trims expired time-window dedupe keys of the source.
func (s *Store) VacuumIdempotency(ctx context.Context, source driver.Source) (int64, error) {
	tag, err := s.pool.Exec(ctx, vacuumIdempotencySQL, string(source))
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: vacuum idempotency: %w", err)
	}
	return tag.RowsAffected(), nil
}

// vacuumCompletedSQL exempts workflow-owned jobs (workflow_id IS NOT NULL): a
// succeeded task can sit for as long as the workflow keeps running behind it, so
// its lifecycle belongs to the workflow — only VacuumWorkflows' terminal-workflow
// cascade removes it. A plain queue/event job of the same age is unaffected.
const vacuumCompletedSQL = `
DELETE FROM azync_jobs
WHERE source = $1 AND state = 'succeeded' AND workflow_id IS NULL AND completed_at < now() - make_interval(secs => $2)`

// VacuumCompleted trims succeeded jobs of the source completed before retention
// ago. A retention <= 0 removes nothing.
func (s *Store) VacuumCompleted(ctx context.Context, source driver.Source, retention time.Duration) (int64, error) {
	if retention <= 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, vacuumCompletedSQL, string(source), retention.Seconds())
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: vacuum completed: %w", err)
	}
	return tag.RowsAffected(), nil
}
