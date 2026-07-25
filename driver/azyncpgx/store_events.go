package azyncpgx

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/jackc/pgx/v5"
)

// replayLimitDefault caps an unbounded Replay so a filter that matches the whole
// ledger cannot fan out without any upper bound.
const replayLimitDefault = 1_000_000

const publishEventSQL = `
INSERT INTO azync_events
	(id, type, tenant_id, aggregate_type, aggregate_id, version, occurred_at, payload, meta, trace_id, span_id, trace_flags)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10, $11, $12)`

// publishDeliveriesSQL fans out one pending delivery per currently registered
// matching subscriber. Deliveries carry an explicit per-subscriber budget
// (max_attempts_explicit=true), so the first-lease default override never
// touches them; payload is NULL because the body lives in the ledger.
const publishDeliveriesSQL = `
INSERT INTO azync_jobs
	(id, source, kind, state, run_at, max_attempts, max_attempts_explicit, event_id, replay, attempt, enqueued_at, meta, payload)
SELECT gen_random_uuid(), 'event', s.name, 'pending', now(), s.max_attempts, true, $1, false, 0, now(), '{}', NULL
FROM azync_subscribers s WHERE s.event_type = $2`

// publishNotifySQL fires one wakeup per matching subscriber, inside the tx.
const publishNotifySQL = `
SELECT pg_notify($1, 'event:' || name) FROM azync_subscribers WHERE event_type = $2`

// Publish atomically appends one event and fans out one pending delivery per
// matching subscriber in a single transaction.
func (s *Store) Publish(ctx context.Context, p driver.PublishParams) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: publish begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	delivered, err := s.publish(ctx, tx, p)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("azyncpgx: publish commit: %w", err)
	}
	return delivered, nil
}

// PublishTx performs Publish within the caller's transaction.
func (s *Store) PublishTx(ctx context.Context, tx pgx.Tx, p driver.PublishParams) (int, error) {
	return s.publish(ctx, tx, p)
}

func (s *Store) publish(ctx context.Context, q querier, p driver.PublishParams) (int, error) {
	metaJSON, err := json.Marshal(orEmptyMeta(p.Meta))
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: marshal event meta: %w", err)
	}
	if _, err := q.Exec(ctx, publishEventSQL,
		p.ID, p.Type, nullableUUID(p.TenantID), p.AggregateType, p.AggregateID, p.Version, p.OccurredAt,
		string(p.Payload), string(metaJSON), nullableString(p.TraceID), nullableString(p.SpanID),
		nullableTraceFlags(p.TraceID, p.TraceFlags),
	); err != nil {
		return 0, fmt.Errorf("azyncpgx: append event: %w", err)
	}
	tag, err := q.Exec(ctx, publishDeliveriesSQL, p.ID, p.Type)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: create deliveries: %w", err)
	}
	delivered := int(tag.RowsAffected())
	if _, err := q.Exec(ctx, publishNotifySQL, s.notifyChannel, p.Type); err != nil {
		return 0, fmt.Errorf("azyncpgx: publish notify: %w", err)
	}
	return delivered, nil
}

const registerSubscriberSQL = `
INSERT INTO azync_subscribers (name, event_type, max_attempts, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (name, event_type) DO UPDATE SET max_attempts = EXCLUDED.max_attempts, updated_at = now()`

// RegisterSubscriber upserts a subscriber keyed by (Name, EventType).
func (s *Store) RegisterSubscriber(ctx context.Context, sub driver.Subscriber) error {
	if _, err := s.pool.Exec(ctx, registerSubscriberSQL, sub.Name, sub.EventType, sub.MaxAttempts); err != nil {
		return fmt.Errorf("azyncpgx: register subscriber: %w", err)
	}
	return nil
}

const subscribersSQL = `
SELECT name, event_type, max_attempts FROM azync_subscribers WHERE event_type = $1 ORDER BY name`

// Subscribers returns the registrations for an event type, ordered by name.
func (s *Store) Subscribers(ctx context.Context, eventType string) ([]driver.Subscriber, error) {
	rows, err := s.pool.Query(ctx, subscribersSQL, eventType)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: list subscribers: %w", err)
	}
	defer rows.Close()
	var out []driver.Subscriber
	for rows.Next() {
		var sub driver.Subscriber
		if err := rows.Scan(&sub.Name, &sub.EventType, &sub.MaxAttempts); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan subscriber: %w", err)
		}
		out = append(out, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate subscribers: %w", err)
	}
	return out, nil
}

// replaySQL re-fans-out ledger events matching the filter into fresh deliveries
// flagged replay=true, reconstructed from the ledger (the single source of
// truth) so no original publish call is needed.
const replaySQL = `
INSERT INTO azync_jobs
	(id, source, kind, state, run_at, max_attempts, max_attempts_explicit, event_id, replay, attempt, enqueued_at, meta, payload)
SELECT gen_random_uuid(), 'event', sub.name, 'pending', now(), sub.max_attempts, true, e.id, true, 0, now(), '{}', NULL
FROM (
	SELECT ev.* FROM azync_events ev
	WHERE ($2 = '' OR ev.type = $2)
	  AND ($5::uuid IS NULL OR ev.id = $5)
	  AND ($3::timestamptz IS NULL OR ev.occurred_at >= $3)
	  AND ($4::timestamptz IS NULL OR ev.occurred_at <= $4)
	ORDER BY ev.id
	LIMIT $6
) e
JOIN azync_subscribers sub ON sub.event_type = e.type
WHERE ($1 = '' OR sub.name = $1)
RETURNING kind`

// Replay re-fans-out ledger events matching filter into fresh pending
// deliveries flagged Replay, and returns the number created. It wakes each
// affected subscriber loop inside the transaction (outbox).
func (s *Store) Replay(ctx context.Context, filter driver.ReplayFilter) (int64, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = replayLimitDefault
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: replay begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, replaySQL,
		filter.Subscriber, filter.EventType, nullableTime(filter.Since), nullableTime(filter.Until),
		nullableUUID(filter.EventID), limit)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: replay: %w", err)
	}
	kinds, created, err := collectKinds(rows)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: replay scan: %w", err)
	}
	for kind := range kinds {
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, s.notifyChannel, notifyPayload(driver.SourceEvent, kind)); err != nil {
			return 0, fmt.Errorf("azyncpgx: replay notify: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("azyncpgx: replay commit: %w", err)
	}
	return created, nil
}

// collectKinds drains a RETURNING kind stream into a distinct set and a count,
// closing the rows before returning so the transaction's connection is free for
// the follow-up notifies.
func collectKinds(rows pgx.Rows) (map[string]struct{}, int64, error) {
	defer rows.Close()
	kinds := map[string]struct{}{}
	var count int64
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			return nil, 0, err
		}
		count++
		kinds[kind] = struct{}{}
	}
	return kinds, count, rows.Err()
}

// retainSQL trims old ledger events whose deliveries have all reached a terminal
// state (succeeded or dead); the FK cascade then removes those terminal
// deliveries. Events with any non-terminal delivery are skipped so the cascade
// never wipes in-flight work.
const retainSQL = `
DELETE FROM azync_events WHERE id IN (
	SELECT e.id FROM azync_events e
	WHERE e.occurred_at < $1
	  AND NOT EXISTS (
	      SELECT 1 FROM azync_jobs j
	      WHERE j.event_id = e.id AND j.state <> ALL (ARRAY['succeeded'::text, 'dead'::text])
	  )
	ORDER BY e.occurred_at, e.id
	LIMIT $2
)`

// Retain deletes up to limit ledger events before the cutoff whose deliveries
// are all terminal, cascading to those deliveries, and returns the count.
func (s *Store) Retain(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, retainSQL, before, limit)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: retain: %w", err)
	}
	return tag.RowsAffected(), nil
}
