package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/queue"
	"github.com/kausys/azync/watch"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// awaitWatchChange drains ch until pred matches or the budget expires. The
// change contract is at-most-once with interleaved resets, so tests match by
// predicate, never by exact sequence.
func awaitWatchChange(t *testing.T, ch <-chan watch.Change, what string, pred func(watch.Change) bool) watch.Change {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case c, open := <-ch:
			if !open {
				t.Fatalf("watch stream closed while waiting for %s", what)
			}
			if pred(c) {
				return c
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// TestWatchDeliversJobLifecycle drives the full public path — queue producer
// and worker over live PostgreSQL, 00011 triggers, the dedicated changes
// LISTEN connection, and the watch package's filtering — and proves one job's
// lifecycle arrives as pending → active → succeeded hints after the opening
// reset.
func TestWatchDeliversJobLifecycle(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	ctx := context.Background()

	q := newQueue(t, h)
	is.NoError(queue.Register(q.Worker(), func(context.Context, itJob) error { return nil }))

	w, err := watch.New(h.core)
	is.NoError(err)
	ch, err := w.Watch(ctx, watch.Filter{Sources: []watch.Source{driver.SourceQueue}})
	is.NoError(err)

	first := <-ch
	is.Equal(watch.EntityReset, first.Entity, "the first delivery is always a reset")

	startWorker(t, q.Worker())
	res, err := q.Producer().Enqueue(ctx, itJob{V: "watched"})
	is.NoError(err)
	id := res.ID

	pending := awaitWatchChange(t, ch, "the pending hint", func(c watch.Change) bool {
		return c.ID == id && c.State == string(driver.StatePending)
	})
	is.Equal(watch.EntityJob, pending.Entity)
	is.Equal(itJob{}.Kind(), pending.Kind)
	awaitWatchChange(t, ch, "the active hint", func(c watch.Change) bool {
		return c.ID == id && c.State == string(driver.StateActive)
	})
	awaitWatchChange(t, ch, "the succeeded hint", func(c watch.Change) bool {
		return c.ID == id && c.State == string(driver.StateSucceeded)
	})
}

// TestChangesBulkCoalesces proves a statement touching more rows than the
// trigger's per-row cap emits one coalesced bulk hint carrying the row count,
// not one notify per row — for both the INSERT and the UPDATE trigger.
func TestChangesBulkCoalesces(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	ctx := context.Background()
	pool := newPool(t, h.base, h.schema)

	w, err := watch.New(h.core)
	is.NoError(err)
	ch, err := w.Watch(ctx, watch.Filter{Sources: []watch.Source{driver.SourceQueue}})
	is.NoError(err)

	// One INSERT statement, 60 rows: over the 50-row cap.
	_, err = pool.Exec(ctx, `
		INSERT INTO azync_jobs (id, source, kind, state, run_at, max_attempts, payload, enqueued_at)
		SELECT gen_random_uuid(), 'queue', 'bulk.kind', 'scheduled', now() + interval '1 hour', 3, '{}'::jsonb, now()
		FROM generate_series(1, 60)`)
	is.NoError(err)

	insertBulk := awaitWatchChange(t, ch, "the insert bulk hint", func(c watch.Change) bool {
		return c.Bulk && c.Entity == watch.EntityJob
	})
	is.Equal(driver.SourceQueue, insertBulk.Source)
	is.Equal(60, insertBulk.Count)
	is.Equal(uuid.Nil, insertBulk.ID, "a bulk hint names no row")

	// One UPDATE statement flipping the state of all 60.
	_, err = pool.Exec(ctx, `UPDATE azync_jobs SET state = 'paused' WHERE kind = 'bulk.kind'`)
	is.NoError(err)

	updateBulk := awaitWatchChange(t, ch, "the update bulk hint", func(c watch.Change) bool {
		return c.Bulk && c.Entity == watch.EntityJob && c.Count == 60
	})
	is.Equal(driver.SourceQueue, updateBulk.Source)
}

// TestChangesSchemaIsolation proves the shared azync_changes channel is
// schema-filtered: a watcher on schema A never sees schema B's changes. The
// negative is bounded by ordering — B commits (and NOTIFYs) before A's
// marker, so if B's hint were going to leak it would arrive before the marker
// does.
func TestChangesSchemaIsolation(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	a := newHarness(t)
	b := newHarness(t)

	w, err := watch.New(a.core)
	is.NoError(err)
	ch, err := w.Watch(ctx, watch.Filter{})
	is.NoError(err)

	foreign := uuid.New()
	_, err = b.core.Store().Enqueue(ctx, driver.EnqueueParams{
		ID: foreign, Kind: "isolation.foreign", Payload: json.RawMessage(`{}`), MaxAttempts: 3,
	})
	is.NoError(err)

	marker := uuid.New()
	_, err = a.core.Store().Enqueue(ctx, driver.EnqueueParams{
		ID: marker, Kind: "isolation.marker", Payload: json.RawMessage(`{}`), MaxAttempts: 3,
	})
	is.NoError(err)

	for {
		c := awaitWatchChange(t, ch, "the marker hint", func(watch.Change) bool { return true })
		is.NotEqual(foreign, c.ID, "schema B's change must never reach schema A's watcher")
		if c.ID == marker {
			return
		}
	}
}

// changeWireKeys are the only keys a change payload may carry. Anything else
// — payload, result, meta, last_error — is a PII leak.
var changeWireKeys = map[string]bool{
	"schema": true, "entity": true, "source": true, "id": true, "dagId": true,
	"kind": true, "taskKey": true, "state": true, "atMs": true, "bulk": true, "count": true,
}

// TestChangesPayloadCarriesNoPII listens raw on azync_changes and proves the
// notification for a job whose payload carries a sentinel value never
// includes that value, and carries only the documented identifier keys.
func TestChangesPayloadCarriesNoPII(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, h.base)
	is.NoError(err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	_, err = conn.Exec(ctx, `LISTEN azync_changes`)
	is.NoError(err)

	const sentinel = "PII-SENTINEL-do-not-broadcast"
	id := uuid.New()
	_, err = h.core.Store().Enqueue(ctx, driver.EnqueueParams{
		ID: id, Kind: "pii.probe", MaxAttempts: 3,
		Payload: json.RawMessage(`{"email":"` + sentinel + `"}`),
	})
	is.NoError(err)

	deadline := time.Now().Add(5 * time.Second)
	for {
		waitCtx, cancel := context.WithDeadline(ctx, deadline)
		n, err := conn.WaitForNotification(waitCtx)
		cancel()
		is.NoError(err, "expected the raw notification before the deadline")
		if !strings.Contains(n.Payload, id.String()) {
			continue // another schema's traffic on the shared channel
		}
		is.NotContains(n.Payload, sentinel, "job payloads must never travel in change notifications")
		var keys map[string]any
		is.NoError(json.Unmarshal([]byte(n.Payload), &keys))
		for k := range keys {
			is.True(changeWireKeys[k], "unexpected key %q in change payload", k)
		}
		return
	}
}
