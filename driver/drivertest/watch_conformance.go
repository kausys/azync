package drivertest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// changeWait bounds each drain-until-match assertion. Change hints ride a
// push channel (NOTIFY on the SQL driver), so arrival is prompt; the budget
// is generous purely to keep slow CI honest.
const changeWait = 5 * time.Second

// RunChangeNotifierConformance exercises the optional [driver.ChangeNotifier]
// capability against the Store returned by newStore. Stores without the
// capability skip the suite; a poll-only store (nil channel, nil error) skips
// too. The contract is best-effort and at-most-once with in-band resets, so
// every assertion drains the stream until a predicate matches instead of
// expecting exact sequences — extra hints, resets and unrelated activity on a
// shared backend are all legal.
func RunChangeNotifierConformance(t *testing.T, newStore func(t *testing.T) driver.Store) {
	t.Helper()
	store := newStore(t)
	notifier, ok := store.(driver.ChangeNotifier)
	if !ok {
		t.Skipf("store %T does not implement driver.ChangeNotifier; skipping the change-notifier conformance suite", store)
	}

	t.Run("InitialReset", func(t *testing.T) { runChangeInitialReset(t, notifier) })
	t.Run("JobLifecycle", func(t *testing.T) { runChangeJobLifecycle(t, store, notifier) })
	t.Run("EventAppendAndDelivery", func(t *testing.T) { runChangeEventAppend(t, store, notifier) })
	t.Run("DAGCreate", func(t *testing.T) { runChangeDAGCreate(t, store, notifier) })
}

// subscribeChanges opens one subscription scoped to the subtest, skipping the
// poll-only backends the capability contract allows.
func subscribeChanges(t *testing.T, notifier driver.ChangeNotifier) <-chan driver.Change {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch, err := notifier.Changes(ctx)
	require.NoError(t, err)
	if ch == nil {
		t.Skip("store is poll-only; change notifications are unavailable")
	}
	return ch
}

// subscribeLive opens a subscription and waits for its opening reset: the
// reset marks the stream live, so a change committed after it is observed
// (or announced by a further reset). Mutating before it races the backend's
// push-channel establishment — exactly what the contract tells consumers
// ("refetch on the reset, then trust the stream").
func subscribeLive(t *testing.T, notifier driver.ChangeNotifier) <-chan driver.Change {
	t.Helper()
	ch := subscribeChanges(t, notifier)
	awaitChange(t, ch, "the opening reset", func(c driver.Change) bool {
		return c.Entity == driver.ChangeReset
	})
	return ch
}

// awaitChange drains ch until pred matches or the budget expires.
func awaitChange(t *testing.T, ch <-chan driver.Change, what string, pred func(driver.Change) bool) driver.Change {
	t.Helper()
	deadline := time.After(changeWait)
	for {
		select {
		case c, open := <-ch:
			if !open {
				t.Fatalf("change stream closed while waiting for %s", what)
			}
			if pred(c) {
				return c
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// runChangeInitialReset proves every new subscription's first delivery is a
// reset, so a consumer can rely on "reset means refetch" covering startup.
func runChangeInitialReset(t *testing.T, notifier driver.ChangeNotifier) {
	t.Helper()
	ch := subscribeChanges(t, notifier)
	awaitChange(t, ch, "the initial reset", func(c driver.Change) bool {
		return c.Entity == driver.ChangeReset
	})
}

// runChangeJobLifecycle proves a queue job announces its insert and every
// state transition with its identifiers intact.
func runChangeJobLifecycle(t *testing.T, store driver.Store, notifier driver.ChangeNotifier) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)
	ch := subscribeLive(t, notifier)

	id := uuid.New()
	inserted, err := store.Enqueue(ctx, driver.EnqueueParams{
		ID: id, Kind: "chg_lifecycle", Payload: json.RawMessage(`{}`),
	})
	is.NoError(err)
	is.True(inserted)
	pending := awaitChange(t, ch, "the pending hint", func(c driver.Change) bool {
		return c.Entity == driver.ChangeJob && c.ID == id
	})
	is.Equal(driver.SourceQueue, pending.Source)
	is.Equal("chg_lifecycle", pending.Kind)
	is.Equal(string(driver.StatePending), pending.State)

	leased, err := store.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{
		Kind: "chg_lifecycle", Limit: 1, Lease: time.Minute,
	})
	is.NoError(err)
	is.Len(leased, 1)
	awaitChange(t, ch, "the active hint", func(c driver.Change) bool {
		return c.ID == id && c.State == string(driver.StateActive)
	})

	is.NoError(store.Ack(ctx, id, leased[0].LeaseToken))
	awaitChange(t, ch, "the succeeded hint", func(c driver.Change) bool {
		return c.ID == id && c.State == string(driver.StateSucceeded)
	})
}

// runChangeEventAppend proves a publish announces both the ledger append and
// the fan-out delivery job it creates.
func runChangeEventAppend(t *testing.T, store driver.Store, notifier driver.ChangeNotifier) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)
	ch := subscribeLive(t, notifier)

	is.NoError(store.RegisterSubscriber(ctx, driver.Subscriber{
		Name: "chg_sub", EventType: "evt.chg", MaxAttempts: 3,
	}))
	eventID := uuid.New()
	delivered, err := store.Publish(ctx, driver.PublishParams{
		ID: eventID, Type: "evt.chg", OccurredAt: time.Now(), Payload: json.RawMessage(`{}`),
	})
	is.NoError(err)
	is.Equal(1, delivered)

	event := awaitChange(t, ch, "the ledger-append hint", func(c driver.Change) bool {
		return c.Entity == driver.ChangeEvent && c.ID == eventID
	})
	is.Equal("evt.chg", event.Kind)

	delivery := awaitChange(t, ch, "the delivery-job hint", func(c driver.Change) bool {
		return c.Entity == driver.ChangeJob && c.Source == driver.SourceEvent && c.Kind == "chg_sub"
	})
	is.Equal(string(driver.StatePending), delivery.State)
}

// runChangeDAGCreate proves a DAG creation announces its header and its task
// rows, tasks carrying the owning DAG id and their keys.
func runChangeDAGCreate(t *testing.T, store driver.Store, notifier driver.ChangeNotifier) {
	t.Helper()
	ws, ok := store.(driver.DAGStore)
	if !ok {
		t.Skipf("store %T does not implement driver.DAGStore; skipping the DAG change subtest", store)
	}
	ctx := context.Background()
	is := require.New(t)
	ch := subscribeLive(t, notifier)

	dagID := uuid.New()
	inserted, _, err := ws.CreateDAG(ctx, driver.DAGParams{
		ID:   dagID,
		Name: "chg-dag",
		Tasks: []driver.DAGTask{
			{Key: "first", Kind: "chg.dag.first", Payload: json.RawMessage(`{}`)},
			{Key: "second", Kind: "chg.dag.second", Payload: json.RawMessage(`{}`)},
		},
		Deps: []driver.DAGDep{{TaskKey: "second", DependsOnKey: "first"}},
	})
	is.NoError(err)
	is.True(inserted)

	header := awaitChange(t, ch, "the DAG header hint", func(c driver.Change) bool {
		return c.Entity == driver.ChangeDAG && c.ID == dagID
	})
	is.Equal("chg-dag", header.Kind)
	is.Equal(string(driver.DAGRunning), header.State)

	task := awaitChange(t, ch, "the root task hint", func(c driver.Change) bool {
		return c.Entity == driver.ChangeJob && c.DAGID == dagID && c.TaskKey == "first"
	})
	is.Equal(driver.SourceDAG, task.Source)
	is.Equal(string(driver.StatePending), task.State)

	blocked := awaitChange(t, ch, "the blocked task hint", func(c driver.Change) bool {
		return c.Entity == driver.ChangeJob && c.DAGID == dagID && c.TaskKey == "second"
	})
	is.Equal(string(driver.StateBlocked), blocked.State)
}
