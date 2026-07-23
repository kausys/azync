package event

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// leaseEventDelivery leases the single pending delivery of a subscriber.
func leaseEventDelivery(t *testing.T, f *drivertest.Fake, subscriber string) driver.Job {
	t.Helper()
	is := require.New(t)
	jobs, err := f.DequeueBatch(context.Background(), driver.SourceEvent,
		driver.DequeueParams{Kind: subscriber, Limit: 1, Lease: time.Minute})
	is.NoError(err)
	is.Len(jobs, 1)
	return jobs[0]
}

// makeDeadDelivery leases and terminally fails a subscriber's delivery.
func makeDeadDelivery(t *testing.T, f *drivertest.Fake, subscriber, msg string) driver.Job {
	t.Helper()
	j := leaseEventDelivery(t, f, subscriber)
	require.NoError(t, f.Dead(context.Background(), j.ID, j.LeaseToken, msg))
	return j
}

func TestManagerStatsAggregatesDepthsAndCounts(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	register(t, r, "billing", orderCreated{}.EventType(), 3)
	register(t, r, "notify", orderCreated{}.EventType(), 3)
	_, err := r.Publisher().Publish(ctx, orderCreated{Value: "x"})
	is.NoError(err)

	// Drive one delivery to succeeded and the other to dead.
	billing := leaseEventDelivery(t, f, "billing")
	is.NoError(f.Ack(ctx, billing.ID, billing.LeaseToken))
	makeDeadDelivery(t, f, "notify", "boom")

	stats, err := r.Manager().Stats(ctx)
	is.NoError(err)
	is.EqualValues(1, stats.Events, "one ledger event")
	is.EqualValues(2, stats.Subscribers)
	is.EqualValues(1, stats.Succeeded)
	is.EqualValues(1, stats.Dead)
	is.Zero(stats.Pending)
	is.Zero(stats.Active)
}

func TestManagerRetryAndRetryDead(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	register(t, r, "billing", orderCreated{}.EventType(), 3)
	register(t, r, "notify", orderCreated{}.EventType(), 3)
	_, err := r.Publisher().Publish(ctx, orderCreated{Value: "x"})
	is.NoError(err)

	dead := makeDeadDelivery(t, f, "billing", "boom")
	makeDeadDelivery(t, f, "notify", "boom")

	// Individual retry re-enqueues one dead delivery.
	is.NoError(r.Manager().Retry(ctx, dead.ID))
	is.Equal(driver.StatePending, deliveryOf(t, f, "billing").State)
	is.Zero(deliveryOf(t, f, "billing").Attempt, "retry resets the attempt budget")

	// Retrying a non-dead delivery is a not-found error.
	err = r.Manager().Retry(ctx, dead.ID)
	is.Error(err)
	is.True(IsNotFound(err))

	// Bulk retry scoped to a subscriber.
	n, err := r.Manager().RetryDead(ctx, DeadFilter{Subscriber: "notify"})
	is.NoError(err)
	is.EqualValues(1, n)
	is.Equal(driver.StatePending, deliveryOf(t, f, "notify").State)
}

func TestManagerReplayCreatesReplayDeliveriesWorkerReceivesFlag(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	register(t, r, "billing", orderCreated{}.EventType(), 3)
	_, err := r.Publisher().Publish(ctx, orderCreated{Value: "x"})
	is.NoError(err)

	report, err := r.Manager().Replay(ctx, ReplayFilter{Subscriber: "billing"})
	is.NoError(err)
	is.EqualValues(1, report.Created)

	// Two deliveries now exist: the original (Replay=false) and the replay
	// (Replay=true). Run the worker and assert both flags reach the handler.
	seen := make(chan bool, 2)
	is.NoError(r.Worker().Subscribe("billing", func(_ context.Context, e Envelope) error {
		seen <- e.Replay
		return nil
	}))
	startWorker(t, r.Worker())

	flags := map[bool]int{}
	for range 2 {
		select {
		case replay := <-seen:
			flags[replay]++
		case <-time.After(2 * time.Second):
			t.Fatal("worker did not deliver both the original and the replay")
		}
	}
	is.Equal(1, flags[false], "the original delivery carries Replay=false")
	is.Equal(1, flags[true], "the replay delivery carries Replay=true")
}

func TestManagerRetainSkipsInFlightTrimsTerminal(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	register(t, r, "billing", orderCreated{}.EventType(), 3)
	_, err := r.Publisher().Publish(ctx, orderCreated{Value: "x"})
	is.NoError(err)

	// A pending (in-flight) delivery blocks retention.
	deleted, err := r.Manager().Retain(ctx, time.Now().Add(time.Second), 10)
	is.NoError(err)
	is.Zero(deleted, "events with in-flight deliveries must survive retention")

	// Drain the delivery to a terminal succeeded state.
	j := leaseEventDelivery(t, f, "billing")
	is.NoError(f.Ack(ctx, j.ID, j.LeaseToken))

	deleted, err = r.Manager().Retain(ctx, time.Now().Add(time.Second), 10)
	is.NoError(err)
	is.EqualValues(1, deleted)
	stats, err := r.Manager().Stats(ctx)
	is.NoError(err)
	is.Zero(stats.Events, "retention cascades the terminal deliveries and removes the event")
}

func TestManagerGetAndListEvents(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	// Missing event → (nil, nil).
	view, err := r.Manager().Get(ctx, uuid.New())
	is.NoError(err)
	is.Nil(view, "a missing event is nil, nil — not an error")

	register(t, r, "billing", orderCreated{}.EventType(), 3)
	id, err := r.Publisher().Publish(ctx, orderCreated{Value: "x"})
	is.NoError(err)
	// A second event with no subscribers → undispatched.
	_, err = r.Publisher().Publish(ctx, orderCancelled{})
	is.NoError(err)

	view, err = r.Manager().Get(ctx, id)
	is.NoError(err)
	is.NotNil(view)
	is.Equal(id, view.ID)
	is.False(view.DispatchedAt.IsZero(), "a dispatched event carries DispatchedAt == OccurredAt")
	is.True(view.DispatchedAt.Equal(view.OccurredAt))

	page, err := r.Manager().List(ctx, EventFilter{}, 0, 50)
	is.NoError(err)
	is.EqualValues(2, page.Total)
	is.Equal(50, page.Size)

	undispatched := true
	page, err = r.Manager().List(ctx, EventFilter{Undispatched: &undispatched}, 0, 50)
	is.NoError(err)
	is.EqualValues(1, page.Total)
	is.Len(page.Items, 1)
	is.True(page.Items[0].DispatchedAt.IsZero(), "an event with no deliveries has a zero DispatchedAt")
}

func TestManagerListDeliveriesFilters(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	register(t, r, "billing", orderCreated{}.EventType(), 3)
	register(t, r, "notify", orderCreated{}.EventType(), 3)
	id, err := r.Publisher().Publish(ctx, orderCreated{Value: "x"})
	is.NoError(err)

	// Scoped by event id: both deliveries.
	page, err := r.Manager().ListDeliveries(ctx, DeliveryFilter{EventID: id}, 0, 50)
	is.NoError(err)
	is.EqualValues(2, page.Total)
	is.Len(page.Items, 2)
	is.Equal(id, page.Items[0].EventID)

	// Scoped by subscriber: one delivery.
	page, err = r.Manager().ListDeliveries(ctx, DeliveryFilter{Subscriber: "billing"}, 0, 50)
	is.NoError(err)
	is.EqualValues(1, page.Total)
	is.Equal("billing", page.Items[0].Subscriber)

	// Scoped by state: make the billing delivery dead, then filter on it.
	makeDeadDelivery(t, f, "billing", "boom")
	page, err = r.Manager().ListDeliveries(ctx, DeliveryFilter{State: StateDead}, 0, 50)
	is.NoError(err)
	is.EqualValues(1, page.Total)
	is.Equal(StateDead, page.Items[0].State)
	is.Equal("boom", page.Items[0].LastError)

	// Event id and state combined (client-side filter path).
	page, err = r.Manager().ListDeliveries(ctx, DeliveryFilter{EventID: id, State: StateDead}, 0, 50)
	is.NoError(err)
	is.EqualValues(1, page.Total)
	is.Equal("billing", page.Items[0].Subscriber)
}

func TestManagerDeliveryAttemptsAfterRetry(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f)
	ctx := context.Background()

	register(t, r, "billing", orderCreated{}.EventType(), 3)
	_, err := r.Publisher().Publish(ctx, orderCreated{Value: "x"})
	is.NoError(err)

	// Attempt 1: reschedule (records boom-1); promote; attempt 2: dead (boom-2).
	first := leaseEventDelivery(t, f, "billing")
	is.NoError(f.Reschedule(ctx, first.ID, first.LeaseToken, 0, "boom-1"))
	_, err = f.PromoteDue(ctx, driver.SourceEvent, []string{"billing"})
	is.NoError(err)
	second := leaseEventDelivery(t, f, "billing")
	is.NoError(f.Dead(ctx, second.ID, second.LeaseToken, "boom-2"))

	attempts, err := r.Manager().DeliveryAttempts(ctx, first.ID)
	is.NoError(err)
	is.Len(attempts, 2)
	is.Equal(1, attempts[0].Attempt)
	is.Equal("boom-1", attempts[0].Error)
	is.Equal(2, attempts[1].Attempt)
	is.Equal("boom-2", attempts[1].Error)

	// A manual retry re-enqueues the dead delivery but preserves its history.
	is.NoError(r.Manager().Retry(ctx, first.ID))
	after, err := r.Manager().DeliveryAttempts(ctx, first.ID)
	is.NoError(err)
	is.Len(after, 2)
}

func TestManagerListSubscribersAndOpsStats(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	register(t, r, "billing", orderCreated{}.EventType(), 3)
	register(t, r, "notify", orderCreated{}.EventType(), 2)
	_, err := r.Publisher().Publish(ctx, orderCancelled{}) // no subscribers → undispatched
	is.NoError(err)

	subs, err := r.Manager().ListSubscribers(ctx, orderCreated{}.EventType())
	is.NoError(err)
	is.Len(subs, 2)

	ops, err := r.Manager().OpsStats(ctx)
	is.NoError(err)
	is.EqualValues(2, ops.Subscribers)
	is.EqualValues(1, ops.Undispatched)
}
