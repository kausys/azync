package event

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"
	"github.com/kausys/azync/internal/engine"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// capturedDelivery is what a handler observes: the decoded event plus the
// delivery metadata it reads from ctx.
type capturedDelivery struct {
	value         string
	id            uuid.UUID
	typ           string
	tenantID      uuid.UUID
	aggregateType string
	aggregateID   string
	version       int64
	meta          map[string]string
	subscriber    string
	attempt       int
	replay        bool
}

func capture(ctx context.Context, evt orderCreated) capturedDelivery {
	return capturedDelivery{
		value:         evt.Value,
		id:            EventID(ctx),
		typ:           Type(ctx),
		tenantID:      TenantID(ctx),
		aggregateType: AggregateType(ctx),
		aggregateID:   AggregateID(ctx),
		version:       Version(ctx),
		meta:          Metadata(ctx),
		subscriber:    SubscriberName(ctx),
		attempt:       Attempt(ctx),
		replay:        IsReplay(ctx),
	}
}

func TestWorkerDeliversTypedEventToEverySubscriber(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	register(t, r, "billing", orderCreated{}.EventType(), 3)
	register(t, r, "notify", orderCreated{}.EventType(), 3)

	billing := make(chan capturedDelivery, 1)
	notify := make(chan capturedDelivery, 1)
	is.NoError(r.Worker().Register(namedSubscriber("billing"), On(func(ctx context.Context, e orderCreated) error {
		billing <- capture(ctx, e)
		return nil
	})))
	is.NoError(r.Worker().Register(namedSubscriber("notify"), On(func(ctx context.Context, e orderCreated) error {
		notify <- capture(ctx, e)
		return nil
	})))

	id, err := r.Publisher().Publish(context.Background(), orderCreated{Value: "hi"},
		WithAggregate("order", "ord_1"), WithVersion(7), WithMeta("k", "v"))
	is.NoError(err)

	startWorker(t, r.Worker())

	for name, ch := range map[string]chan capturedDelivery{"billing": billing, "notify": notify} {
		select {
		case e := <-ch:
			is.Equal("hi", e.value, "the handler receives the decoded domain event")
			is.Equal(id, e.id)
			is.Equal("orders.created.v1", e.typ)
			is.Equal("order", e.aggregateType)
			is.Equal("ord_1", e.aggregateID)
			is.EqualValues(7, e.version)
			is.Equal(map[string]string{"k": "v"}, e.meta)
			is.Equal(name, e.subscriber)
			is.Equal(1, e.attempt)
			is.False(e.replay)
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %q did not receive its delivery", name)
		}
	}

	is.Eventually(func() bool {
		return deliveryOf(t, f, "billing").State == driver.StateSucceeded &&
			deliveryOf(t, f, "notify").State == driver.StateSucceeded
	}, 2*time.Second, 2*time.Millisecond)
}

func TestWorkerRetriesFailedDeliveryWithBackoff(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f, withMaintenanceIntervals(2*time.Millisecond, time.Hour))

	register(t, r, "billing", orderCreated{}.EventType(), 3)

	var attempts atomic.Int32
	seen := make(chan bool, 4) // IsRetry per entry
	is.NoError(r.Worker().Register(namedSubscriber("billing"), On(func(ctx context.Context, _ orderCreated) error {
		seen <- IsRetry(ctx)
		if attempts.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	})))

	_, err := r.Publisher().Publish(context.Background(), orderCreated{Value: "x"})
	is.NoError(err)

	startWorker(t, r.Worker())

	// Attempt 1 is not a retry -> fails -> scheduled at now+Backoff(1).
	is.False(<-seen, "the first delivery is not a retry")
	is.Eventually(func() bool {
		return deliveryOf(t, f, "billing").State == driver.StateScheduled
	}, 2*time.Second, 2*time.Millisecond)
	got := deliveryOf(t, f, "billing")
	is.True(got.RunAt.Equal(clk.Now().Add(engine.Backoff(1))),
		"a plain error must park the delivery at now+Backoff(attempt): %v", got.RunAt)
	is.Equal("transient", got.LastError)

	// Make the retry due; promotion re-dequeues it and attempt 2 (a retry) succeeds.
	clk.Advance(engine.Backoff(1) + time.Second)
	is.True(<-seen, "the second delivery is a retry")
	is.Eventually(func() bool {
		return deliveryOf(t, f, "billing").State == driver.StateSucceeded
	}, 2*time.Second, 2*time.Millisecond)
}

func TestWorkerPermanentErrorDeadLettersImmediately(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	register(t, r, "billing", orderCreated{}.EventType(), 9)

	var runs atomic.Int32
	is.NoError(r.Worker().Register(namedSubscriber("billing"), On(func(context.Context, orderCreated) error {
		runs.Add(1)
		return Permanent(errors.New("invalid payload"))
	})))

	_, err := r.Publisher().Publish(context.Background(), orderCreated{Value: "x"})
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return deliveryOf(t, f, "billing").State == driver.StateDead
	}, 2*time.Second, 2*time.Millisecond)
	is.Equal("invalid payload", deliveryOf(t, f, "billing").LastError)
	is.Equal(int32(1), runs.Load(), "Permanent must not retry")
}

func TestWorkerExhaustedBudgetGoesDead(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f, withMaintenanceIntervals(2*time.Millisecond, time.Hour))

	register(t, r, "billing", orderCreated{}.EventType(), 2)

	var runs atomic.Int32
	is.NoError(r.Worker().Register(namedSubscriber("billing"), On(func(context.Context, orderCreated) error {
		runs.Add(1)
		return errors.New("keeps failing")
	})))

	_, err := r.Publisher().Publish(context.Background(), orderCreated{Value: "x"})
	is.NoError(err)

	startWorker(t, r.Worker())
	deadline := time.Now().Add(4 * time.Second)
	for deliveryOf(t, f, "billing").State != driver.StateDead {
		if time.Now().After(deadline) {
			t.Fatalf("delivery did not exhaust its budget; runs=%d", runs.Load())
		}
		clk.Advance(time.Second) // keep every reschedule due
		time.Sleep(2 * time.Millisecond)
	}
	is.Equal(int32(2), runs.Load(), "the budget must bound handler executions")
	is.Equal(2, deliveryOf(t, f, "billing").Attempt)
}

func TestWorkerIsolatesSubscribersByKind(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	// Two subscribers registered durably, but only one has a live binding: the
	// engine only fetches subscribed kinds, so the unhandled delivery is never
	// leased.
	register(t, r, "billing", orderCreated{}.EventType(), 3)
	register(t, r, "notify", orderCreated{}.EventType(), 3)

	done := make(chan struct{}, 1)
	is.NoError(r.Worker().Register(namedSubscriber("billing"), On(func(context.Context, orderCreated) error {
		done <- struct{}{}
		return nil
	})))

	_, err := r.Publisher().Publish(context.Background(), orderCreated{Value: "x"})
	is.NoError(err)

	startWorker(t, r.Worker())
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscribed handler did not run")
	}
	is.Eventually(func() bool {
		return deliveryOf(t, f, "billing").State == driver.StateSucceeded
	}, 2*time.Second, 2*time.Millisecond)

	// Give the isolated delivery time to prove it is never leased.
	time.Sleep(50 * time.Millisecond)
	is.Equal(driver.StatePending, deliveryOf(t, f, "notify").State,
		"a subscriber without a live binding must never have its delivery leased")
	is.Equal(0, deliveryOf(t, f, "notify").Attempt)
}

func TestWorkerPerSubscriberConcurrency(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithDefaultConcurrency(2))

	register(t, r, "billing", orderCreated{}.EventType(), 3)

	const deliveries = 6
	var inflight, maxInflight, processed atomic.Int32
	allDone := make(chan struct{})
	is.NoError(r.Worker().Register(namedSubscriber("billing"), On(func(context.Context, orderCreated) error {
		cur := inflight.Add(1)
		for {
			prev := maxInflight.Load()
			if cur <= prev || maxInflight.CompareAndSwap(prev, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inflight.Add(-1)
		if processed.Add(1) == deliveries {
			close(allDone)
		}
		return nil
	})))

	for range deliveries {
		_, err := r.Publisher().Publish(context.Background(), orderCreated{Value: "slow"})
		is.NoError(err)
	}
	startWorker(t, r.Worker())

	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		t.Fatal("deliveries did not finish")
	}
	is.Equal(int32(2), maxInflight.Load())
}

func TestWorkerReadyClosesAfterStart(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	is.NoError(r.Worker().Register(namedSubscriber("billing"),
		On(func(context.Context, orderCreated) error { return nil })))

	select {
	case <-r.Worker().Ready():
		t.Fatal("Ready closed before Start")
	default:
	}
	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())
}

func TestWorkerShutdownDrainLetsSlowDeliveryFinish(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	register(t, r, "billing", orderCreated{}.EventType(), 3)

	started := make(chan struct{}, 1)
	var canceled atomic.Bool
	is.NoError(r.Worker().Register(namedSubscriber("billing"), On(func(ctx context.Context, _ orderCreated) error {
		started <- struct{}{}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			canceled.Store(true)
		}
		return nil
	})))

	_, err := r.Publisher().Publish(context.Background(), orderCreated{Value: "slow"})
	is.NoError(err)
	stop := startWorker(t, r.Worker())

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery did not start")
	}
	is.ErrorIs(stop(), context.Canceled)
	is.False(canceled.Load(), "a delivery inside the drain budget must complete, not be cancelled")
	is.Equal(driver.StateSucceeded, deliveryOf(t, f, "billing").State)
}

// wakeFailStore fails its first Wake so Start fails during engine setup.
type wakeFailStore struct {
	*drivertest.Fake
	calls atomic.Int32
}

func (s *wakeFailStore) Wake(ctx context.Context) (<-chan driver.Wake, error) {
	if s.calls.Add(1) == 1 {
		return nil, errors.New("listen unavailable")
	}
	return s.Fake.Wake(ctx)
}

func TestWorkerStartCanRetryAfterSetupFailure(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	store := &wakeFailStore{Fake: drivertest.NewFake()}
	core, err := azyncNew(store)
	is.NoError(err)
	r, err := New(core, fastOptions()...)
	is.NoError(err)

	done := make(chan struct{}, 1)
	is.NoError(r.Worker().Register(namedSubscriber("billing"), On(func(context.Context, orderCreated) error {
		done <- struct{}{}
		return nil
	})))

	err = r.Worker().Start(context.Background())
	is.Error(err)
	is.Contains(err.Error(), "wake")

	// The failed setup must not poison the worker: a retry starts and delivers.
	_, err = r.Publisher().Publish(context.Background(), orderCreated{Value: "retry"})
	is.NoError(err)
	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the retried Start did not process the delivery")
	}
}

func TestWorkerMissingLedgerRecordDeadLetters(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	register(t, r, "billing", orderCreated{}.EventType(), 3)

	var runs atomic.Int32
	is.NoError(r.Worker().Register(namedSubscriber("billing"), On(func(context.Context, orderCreated) error {
		runs.Add(1)
		return nil
	})))

	// A delivery whose ledger event is absent (should never happen in practice):
	// the adapter must dead-letter it, not panic, and never call the handler.
	id := uuid.New()
	f.SeedOrphanDelivery(id, "billing")

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		j, err := f.GetJob(context.Background(), driver.SourceEvent, id)
		return err == nil && j.State == driver.StateDead
	}, 2*time.Second, 2*time.Millisecond)
	is.Equal(int32(0), runs.Load(), "a delivery without a ledger record must not reach the handler")
}
