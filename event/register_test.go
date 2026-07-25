package event

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// badPayloadCreated shares the "orders.created.v1" type but marshals to a JSON
// string, so an On(orderCreated) binding cannot decode it — the decode-fail
// path.
type badPayloadCreated string

func (badPayloadCreated) EventType() string { return "orders.created.v1" }

func TestRegisterRoutesEachTypeToItsBinding(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	ctx := context.Background()

	created := make(chan orderCreated, 1)
	cancelled := make(chan orderCancelled, 1)
	is.NoError(r.Worker().Register(namedSubscriber("multi"),
		On(func(_ context.Context, e orderCreated) error { created <- e; return nil }),
		On(func(_ context.Context, e orderCancelled) error { cancelled <- e; return nil }),
	))

	// Start upserts both subscriptions; wait for Ready so they exist before
	// publishing.
	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	_, err := r.Publisher().Publish(ctx, orderCreated{Value: "made"})
	is.NoError(err)
	_, err = r.Publisher().Publish(ctx, orderCancelled{})
	is.NoError(err)

	select {
	case e := <-created:
		is.Equal("made", e.Value, "the orders.created.v1 delivery reaches the orderCreated binding")
	case <-time.After(2 * time.Second):
		t.Fatal("orderCreated binding never fired")
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("orderCancelled binding never fired")
	}
}

func TestStartUpsertsSubscriptionsDurably(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	// No manual Publisher.Register: Start must create the durable subscription.
	is.NoError(r.Worker().Register(namedSubscriber("billing"),
		On(func(context.Context, orderCreated) error { return nil })))

	subsBefore, err := f.ListSubscriberViews(ctx, "")
	is.NoError(err)
	is.Empty(subsBefore, "nothing is registered until Start")

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	subs, err := f.ListSubscriberViews(ctx, "")
	is.NoError(err)
	is.Len(subs, 1, "Start upserts the subscription into the catalog")
	is.Equal("billing", subs[0].Subscriber)
	is.Equal("orders.created.v1", subs[0].EventType)
}

func TestRegisterMaxAttemptsInterfaceReachesCatalog(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithDefaultMaxAttempts(9))
	ctx := context.Background()

	// budgetSubscriber pins MaxAttempts; namedSubscriber inherits the default.
	is.NoError(r.Worker().Register(budgetSubscriber{name: "billing", max: 2},
		On(func(context.Context, orderCreated) error { return nil })))
	is.NoError(r.Worker().Register(namedSubscriber("notify"),
		On(func(context.Context, orderCreated) error { return nil })))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	subs, err := f.ListSubscriberViews(ctx, orderCreated{}.EventType())
	is.NoError(err)
	byName := map[string]int{}
	for _, s := range subs {
		byName[s.Subscriber] = s.MaxAttempts
	}
	is.Equal(2, byName["billing"], "the MaxAttempts interface pins the budget")
	is.Equal(9, byName["notify"], "a subscriber without MaxAttempts inherits the runtime default")
}

func TestRegisterFuncDeliversTypedEndToEnd(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	got := make(chan orderCreated, 1)
	is.NoError(RegisterFunc(r.Worker(), "billing",
		func(_ context.Context, e orderCreated) error { got <- e; return nil },
		WithMaxAttempts(4)))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	// The RegisterFunc budget must reach the catalog too.
	subs, err := f.ListSubscriberViews(ctx, orderCreated{}.EventType())
	is.NoError(err)
	is.Len(subs, 1)
	is.Equal(4, subs[0].MaxAttempts)

	_, err = r.Publisher().Publish(ctx, orderCreated{Value: "rf"})
	is.NoError(err)
	select {
	case e := <-got:
		is.Equal("rf", e.Value)
	case <-time.After(2 * time.Second):
		t.Fatal("RegisterFunc handler never received the typed event")
	}
}

func TestDecodeFailureDeadLetters(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	// A durable subscription so the publish creates a delivery.
	register(t, r, "billing", orderCreated{}.EventType(), 5)

	var runs atomic.Int32
	is.NoError(r.Worker().Register(namedSubscriber("billing"), On(func(context.Context, orderCreated) error {
		runs.Add(1)
		return nil
	})))

	// Publish an event of the bound type whose payload cannot decode into
	// orderCreated (a JSON string, not an object).
	_, err := r.Publisher().Publish(ctx, badPayloadCreated("nope"))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return deliveryOf(t, f, "billing").State == driver.StateDead
	}, 2*time.Second, 2*time.Millisecond, "an undecodable payload must dead-letter")
	is.Contains(deliveryOf(t, f, "billing").LastError, "decode orders.created.v1 payload")
	is.Equal(int32(0), runs.Load(), "the typed handler must never see an undecodable payload")
	is.Equal(1, deliveryOf(t, f, "billing").Attempt, "decode failure must not burn retries")
}

func TestNewContextExposesDeliveryToAccessors(t *testing.T) {
	t.Parallel()
	is := require.New(t)

	id := uuid.New()
	tenant := uuid.New()
	occurred := time.Date(2026, 7, 23, 9, 0, 0, 0, time.UTC)
	d := Delivery{
		ID:            id,
		Type:          "orders.created.v1",
		TenantID:      tenant,
		AggregateType: "order",
		AggregateID:   "ord_1",
		Version:       7,
		OccurredAt:    occurred,
		Meta:          map[string]string{"k": "v"},
		Subscriber:    "billing",
		Attempt:       2,
		MaxAttempts:   5,
		Replay:        true,
	}
	ctx := NewContext(context.Background(), d)

	got, ok := DeliveryFromContext(ctx)
	is.True(ok)
	is.Equal(d, got)

	is.Equal(id, EventID(ctx))
	is.Equal("orders.created.v1", Type(ctx))
	is.Equal(tenant, TenantID(ctx))
	is.Equal("order", AggregateType(ctx))
	is.Equal("ord_1", AggregateID(ctx))
	is.EqualValues(7, Version(ctx))
	is.Equal(occurred, OccurredAt(ctx))
	is.Equal(map[string]string{"k": "v"}, Metadata(ctx))
	is.Equal("billing", SubscriberName(ctx))
	is.Equal(2, Attempt(ctx))
	is.Equal(5, MaxAttempts(ctx))
	is.True(IsRetry(ctx))
	is.True(IsReplay(ctx))
}

func TestEventAccessorsAreZeroValueSafeOutsideADelivery(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()

	_, ok := DeliveryFromContext(ctx)
	is.False(ok)

	is.Equal(uuid.Nil, EventID(ctx))
	is.Empty(Type(ctx))
	is.Equal(uuid.Nil, TenantID(ctx))
	is.Empty(AggregateType(ctx))
	is.Empty(AggregateID(ctx))
	is.Zero(Version(ctx))
	is.True(OccurredAt(ctx).IsZero())
	is.Nil(Metadata(ctx))
	is.Empty(SubscriberName(ctx))
	is.Zero(Attempt(ctx))
	is.Zero(MaxAttempts(ctx))
	is.False(IsRetry(ctx))
	is.False(IsReplay(ctx))
}
