package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/event"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// orderEvent is the canonical ledger event for the integration suite.
type orderEvent struct {
	Amount int `json:"amount"`
}

func (orderEvent) EventType() string { return "it.order.created" }

func newEvent(t *testing.T, h *harness, opts ...event.Option) *event.Runtime {
	t.Helper()
	e, err := event.New(h.core, opts...)
	require.NoError(t, err)
	return e
}

func TestEventFanOutDeliversCompleteEnvelope(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	e := newEvent(t, h)
	ctx := context.Background()

	for _, name := range []string{"billing", "notify"} {
		is.NoError(e.Publisher().Register(ctx, event.Subscriber{Name: name, EventType: orderEvent{}.EventType(), MaxAttempts: 3}))
	}

	envs := make(chan event.Envelope, 4)
	handler := func(_ context.Context, env event.Envelope) error {
		envs <- env
		return nil
	}
	is.NoError(e.Worker().Subscribe("billing", handler))
	is.NoError(e.Worker().Subscribe("notify", handler))
	startWorker(t, e.Worker())

	tenant := uuid.New()
	eventID, err := e.Publisher().Publish(ctx, orderEvent{Amount: 42},
		event.WithTenantID(tenant),
		event.WithAggregate("order", "order-99"),
		event.WithVersion(7),
		event.WithMeta("origin", "integration"),
	)
	is.NoError(err)

	got := map[string]event.Envelope{}
	for range 2 {
		select {
		case env := <-envs:
			got[env.Subscriber] = env
		case <-time.After(3 * time.Second):
			t.Fatal("did not receive both deliveries")
		}
	}
	is.Len(got, 2)
	for _, name := range []string{"billing", "notify"} {
		env := got[name]
		is.Equal(eventID, env.ID, "envelope carries the ledger event id")
		is.Equal(orderEvent{}.EventType(), env.Type)
		is.Equal(tenant, env.TenantID)
		is.Equal("order", env.AggregateType)
		is.Equal("order-99", env.AggregateID)
		is.Equal(int64(7), env.Version)
		is.False(env.OccurredAt.IsZero())
		is.JSONEq(`{"amount":42}`, string(env.Payload))
		is.Equal(map[string]string{"origin": "integration"}, env.Meta)
		is.Equal(name, env.Subscriber)
		is.Equal(1, env.Attempt)
		is.False(env.Replay)
	}
}

func TestEventFailingSubscriberRetriesToDead(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	e := newEvent(t, h)
	ctx := context.Background()

	is.NoError(e.Publisher().Register(ctx, event.Subscriber{Name: "flaky", EventType: orderEvent{}.EventType(), MaxAttempts: 2}))
	is.NoError(e.Worker().Subscribe("flaky", func(_ context.Context, _ event.Envelope) error {
		return errors.New("subscriber keeps failing")
	}))
	startWorker(t, e.Worker())

	_, err := e.Publisher().Publish(ctx, orderEvent{Amount: 1})
	is.NoError(err)

	require.Eventually(t, func() bool {
		s, serr := e.Manager().Stats(ctx)
		is.NoError(serr)
		return s.Dead == 1 && s.Pending == 0 && s.Active == 0
	}, 8*time.Second, 50*time.Millisecond)
}

func TestEventReplayDeliversWithReplayFlag(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	e := newEvent(t, h)
	ctx := context.Background()

	is.NoError(e.Publisher().Register(ctx, event.Subscriber{Name: "proj", EventType: orderEvent{}.EventType(), MaxAttempts: 3}))
	envs := make(chan event.Envelope, 4)
	is.NoError(e.Worker().Subscribe("proj", func(_ context.Context, env event.Envelope) error {
		envs <- env
		return nil
	}))
	startWorker(t, e.Worker())

	_, err := e.Publisher().Publish(ctx, orderEvent{Amount: 5})
	is.NoError(err)
	select {
	case env := <-envs:
		is.False(env.Replay, "the original delivery is not a replay")
	case <-time.After(3 * time.Second):
		t.Fatal("original delivery not received")
	}

	report, err := e.Manager().Replay(ctx, event.ReplayFilter{Subscriber: "proj"})
	is.NoError(err)
	is.EqualValues(1, report.Created)

	select {
	case env := <-envs:
		is.True(env.Replay, "the replayed delivery is flagged")
	case <-time.After(3 * time.Second):
		t.Fatal("replayed delivery not received")
	}
}

func TestEventRetainTrimsTerminalOnly(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	e := newEvent(t, h)
	ctx := context.Background()
	store := h.core.Store()

	is.NoError(e.Publisher().Register(ctx, event.Subscriber{Name: "sink", EventType: orderEvent{}.EventType(), MaxAttempts: 3}))

	// e1: publish then drain its delivery to a terminal succeeded state.
	e1, err := e.Publisher().Publish(ctx, orderEvent{Amount: 1})
	is.NoError(err)
	leased, err := store.DequeueBatch(ctx, driver.SourceEvent, driver.DequeueParams{Kind: "sink", Limit: 1, Lease: time.Minute})
	is.NoError(err)
	is.Len(leased, 1)
	is.NoError(store.Ack(ctx, leased[0].ID, leased[0].LeaseToken))

	// e2: publish but leave its delivery pending (in-flight).
	e2, err := e.Publisher().Publish(ctx, orderEvent{Amount: 2})
	is.NoError(err)

	deleted, err := e.Manager().Retain(ctx, time.Now().Add(time.Minute), 10)
	is.NoError(err)
	is.EqualValues(1, deleted, "only the event whose deliveries are all terminal is trimmed")

	gone, err := e.Manager().Get(ctx, e1)
	is.NoError(err)
	is.Nil(gone, "the terminal event was retained away")
	kept, err := e.Manager().Get(ctx, e2)
	is.NoError(err)
	is.NotNil(kept, "the event with an in-flight delivery survives")
}

func TestEventManagerOperations(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	e := newEvent(t, h)
	ctx := context.Background()
	m := e.Manager()
	store := h.core.Store()

	for _, name := range []string{"billing", "notify"} {
		is.NoError(e.Publisher().Register(ctx, event.Subscriber{Name: name, EventType: orderEvent{}.EventType(), MaxAttempts: 3}))
	}
	eventID, err := e.Publisher().Publish(ctx, orderEvent{Amount: 10}, event.WithMeta("k", "v"))
	is.NoError(err)

	stats, err := m.Stats(ctx)
	is.NoError(err)
	is.EqualValues(1, stats.Events)
	is.EqualValues(2, stats.Pending)
	is.EqualValues(2, stats.Subscribers)

	page, err := m.List(ctx, event.EventFilter{}, 0, 10)
	is.NoError(err)
	is.EqualValues(1, page.Total)

	view, err := m.Get(ctx, eventID)
	is.NoError(err)
	is.NotNil(view)
	is.Equal(orderEvent{}.EventType(), view.Type)
	is.Equal(map[string]string{"k": "v"}, view.Meta)

	subs, err := m.ListSubscribers(ctx, "")
	is.NoError(err)
	is.Len(subs, 2)

	byEvent, err := m.ListDeliveries(ctx, event.DeliveryFilter{EventID: eventID}, 0, 10)
	is.NoError(err)
	is.EqualValues(2, byEvent.Total)
	byName, err := m.ListDeliveries(ctx, event.DeliveryFilter{Subscriber: "billing"}, 0, 10)
	is.NoError(err)
	is.EqualValues(1, byName.Total)

	// Dead-letter one delivery through the store, then list and retry it.
	leased, err := store.DequeueBatch(ctx, driver.SourceEvent, driver.DequeueParams{Kind: "billing", Limit: 1, Lease: time.Minute})
	is.NoError(err)
	is.Len(leased, 1)
	is.NoError(store.Dead(ctx, leased[0].ID, leased[0].LeaseToken, "boom"))

	dead, err := m.ListDeliveries(ctx, event.DeliveryFilter{State: event.StateDead}, 0, 10)
	is.NoError(err)
	is.EqualValues(1, dead.Total)
	is.Equal("boom", dead.Items[0].LastError)
	is.NoError(m.Retry(ctx, dead.Items[0].ID))
	revived, err := m.Get(ctx, eventID)
	is.NoError(err)
	is.NotNil(revived)

	ops, err := m.OpsStats(ctx)
	is.NoError(err)
	is.EqualValues(2, ops.Subscribers)
	is.EqualValues(1, ops.Total24h)
	is.EqualValues(0, ops.Undispatched)
}
