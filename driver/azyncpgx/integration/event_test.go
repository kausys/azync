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

// awaitEventReady blocks until the event worker has upserted its subscriptions
// and is running, so a subsequent Publish fans out to them.
func awaitEventReady(t *testing.T, e *event.Runtime) {
	t.Helper()
	select {
	case <-e.Worker().Ready():
	case <-time.After(3 * time.Second):
		t.Fatal("event worker never became ready")
	}
}

// eventDelivery is what a handler observes end to end: the decoded event plus
// the delivery metadata read from ctx.
type eventDelivery struct {
	amount        int
	id            uuid.UUID
	typ           string
	tenantID      uuid.UUID
	aggregateType string
	aggregateID   string
	version       int64
	occurredAt    time.Time
	meta          map[string]string
	subscriber    string
	attempt       int
	replay        bool
}

func captureEvent(ctx context.Context, e orderEvent) eventDelivery {
	return eventDelivery{
		amount:        e.Amount,
		id:            event.EventID(ctx),
		typ:           event.Type(ctx),
		tenantID:      event.TenantID(ctx),
		aggregateType: event.AggregateType(ctx),
		aggregateID:   event.AggregateID(ctx),
		version:       event.Version(ctx),
		occurredAt:    event.OccurredAt(ctx),
		meta:          event.Metadata(ctx),
		subscriber:    event.SubscriberName(ctx),
		attempt:       event.Attempt(ctx),
		replay:        event.IsReplay(ctx),
	}
}

func TestEventFanOutDeliversCompleteDelivery(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	e := newEvent(t, h)
	ctx := context.Background()

	deliveries := make(chan eventDelivery, 4)
	handler := func(ctx context.Context, ev orderEvent) error {
		deliveries <- captureEvent(ctx, ev)
		return nil
	}
	is.NoError(event.RegisterFunc(e.Worker(), "billing", handler))
	is.NoError(event.RegisterFunc(e.Worker(), "notify", handler))
	startWorker(t, e.Worker())
	awaitEventReady(t, e)

	tenant := uuid.New()
	eventID, err := e.Publisher().Publish(ctx, orderEvent{Amount: 42},
		event.WithTenantID(tenant),
		event.WithAggregate("order", "order-99"),
		event.WithVersion(7),
		event.WithMeta("origin", "integration"),
	)
	is.NoError(err)

	got := map[string]eventDelivery{}
	for range 2 {
		select {
		case d := <-deliveries:
			got[d.subscriber] = d
		case <-time.After(3 * time.Second):
			t.Fatal("did not receive both deliveries")
		}
	}
	is.Len(got, 2)
	for _, name := range []string{"billing", "notify"} {
		d := got[name]
		is.Equal(42, d.amount, "the handler receives the decoded domain event")
		is.Equal(eventID, d.id, "the delivery carries the ledger event id")
		is.Equal(orderEvent{}.EventType(), d.typ)
		is.Equal(tenant, d.tenantID)
		is.Equal("order", d.aggregateType)
		is.Equal("order-99", d.aggregateID)
		is.Equal(int64(7), d.version)
		is.False(d.occurredAt.IsZero())
		is.Equal(map[string]string{"origin": "integration"}, d.meta)
		is.Equal(name, d.subscriber)
		is.Equal(1, d.attempt)
		is.False(d.replay)
	}
}

func TestEventFailingSubscriberRetriesToDead(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	e := newEvent(t, h)
	ctx := context.Background()

	is.NoError(event.RegisterFunc(e.Worker(), "flaky", func(context.Context, orderEvent) error {
		return errors.New("subscriber keeps failing")
	}, event.WithMaxAttempts(2)))
	startWorker(t, e.Worker())
	awaitEventReady(t, e)

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

	replays := make(chan bool, 4)
	is.NoError(event.RegisterFunc(e.Worker(), "proj", func(ctx context.Context, _ orderEvent) error {
		replays <- event.IsReplay(ctx)
		return nil
	}))
	startWorker(t, e.Worker())
	awaitEventReady(t, e)

	_, err := e.Publisher().Publish(ctx, orderEvent{Amount: 5})
	is.NoError(err)
	select {
	case replay := <-replays:
		is.False(replay, "the original delivery is not a replay")
	case <-time.After(3 * time.Second):
		t.Fatal("original delivery not received")
	}

	report, err := e.Manager().Replay(ctx, event.ReplayFilter{Subscriber: "proj"})
	is.NoError(err)
	is.EqualValues(1, report.Created)

	select {
	case replay := <-replays:
		is.True(replay, "the replayed delivery is flagged")
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

	// No worker here: register the subscription administratively so the fan-out
	// creates deliveries this test drains through the store directly.
	is.NoError(e.Publisher().Register(ctx, event.Subscription{Name: "sink", EventType: orderEvent{}.EventType(), MaxAttempts: 3}))

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

	// Administrative registration: no worker binding, so drive deliveries through
	// the store below.
	for _, name := range []string{"billing", "notify"} {
		is.NoError(e.Publisher().Register(ctx, event.Subscription{Name: name, EventType: orderEvent{}.EventType(), MaxAttempts: 3}))
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
