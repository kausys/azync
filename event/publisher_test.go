package event

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestRegisterValidatesAndResolvesMaxAttempts(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithDefaultMaxAttempts(9))
	ctx := context.Background()

	// Missing name or event type is rejected.
	is.Error(r.Publisher().Register(ctx, Subscription{EventType: "orders.created.v1"}))
	is.Error(r.Publisher().Register(ctx, Subscription{Name: "billing"}))

	// MaxAttempts <= 0 inherits the runtime default.
	is.NoError(r.Publisher().Register(ctx, Subscription{Name: "billing", EventType: "orders.created.v1"}))
	subs, err := f.ListSubscriberViews(ctx, "orders.created.v1")
	is.NoError(err)
	is.Len(subs, 1)
	is.Equal(9, subs[0].MaxAttempts, "an unset budget must inherit the runtime default")

	// An explicit budget is kept.
	is.NoError(r.Publisher().Register(ctx, Subscription{Name: "notify", EventType: "orders.created.v1", MaxAttempts: 3}))
	subs, err = f.ListSubscriberViews(ctx, "orders.created.v1")
	is.NoError(err)
	is.Len(subs, 2)
}

func TestRegisterFloorsDefaultToOne(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	// Mask the fake so New accepts a custom core; then force the resolved default
	// to 1 via the option and verify the floor holds even at the minimum.
	core, err := azync.New(f, azync.WithLogger(discardLogger()), azync.WithDefaultMaxAttempts(1))
	is.NoError(err)
	r, err := New(core)
	is.NoError(err)
	ctx := context.Background()

	is.NoError(r.Publisher().Register(ctx, Subscription{Name: "billing", EventType: "orders.created.v1"}))
	subs, err := f.ListSubscriberViews(ctx, "orders.created.v1")
	is.NoError(err)
	is.Len(subs, 1)
	is.Equal(1, subs[0].MaxAttempts)
}

func TestRegisterUpsertsExisting(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	is.NoError(r.Publisher().Register(ctx, Subscription{Name: "billing", EventType: "orders.created.v1", MaxAttempts: 2}))
	is.NoError(r.Publisher().Register(ctx, Subscription{Name: "billing", EventType: "orders.created.v1", MaxAttempts: 5}))

	subs, err := f.ListSubscriberViews(ctx, "orders.created.v1")
	is.NoError(err)
	is.Len(subs, 1, "re-registering the same (name, event type) upserts")
	is.Equal(5, subs[0].MaxAttempts, "the upsert updates the budget")
}

func TestPublishStampsPayloadAndOptions(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	register(t, r, "billing", orderCreated{}.EventType(), 3)
	tenant := uuid.New()
	id, err := r.Publisher().Publish(ctx, orderCreated{Value: "hello"},
		WithAggregate("order", "ord_9"),
		WithVersion(7),
		WithMeta("origin", "test"),
		WithMeta("region", "eu"),
		WithTenantID(tenant),
	)
	is.NoError(err)

	view, err := r.Manager().Get(ctx, id)
	is.NoError(err)
	is.NotNil(view)
	is.Equal("orders.created.v1", view.Type)
	is.Equal("order", view.AggregateType)
	is.Equal("ord_9", view.AggregateID)
	is.EqualValues(7, view.Version)
	is.Equal(tenant, view.TenantID)
	is.Equal(map[string]string{"origin": "test", "region": "eu"}, view.Meta)
	is.JSONEq(`{"value":"hello"}`, string(view.Payload))
}

func TestPublishExplicitTraceWinsOverContext(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	// Ambient span context on ctx.
	ctxTraceID := trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	ctxSpanID := trace.SpanID{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: ctxTraceID, SpanID: ctxSpanID, TraceFlags: trace.FlagsSampled,
	}))

	register(t, r, "billing", orderCreated{}.EventType(), 3)

	// Explicit WithTrace must override the ambient span context.
	explicitTrace := "0f0e0d0c0b0a09080706050403020100"
	explicitSpan := "2211ffeeddccbbaa"
	id, err := r.Publisher().Publish(ctx, orderCreated{Value: "x"},
		WithTrace(explicitTrace, explicitSpan, 0))
	is.NoError(err)

	// Lease the delivery to inspect the rehydrated ledger trace.
	jobs, err := f.DequeueBatch(context.Background(), driver.SourceEvent,
		driver.DequeueParams{Kind: "billing", Limit: 1, Lease: time.Minute})
	is.NoError(err)
	is.Len(jobs, 1)
	is.Equal(id, jobs[0].EventID)
	is.Equal(explicitTrace, jobs[0].Event.TraceID, "explicit WithTrace must win over TraceFromContext")
	is.Equal(explicitSpan, jobs[0].Event.SpanID)
}

func TestPublishAppliesTraceFromContextWhenNoExplicit(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	traceID := trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := trace.SpanID{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	}))

	register(t, r, "billing", orderCreated{}.EventType(), 3)
	_, err := r.Publisher().Publish(ctx, orderCreated{Value: "x"})
	is.NoError(err)

	jobs, err := f.DequeueBatch(context.Background(), driver.SourceEvent,
		driver.DequeueParams{Kind: "billing", Limit: 1, Lease: time.Minute})
	is.NoError(err)
	is.Len(jobs, 1)
	is.Equal(traceID.String(), jobs[0].Event.TraceID, "the ambient span must be stamped when no explicit trace is given")
	is.Equal(spanID.String(), jobs[0].Event.SpanID)
	is.Equal(int16(trace.FlagsSampled), jobs[0].Event.TraceFlags)
}

func TestPublishRequiresEventType(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())

	_, err := r.Publisher().Publish(context.Background(), emptyTypeEvent{})
	is.Error(err)
	is.Contains(err.Error(), "event type is required")
}

func TestPublishMarshalFailure(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())

	_, err := r.Publisher().Publish(context.Background(), badEvent{})
	is.Error(err)
	is.Contains(err.Error(), "marshal")
}

// emptyTypeEvent has an empty EventType, which Publish must reject.
type emptyTypeEvent struct{}

func (emptyTypeEvent) EventType() string { return "" }

// badEvent cannot marshal (channels are not JSON-serializable).
type badEvent struct {
	Ch chan int `json:"ch"`
}

func (badEvent) EventType() string { return "orders.bad.v1" }

// txFake adds a trivial driver.TxStore[struct{}] over the fake so the positive
// TxPublisher path can be exercised without a transactional backend.
type txFake struct {
	*drivertest.Fake
}

func (f *txFake) EnqueueTx(ctx context.Context, _ struct{}, p driver.EnqueueParams) (bool, error) {
	return f.Enqueue(ctx, p)
}

func (f *txFake) PublishTx(ctx context.Context, _ struct{}, p driver.PublishParams) (int, error) {
	return f.Publish(ctx, p)
}

var _ driver.TxStore[struct{}] = (*txFake)(nil)

func TestTxPublisherRequiresTxStoreDriver(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake()) // implements no TxStore

	_, err := TxPublisher[struct{}](r)
	is.Error(err)
	is.Contains(err.Error(), "does not support transactional publishes")
	is.Contains(err.Error(), "struct {}")
}

func TestTxPublisherPublishesThroughTx(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := &txFake{Fake: drivertest.NewFake()}
	core, err := azync.New(f, azync.WithLogger(discardLogger()))
	is.NoError(err)
	r, err := New(core, fastOptions()...)
	is.NoError(err)
	ctx := context.Background()

	register(t, r, "billing", orderCreated{}.EventType(), 3)
	tp, err := TxPublisher[struct{}](r)
	is.NoError(err)

	id, err := tp.PublishTx(ctx, struct{}{}, orderCreated{Value: "in-tx"})
	is.NoError(err)

	view, err := r.Manager().Get(ctx, id)
	is.NoError(err)
	is.NotNil(view)
	is.JSONEq(`{"value":"in-tx"}`, string(view.Payload))
	// The fan-out created the billing delivery inside the same call.
	is.Equal(driver.StatePending, deliveryOf(t, f.Fake, "billing").State)
}
