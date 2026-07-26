package event

import (
	"context"
	"testing"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
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
	id, err := r.Publisher().Publish(ctx, orderCreated{Value: "hello"},
		WithAggregate("order", "ord_9"),
		WithVersion(7),
		WithMeta("origin", "test"),
		WithMeta("region", "eu"),
		WithMeta("tenant_id", "ten_1"),
	)
	is.NoError(err)

	view, err := r.Manager().Get(ctx, id)
	is.NoError(err)
	is.NotNil(view)
	is.Equal("orders.created.v1", view.Type)
	is.Equal("order", view.AggregateType)
	is.Equal("ord_9", view.AggregateID)
	is.EqualValues(7, view.Version)
	is.Equal(map[string]string{"origin": "test", "region": "eu", "tenant_id": "ten_1"}, view.Meta)
	is.JSONEq(`{"value":"hello"}`, string(view.Payload))
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
