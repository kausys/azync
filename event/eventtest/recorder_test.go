package eventtest

import (
	"context"
	"sync"
	"testing"

	"github.com/kausys/azync/event"

	"github.com/stretchr/testify/require"
)

type orderCreated struct {
	Value string `json:"value"`
}

func (orderCreated) EventType() string { return "orders.created.v1" }

type orderCancelled struct{}

func (orderCancelled) EventType() string { return "orders.cancelled.v1" }

func TestRecorderCapturesAndFiltersByType(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	rec := New(t)
	ctx := context.Background()

	_, err := rec.Publish(ctx, orderCreated{Value: "a"}, event.WithVersion(1))
	is.NoError(err)
	_, err = rec.Publish(ctx, orderCancelled{})
	is.NoError(err)
	_, err = rec.Publish(ctx, orderCreated{Value: "b"})
	is.NoError(err)

	rec.RequireLen(t, 3)
	is.Equal(3, rec.Len())

	created := Of[orderCreated](rec)
	is.Len(created, 2)
	is.Equal("a", created[0].Value)
	is.Equal("b", created[1].Value)

	// The first record recorded its single option.
	all := rec.All()
	is.Equal(1, all[0].OptCount)
	is.NotEqual(all[0].ID, all[1].ID, "each publish gets a fresh id")
}

func TestRecorderRequireOneAndNone(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	rec := New(t)

	_, err := rec.Publish(context.Background(), orderCreated{Value: "only"})
	is.NoError(err)

	got := RequireOne[orderCreated](t, rec)
	is.Equal("only", got.Value)
	RequireNone[orderCancelled](t, rec)
}

func TestRecorderReset(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	rec := New(t)

	_, err := rec.Publish(context.Background(), orderCreated{Value: "x"})
	is.NoError(err)
	is.Equal(1, rec.Len())

	rec.Reset()
	is.Zero(rec.Len())
	is.Empty(rec.All())
	RequireNone[orderCreated](t, rec)
}

func TestRecorderIsConcurrencySafe(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	rec := New(t)

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			_, _ = rec.Publish(context.Background(), orderCreated{Value: "x"})
		})
	}
	wg.Wait()
	is.Equal(50, rec.Len())
}
