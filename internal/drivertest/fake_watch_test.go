package drivertest_test

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// awaitFakeChange drains ch until pred matches or the timeout expires. The
// change contract is at-most-once with interleaved resets, so tests match by
// predicate instead of asserting exact sequences.
func awaitFakeChange(t *testing.T, ch <-chan driver.Change, pred func(driver.Change) bool) driver.Change {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case c, ok := <-ch:
			if !ok {
				t.Fatal("change channel closed before the expected change arrived")
			}
			if pred(c) {
				return c
			}
		case <-deadline:
			t.Fatal("expected change did not arrive")
		}
	}
}

func TestChangeNotifierFirstDeliveryIsReset(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, _ := newFake(t)

	changes, err := f.Changes(t.Context())
	is.NoError(err)
	is.NotNil(changes)

	select {
	case c := <-changes:
		is.Equal(driver.ChangeReset, c.Entity, "every subscription opens with a reset")
	case <-time.After(time.Second):
		t.Fatal("expected the initial reset")
	}
}

func TestChangeNotifierEmitsJobLifecycle(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := t.Context()
	f, _ := newFake(t)

	changes, err := f.Changes(ctx)
	is.NoError(err)

	id := uuid.New()
	_, err = f.Enqueue(ctx, driver.EnqueueParams{ID: id, Kind: "send", Payload: payload()})
	is.NoError(err)
	c := awaitFakeChange(t, changes, func(c driver.Change) bool {
		return c.Entity == driver.ChangeJob && c.ID == id
	})
	is.Equal(driver.SourceQueue, c.Source)
	is.Equal("send", c.Kind)
	is.Equal(string(driver.StatePending), c.State)

	leased, err := f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "send", Limit: 1, Lease: time.Minute})
	is.NoError(err)
	is.Len(leased, 1)
	awaitFakeChange(t, changes, func(c driver.Change) bool {
		return c.ID == id && c.State == string(driver.StateActive)
	})

	is.NoError(f.Ack(ctx, id, leased[0].LeaseToken))
	awaitFakeChange(t, changes, func(c driver.Change) bool {
		return c.ID == id && c.State == string(driver.StateSucceeded)
	})
}

func TestChangeNotifierEmitsEventAppend(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := t.Context()
	f, _ := newFake(t)

	changes, err := f.Changes(ctx)
	is.NoError(err)

	id := uuid.New()
	_, err = f.Publish(ctx, driver.PublishParams{ID: id, Type: "user.signed_up", OccurredAt: f.Clock.Now(), Payload: payload()})
	is.NoError(err)
	c := awaitFakeChange(t, changes, func(c driver.Change) bool {
		return c.Entity == driver.ChangeEvent && c.ID == id
	})
	is.Equal("user.signed_up", c.Kind)
}

func TestChangeNotifierOverflowAnnouncesGapWithReset(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := t.Context()
	f, _ := newFake(t)

	changes, err := f.Changes(ctx)
	is.NoError(err)

	// Overrun the subscription buffer without consuming anything: the extra
	// hints are dropped and the subscription flips into overflow mode.
	for range 400 {
		_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: uuid.New(), Kind: "flood", Payload: payload()})
		is.NoError(err)
	}
	for len(changes) > 0 {
		<-changes
	}

	// The next delivery after the gap must be preceded by a reset.
	marker := uuid.New()
	_, err = f.Enqueue(ctx, driver.EnqueueParams{ID: marker, Kind: "flood", Payload: payload()})
	is.NoError(err)

	first := <-changes
	is.Equal(driver.ChangeReset, first.Entity, "a dropped hint must surface as an in-band reset")
	awaitFakeChange(t, changes, func(c driver.Change) bool { return c.ID == marker })
}

func TestChangeNotifierChannelClosesOnCtxEnd(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	f, _ := newFake(t)

	changes, err := f.Changes(ctx)
	is.NoError(err)
	cancel()

	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-changes:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("channel did not close after ctx cancellation")
		}
	}
}
