package event

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

// TestLedgerRetentionRemovesEventAfterTerminalDelivery proves the retention
// loop, once enabled, sweeps ledger events past retention whose deliveries
// have all reached a terminal state.
//
// OccurredAt is stamped by the Publisher with the real wall clock (see
// publisher.go), not any injectable clock, so this test uses a tiny real
// retention window and a short sleep rather than a ManualClock (which would
// only move Worker.now, leaving OccurredAt on real time and the two
// permanently out of sync).
func TestLedgerRetentionRemovesEventAfterTerminalDelivery(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()

	r := newTestRuntime(t, f, WithLedgerRetention(20*time.Millisecond), withRetentionInterval(5*time.Millisecond))

	delivered := make(chan struct{}, 1)
	is.NoError(r.Worker().Register(namedSubscriber("billing"), On(func(context.Context, orderCreated) error {
		delivered <- struct{}{}
		return nil
	})))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	res, err := r.Publisher().Publish(context.Background(), orderCreated{Value: "x"})
	is.NoError(err)

	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery did not complete")
	}
	is.Eventually(func() bool {
		return deliveryOf(t, f, "billing").State == driver.StateSucceeded
	}, 2*time.Second, 2*time.Millisecond)

	is.Eventually(func() bool {
		_, err := f.GetEvent(context.Background(), res)
		return driver.IsNotFound(err)
	}, 2*time.Second, 2*time.Millisecond, "the event must be retained (removed) once past the window")
}

// TestLedgerRetentionDisabledByDefault proves that without
// WithLedgerRetention, the ledger is kept forever: no retention loop ever
// runs, matching the pre-existing behavior for callers that do not opt in.
func TestLedgerRetentionDisabledByDefault(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()

	r := newTestRuntime(t, f, withRetentionInterval(5*time.Millisecond))

	delivered := make(chan struct{}, 1)
	is.NoError(r.Worker().Register(namedSubscriber("billing"), On(func(context.Context, orderCreated) error {
		delivered <- struct{}{}
		return nil
	})))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	res, err := r.Publisher().Publish(context.Background(), orderCreated{Value: "x"})
	is.NoError(err)

	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery did not complete")
	}

	time.Sleep(50 * time.Millisecond) // several ticks, if the loop were running
	_, err = f.GetEvent(context.Background(), res)
	is.NoError(err, "the ledger is retained forever when WithLedgerRetention is unset")
}
