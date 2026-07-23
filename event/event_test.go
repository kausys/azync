package event

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

// orderCreated is the canonical typed event of the suite.
type orderCreated struct {
	Value string `json:"value"`
}

func (orderCreated) EventType() string { return "orders.created.v1" }

// orderCancelled is a second event type, for isolation and filter tests.
type orderCancelled struct{}

func (orderCancelled) EventType() string { return "orders.cancelled.v1" }

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// azyncNew builds a quiet Core over any store (used to mask fake capabilities).
func azyncNew(store driver.Store) (*azync.Core, error) {
	return azync.New(store, azync.WithLogger(discardLogger()))
}

// newTestRuntime composes an event runtime over a fake store with fast fetch
// tuning.
func newTestRuntime(t *testing.T, f *drivertest.Fake, opts ...Option) *Runtime {
	t.Helper()
	is := require.New(t)
	core, err := azyncNew(f)
	is.NoError(err)
	r, err := New(core, append(fastOptions(), opts...)...)
	is.NoError(err)
	return r
}

func fastOptions() []Option {
	return []Option{
		WithFetchCooldown(time.Millisecond),
		WithFetchPollInterval(5 * time.Millisecond),
		WithIdleBackoffMax(10 * time.Millisecond),
		WithLeaseTTL(time.Minute),
		WithShutdownDrain(2 * time.Second),
	}
}

// startWorker runs the worker and returns a stop func that cancels it and waits
// for Start to return its error.
func startWorker(t *testing.T, w *Worker) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()
	var result error
	var once atomic.Bool
	stop := func() error {
		if once.CompareAndSwap(false, true) {
			cancel()
			result = <-done
		}
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func awaitReady(t *testing.T, w *Worker) {
	t.Helper()
	select {
	case <-w.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not become ready")
	}
}

// register adds a subscriber directly through the publisher.
func register(t *testing.T, r *Runtime, name, eventType string, maxAttempts int) {
	t.Helper()
	require.NoError(t, r.Publisher().Register(context.Background(),
		Subscriber{Name: name, EventType: eventType, MaxAttempts: maxAttempts}))
}

// deliveryOf returns the single delivery job of a subscriber.
func deliveryOf(t *testing.T, f *drivertest.Fake, subscriber string) driver.Job {
	t.Helper()
	is := require.New(t)
	rows, _, err := f.ListJobs(context.Background(), driver.SourceEvent,
		driver.JobFilter{Kind: subscriber}, 0, 10)
	is.NoError(err)
	is.Len(rows, 1)
	return rows[0]
}

func TestNewRejectsNilCore(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	_, err := New(nil)
	is.Error(err)
	is.Contains(err.Error(), "core is nil")
}

// registerOpenTestDriver registers the test scheme exactly once per process
// (RegisterDriver panics on duplicates, e.g. under go test -count=2).
var registerOpenTestDriver = sync.OnceFunc(func() {
	azync.RegisterDriver("azyncevent-opentest", func(string, driver.Config) (driver.Store, error) {
		return drivertest.NewFake(), nil
	})
})

func TestOpenOwnsCoreAndLayersOptions(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	registerOpenTestDriver()

	r, err := Open("azyncevent-opentest://ignored",
		WithCoreOptions(azync.WithLogger(discardLogger()),
			azync.WithLeaseTTL(70*time.Second), azync.WithDefaultMaxAttempts(9)),
		WithDefaultMaxAttempts(4))
	is.NoError(err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })

	is.Equal(70*time.Second, r.cfg.LeaseTTL, "WithCoreOptions must reach the owned Core's defaults")
	is.Equal(4, r.cfg.DefaultMaxAttempts, "the event option must win over the core option")
	is.True(r.ownedCore, "Open must own its Core so Close releases it")
}

func TestOpenRejectsInvalidOptionBeforeDialing(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	// The scheme is never registered: option validation must fail first, before
	// any driver is dialed.
	_, err := Open("azyncevent-never-registered://x", WithLeaseTTL(0))
	is.Error(err)
	is.Contains(err.Error(), "WithLeaseTTL")
}

func TestCloseOnSharedCoreLeavesCoreOpen(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	is.NoError(r.Close(context.Background()))

	// The shared core's store keeps working after the runtime closes.
	register(t, r, "billing", orderCreated{}.EventType(), 1)
	_, err := r.Publisher().Publish(context.Background(), orderCreated{Value: "after-close"})
	is.NoError(err)
}
