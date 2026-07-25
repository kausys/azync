package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// startEngine runs e.Start on a cancellable context and returns a stop func
// that cancels it and waits for Start to return, plus the returned error.
func startEngine(t *testing.T, e *Engine) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Start(ctx) }()
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

func enqueue(t *testing.T, f *drivertest.Fake, kind string) uuid.UUID {
	t.Helper()
	is := require.New(t)
	id := uuid.New()
	_, err := f.Enqueue(context.Background(), driver.EnqueueParams{
		ID: id, Kind: kind, Payload: json.RawMessage(`{}`), MaxAttempts: 3, MaxAttemptsExplicit: true,
	})
	is.NoError(err)
	return id
}

func TestStartProcessesJobEndToEnd(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	done := make(chan uuid.UUID, 1)
	is.NoError(e.Register(Kind{Name: "send", Concurrency: 2,
		Handler: func(_ context.Context, job driver.Job) (json.RawMessage, error) {
			done <- job.ID
			return nil, nil
		}}))

	id := enqueue(t, f, "send")
	startEngine(t, e)

	select {
	case got := <-done:
		is.Equal(id, got)
	case <-time.After(2 * time.Second):
		t.Fatal("job was not processed")
	}
	is.Eventually(func() bool {
		return getJob(t, f, id).State == driver.StateSucceeded
	}, 2*time.Second, 2*time.Millisecond)
}

func TestWakeNudgesIdleFetchLoop(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	settings := testSettings()
	// Polling alone would take seconds; only a wake can beat the deadline.
	settings.FetchPollInterval = 10 * time.Second
	settings.IdleBackoffMax = 10 * time.Second
	e := newTestEngine(f, settings)

	done := make(chan struct{}, 1)
	is.NoError(e.Register(Kind{Name: "send", Concurrency: 1,
		Handler: func(context.Context, driver.Job) (json.RawMessage, error) {
			done <- struct{}{}
			return nil, nil
		}}))

	startEngine(t, e)
	select {
	case <-e.Ready():
	case <-time.After(time.Second):
		t.Fatal("engine did not become ready")
	}
	// Let the fetch loop reach its idle wait before enqueueing.
	time.Sleep(20 * time.Millisecond)

	enqueue(t, f, "send")
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("wake did not nudge the fetch loop before the poll fallback")
	}
}

func TestPerKindConcurrencyIsRespected(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	const concurrency = 2
	const jobs = 6
	var inflight, maxInflight atomic.Int32
	var processed atomic.Int32
	allDone := make(chan struct{})
	is.NoError(e.Register(Kind{Name: "slow", Concurrency: concurrency,
		Handler: func(context.Context, driver.Job) (json.RawMessage, error) {
			cur := inflight.Add(1)
			for {
				prev := maxInflight.Load()
				if cur <= prev || maxInflight.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			inflight.Add(-1)
			if processed.Add(1) == jobs {
				close(allDone)
			}
			return nil, nil
		}}))

	for range jobs {
		enqueue(t, f, "slow")
	}
	startEngine(t, e)

	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		t.Fatal("jobs did not finish")
	}
	is.Equal(int32(concurrency), maxInflight.Load(),
		"per-kind semaphore must cap in-flight handlers at the kind concurrency")
}

func TestGlobalConcurrencyCapsAcrossKinds(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	settings := testSettings()
	settings.MaxConcurrency = 1
	e := newTestEngine(f, settings)

	var inflight, maxInflight atomic.Int32
	var processed atomic.Int32
	allDone := make(chan struct{})
	handler := func(context.Context, driver.Job) (json.RawMessage, error) {
		cur := inflight.Add(1)
		for {
			prev := maxInflight.Load()
			if cur <= prev || maxInflight.CompareAndSwap(prev, cur) {
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
		inflight.Add(-1)
		if processed.Add(1) == 4 {
			close(allDone)
		}
		return nil, nil
	}
	is.NoError(e.Register(Kind{Name: "a", Concurrency: 2, Handler: handler}))
	is.NoError(e.Register(Kind{Name: "b", Concurrency: 2, Handler: handler}))

	for range 2 {
		enqueue(t, f, "a")
		enqueue(t, f, "b")
	}
	startEngine(t, e)

	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		t.Fatal("jobs did not finish")
	}
	is.Equal(int32(1), maxInflight.Load(),
		"the shared global semaphore must cap total in-flight handlers")
}

func TestShutdownDrainsInflightJob(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	settings := testSettings()
	settings.ShutdownDrain = 2 * time.Second
	e := newTestEngine(f, settings)

	started := make(chan struct{}, 1)
	var canceled atomic.Bool
	is.NoError(e.Register(Kind{Name: "slow", Concurrency: 1,
		Handler: func(ctx context.Context, _ driver.Job) (json.RawMessage, error) {
			started <- struct{}{}
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				canceled.Store(true)
			}
			return nil, nil
		}}))

	id := enqueue(t, f, "slow")
	stop := startEngine(t, e)

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}
	err := stop()
	is.ErrorIs(err, context.Canceled)
	is.False(canceled.Load(), "handler within the drain budget must not be cancelled")
	is.Equal(driver.StateSucceeded, getJob(t, f, id).State,
		"the drained job must settle even though the worker ctx ended")
}

func TestShutdownCancelsJobsPastDrainBudget(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	settings := testSettings()
	settings.ShutdownDrain = 20 * time.Millisecond
	e := newTestEngine(f, settings)

	started := make(chan struct{}, 1)
	handlerCanceled := make(chan struct{})
	is.NoError(e.Register(Kind{Name: "stuck", Concurrency: 1,
		Handler: func(ctx context.Context, _ driver.Job) (json.RawMessage, error) {
			started <- struct{}{}
			<-ctx.Done() // only the drain cancellation releases this handler
			close(handlerCanceled)
			return nil, ctx.Err()
		}}))

	enqueue(t, f, "stuck")
	stop := startEngine(t, e)

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}
	err := stop()
	is.ErrorIs(err, context.Canceled)
	select {
	case <-handlerCanceled:
	case <-time.After(time.Second):
		t.Fatal("drain budget exhaustion did not cancel the stuck handler")
	}
}

func TestReadyClosesOnlyAfterStart(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	select {
	case <-e.Ready():
		t.Fatal("Ready closed before Start")
	default:
	}

	startEngine(t, e)
	select {
	case <-e.Ready():
	case <-time.After(time.Second):
		t.Fatal("Ready did not close after Start")
	}
	is.True(e.Started())
}

func TestStartTwiceFails(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())
	startEngine(t, e)

	select {
	case <-e.Ready():
	case <-time.After(time.Second):
		t.Fatal("engine did not become ready")
	}
	err := e.Start(context.Background())
	is.Error(err)
	is.Contains(err.Error(), "already started")
}

// wakeFailStore fails its first Wake to prove Start can be retried after a
// setup failure (the started flag is reset).
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

func TestStartCanRetryAfterWakeSetupFailure(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	store := &wakeFailStore{Fake: drivertest.NewFake()}
	e := New(Config{Store: store, Source: driver.SourceQueue, Logger: discardLogger(), Settings: testSettings()})

	err := e.Start(context.Background())
	is.Error(err)
	is.Contains(err.Error(), "wake")
	is.False(e.Started(), "a failed setup must allow a retry")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	is.ErrorIs(e.Start(ctx), context.Canceled)
}

func TestRegisterDuplicateAndAfterStart(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	handler := func(context.Context, driver.Job) (json.RawMessage, error) { return nil, nil }
	is.NoError(e.Register(Kind{Name: "send", Concurrency: 1, Handler: handler}))
	err := e.Register(Kind{Name: "send", Concurrency: 1, Handler: handler})
	is.Error(err)
	is.Contains(err.Error(), `kind "send" already registered`)

	startEngine(t, e)
	select {
	case <-e.Ready():
	case <-time.After(time.Second):
		t.Fatal("engine did not become ready")
	}
	err = e.Register(Kind{Name: "other", Concurrency: 1, Handler: handler})
	is.Error(err)
	is.Contains(err.Error(), "cannot register after start")
}

func TestRenewLeaseKeepsLongJobAlive(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake() // system clock: leases really expire
	settings := testSettings()
	settings.LeaseTTL = 100 * time.Millisecond // renew every 50ms, reap sweep every 100ms
	e := newTestEngine(f, settings)

	var runs atomic.Int32
	done := make(chan struct{}, 4)
	is.NoError(e.Register(Kind{Name: "long", Concurrency: 1,
		Handler: func(context.Context, driver.Job) (json.RawMessage, error) {
			runs.Add(1)
			time.Sleep(300 * time.Millisecond) // three lease TTLs
			done <- struct{}{}
			return nil, nil
		}}))

	id := enqueue(t, f, "long")
	startEngine(t, e)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("long job did not finish")
	}
	is.Eventually(func() bool {
		return getJob(t, f, id).State == driver.StateSucceeded
	}, 2*time.Second, 5*time.Millisecond)
	is.Equal(int32(1), runs.Load(),
		"lease renewal must keep the job owned; a reap would have re-run it")
	is.Equal(0, getJob(t, f, id).ReapCount)
}

func TestRenewLeaseFailureCancelsHandler(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	settings := testSettings()
	settings.LeaseTTL = 40 * time.Millisecond // renew every 20ms
	e := newTestEngine(f, settings)

	job := leaseOne(t, f, "held", 5)
	// Steal the lease out from under the executor before it starts.
	is.NoError(f.Release(context.Background(), job.ID, job.LeaseToken))

	handlerCanceled := make(chan struct{})
	k := Kind{Name: "held", Concurrency: 1,
		Handler: func(ctx context.Context, _ driver.Job) (json.RawMessage, error) {
			<-ctx.Done() // only a renew failure can cancel this ctx
			close(handlerCanceled)
			return nil, ctx.Err()
		},
		Classify: retryClassifier(Outcome{Kind: OutcomeRetry}),
	}
	finished := make(chan struct{})
	go func() {
		e.execute(context.Background(), k, job, func() {})
		close(finished)
	}()

	select {
	case <-handlerCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("losing the lease did not cancel the handler")
	}
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("execute did not return after the stale settle")
	}
	// The stale token could not resettle the job; the Release outcome stands.
	is.Equal(driver.StatePending, getJob(t, f, job.ID).State)
}
