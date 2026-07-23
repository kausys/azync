package queue

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"
	"github.com/kausys/azync/internal/engine"

	"github.com/stretchr/testify/require"
)

func TestWorkerProcessesJobEndToEnd(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	got := make(chan Job[testArgs], 1)
	is.NoError(Register(r.Worker(), func(_ context.Context, job Job[testArgs]) error {
		got <- job
		return nil
	}))

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "payload"}, Meta("k", "v"))
	is.NoError(err)

	startWorker(t, r.Worker())
	select {
	case job := <-got:
		is.Equal(res.ID, job.ID)
		is.Equal("payload", job.Args.Value)
		is.Equal(1, job.Attempt)
		is.Equal(map[string]string{"k": "v"}, job.Meta)
	case <-time.After(2 * time.Second):
		t.Fatal("job did not run")
	}
	is.Eventually(func() bool {
		return getJob(t, f, res.ID).State == driver.StateSucceeded
	}, 2*time.Second, 2*time.Millisecond)
}

func TestFailedJobReschedulesWithBackoff(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f)

	var runs atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, Job[testArgs]) error {
		runs.Add(1)
		return errors.New("transient")
	}))

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "x"})
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return getJob(t, f, res.ID).State == driver.StateScheduled
	}, 2*time.Second, 2*time.Millisecond)

	got := getJob(t, f, res.ID)
	is.True(got.RunAt.Equal(clk.Now().Add(engine.Backoff(1))),
		"a plain error must park the job at now+Backoff(attempt): %v", got.RunAt)
	is.Equal("transient", got.LastError)
	is.Equal(int32(1), runs.Load(), "the frozen clock must keep the retry parked")
}

func TestRetryAfterFixedDelayThenCompletes(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f, withMaintenanceIntervals(2*time.Millisecond, time.Hour))

	var runs atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, Job[testArgs]) error {
		if runs.Add(1) == 1 {
			return RetryAfter(errors.New("warming up"), 10*time.Millisecond)
		}
		return nil
	}))

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "x"})
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return getJob(t, f, res.ID).State == driver.StateScheduled
	}, 2*time.Second, 2*time.Millisecond)
	is.True(getJob(t, f, res.ID).RunAt.Equal(clk.Now().Add(10*time.Millisecond)),
		"RetryAfter must use the fixed delay, not the backoff")

	// Make the retry due; promotion moves it back to pending and it completes.
	clk.Advance(time.Second)
	is.Eventually(func() bool {
		return getJob(t, f, res.ID).State == driver.StateSucceeded
	}, 2*time.Second, 2*time.Millisecond)
	is.Equal(int32(2), runs.Load())
}

func TestAbortDeadLettersImmediately(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	var runs atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, Job[testArgs]) error {
		runs.Add(1)
		return Abort(errors.New("permanent failure"))
	}))

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "x"})
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return getJob(t, f, res.ID).State == driver.StateDead
	}, 2*time.Second, 2*time.Millisecond)
	is.Equal("permanent failure", getJob(t, f, res.ID).LastError)
	is.Equal(int32(1), runs.Load(), "Abort must not retry")
}

func TestReportableDiesWhenBudgetExhausts(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f, withMaintenanceIntervals(2*time.Millisecond, time.Hour))

	var runs atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, Job[testArgs]) error {
		runs.Add(1)
		return Reportable(errors.New("keeps failing"))
	}, WithMaxRetries(2)))

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "x"})
	is.NoError(err)

	startWorker(t, r.Worker())
	// Attempt 1 fails -> scheduled at now+Backoff(1); make it due.
	is.Eventually(func() bool {
		return getJob(t, f, res.ID).State == driver.StateScheduled
	}, 2*time.Second, 2*time.Millisecond)
	clk.Advance(time.Minute)

	// Attempt 2 exhausts the budget: dead.
	is.Eventually(func() bool {
		return getJob(t, f, res.ID).State == driver.StateDead
	}, 2*time.Second, 2*time.Millisecond)
	is.Equal(int32(2), runs.Load())
	is.Equal(2, getJob(t, f, res.ID).Attempt)
}

func TestExhaustedBudgetWithFixedDelaysGoesDead(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f, withMaintenanceIntervals(2*time.Millisecond, time.Hour))

	var runs atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, Job[testArgs]) error {
		runs.Add(1)
		return RetryAfter(errors.New("always fails"), time.Millisecond)
	}, WithMaxRetries(3)))

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "x"})
	is.NoError(err)

	startWorker(t, r.Worker())
	deadline := time.Now().Add(4 * time.Second)
	for getJob(t, f, res.ID).State != driver.StateDead {
		if time.Now().After(deadline) {
			t.Fatalf("job did not exhaust its budget; state=%s runs=%d", getJob(t, f, res.ID).State, runs.Load())
		}
		clk.Advance(time.Second) // keep every reschedule due
		time.Sleep(2 * time.Millisecond)
	}
	is.Equal(int32(3), runs.Load(), "the budget must bound handler executions")
	attempts, err := f.JobAttempts(context.Background(), driver.SourceQueue, res.ID)
	is.NoError(err)
	is.Len(attempts, 3, "every failed attempt must be recorded")
}

func TestJobTimeoutCancelsHandlerAndReschedules(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	canceled := make(chan struct{}, 1)
	is.NoError(Register(r.Worker(), func(ctx context.Context, _ Job[testArgs]) error {
		<-ctx.Done() // only the per-job timeout can fire here
		canceled <- struct{}{}
		return ctx.Err()
	}, WithJobTimeout(20*time.Millisecond)))

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "x"})
	is.NoError(err)

	startWorker(t, r.Worker())
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("job timeout did not cancel the handler")
	}
	is.Eventually(func() bool {
		got := getJob(t, f, res.ID)
		return got.State == driver.StateScheduled
	}, 2*time.Second, 2*time.Millisecond)
	is.Contains(getJob(t, f, res.ID).LastError, context.DeadlineExceeded.Error())
}

func TestRuntimeDefaultJobTimeoutApplies(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithDefaultJobTimeout(20*time.Millisecond))

	canceled := make(chan struct{}, 1)
	is.NoError(Register(r.Worker(), func(ctx context.Context, _ Job[testArgs]) error {
		<-ctx.Done()
		canceled <- struct{}{}
		return ctx.Err()
	})) // no per-kind timeout: the runtime default must bound it

	_, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "x"})
	is.NoError(err)

	startWorker(t, r.Worker())
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime default job timeout did not apply")
	}
}

func TestPerKindConcurrencyIsRespected(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	const jobs = 6
	var inflight, maxInflight, processed atomic.Int32
	allDone := make(chan struct{})
	is.NoError(Register(r.Worker(), func(context.Context, Job[testArgs]) error {
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
		return nil
	}, WithConcurrency(2)))

	for range jobs {
		_, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "slow"})
		is.NoError(err)
	}
	startWorker(t, r.Worker())

	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		t.Fatal("jobs did not finish")
	}
	is.Equal(int32(2), maxInflight.Load())
}

func TestShutdownDrainLetsSlowJobFinish(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f) // ShutdownDrain 2s via fastOptions

	started := make(chan struct{}, 1)
	var canceled atomic.Bool
	is.NoError(Register(r.Worker(), func(ctx context.Context, _ Job[testArgs]) error {
		started <- struct{}{}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			canceled.Store(true)
		}
		return nil
	}))

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "slow"})
	is.NoError(err)
	stop := startWorker(t, r.Worker())

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}
	is.ErrorIs(stop(), context.Canceled)
	is.False(canceled.Load(), "a job inside the drain budget must complete, not be cancelled")
	is.Equal(driver.StateSucceeded, getJob(t, f, res.ID).State)
}

func TestReadyClosesAfterStart(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	is.NoError(Register(r.Worker(), func(context.Context, Job[testArgs]) error { return nil }))

	select {
	case <-r.Worker().Ready():
		t.Fatal("Ready closed before Start")
	default:
	}
	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())
}

func TestWakeNudgesIdleWorker(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	// Polling alone would take seconds; only the Notifier wake can beat the
	// deadline.
	r := newTestRuntime(t, f,
		WithFetchPollInterval(10*time.Second), WithIdleBackoffMax(10*time.Second))

	done := make(chan struct{}, 1)
	is.NoError(Register(r.Worker(), func(context.Context, Job[testArgs]) error {
		done <- struct{}{}
		return nil
	}))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())
	time.Sleep(20 * time.Millisecond) // let the fetch loop reach its idle wait

	_, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "wake"})
	is.NoError(err)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("wake did not nudge the idle fetch loop before the poll fallback")
	}
}

// storeOnly masks every optional capability of the fake (Notifier,
// LeaderElector), leaving the bare driver.Store: the poll-only path.
type storeOnly struct {
	driver.Store
}

func TestPollOnlyStoreStillProcessesJobs(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	core, err := azyncNew(storeOnly{Store: f})
	is.NoError(err)
	r, err := New(core, fastOptions()...)
	is.NoError(err)

	done := make(chan struct{}, 1)
	is.NoError(Register(r.Worker(), func(context.Context, Job[testArgs]) error {
		done <- struct{}{}
		return nil
	}))

	_, err = r.Producer().Enqueue(context.Background(), testArgs{Value: "poll"})
	is.NoError(err)
	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll-only worker did not process the job")
	}
}
