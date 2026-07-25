package queue

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

type cronArgs struct {
	Tag string `json:"tag"`
}

func (cronArgs) Kind() string { return "queue.cron" }

// cronRuntime builds a runtime whose cron loop ticks fast and reads an
// injectable clock.
func cronRuntime(t *testing.T, f *drivertest.Fake, clk *drivertest.ManualClock) *Runtime {
	t.Helper()
	r := newTestRuntime(t, f, WithCron(true), WithCronTick(5*time.Millisecond))
	r.Worker().now = clk.Now
	return r
}

// runCronLoop drives only the worker's cron loop (no engine), so tests observe
// exactly what the scheduler enqueues.
func runCronLoop(t *testing.T, w *Worker, elector driver.LeaderElector) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.cronLoop(ctx, elector)
	}()
	var stopped bool
	stop := func() {
		if !stopped {
			stopped = true
			cancel()
			<-done
		}
	}
	t.Cleanup(stop)
	return stop
}

// awaitCronLeader waits until some cron loop holds the leadership (the fake
// refuses a second holder). Tests must reach this point before jumping the
// cron clock: leadership acquisition computes each schedule's next occurrence
// from "now", and a pre-acquisition jump would legitimately skip it (the
// no-backfill semantics).
func awaitCronLeader(t *testing.T, f *drivertest.Fake) {
	t.Helper()
	is := require.New(t)
	is.Eventually(func() bool {
		release, acquired, err := f.AcquireLeadership(context.Background(), "cron")
		if err != nil {
			return false
		}
		if acquired {
			release() // nobody leads yet; put it back
			return false
		}
		return true
	}, 2*time.Second, 2*time.Millisecond, "a cron loop must take leadership")
}

func countJobs(t *testing.T, f *drivertest.Fake, kind string) int64 {
	t.Helper()
	is := require.New(t)
	_, total, err := f.ListJobs(context.Background(), driver.SourceQueue, driver.JobFilter{Kind: kind}, 0, 100)
	is.NoError(err)
	return total
}

func TestRegisterCronInvalidSpecFails(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	err := r.Worker().RegisterCron("bad", "not a spec", cronArgs{})
	is.Error(err)
	is.Contains(err.Error(), `queue: cron "bad" spec`)
}

func TestRegisterCronDuplicateFails(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	is.NoError(r.Worker().RegisterCron("nightly", "0 3 * * *", cronArgs{}))
	err := r.Worker().RegisterCron("nightly", "0 4 * * *", cronArgs{})
	is.Error(err)
	is.Contains(err.Error(), `queue: cron "nightly" already registered`)
}

func TestRegisterCronAfterStartFails(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	err := r.Worker().RegisterCron("late", "* * * * *", cronArgs{})
	is.Error(err)
	is.Contains(err.Error(), "queue: cannot register cron after Start")
}

func TestCronNoBackfillOnLeadershipAcquisition(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 30, 0, time.UTC))
	r := cronRuntime(t, f, clk)
	is.NoError(r.Worker().RegisterCron("minutely", "* * * * *", cronArgs{Tag: "m"}))

	runCronLoop(t, r.Worker(), f)
	awaitCronLeader(t, f)

	// The leader counts from "now": the 12:00:00 occurrence is in the past and
	// must never be backfilled.
	time.Sleep(40 * time.Millisecond) // several ticks with leadership held
	is.Zero(countJobs(t, f, "queue.cron"))
}

func TestCronSingleLeaderEnqueuesEachOccurrenceOnce(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 30, 0, time.UTC))

	first := cronRuntime(t, f, clk)
	second := cronRuntime(t, f, clk)
	for _, r := range []*Runtime{first, second} {
		is.NoError(r.Worker().RegisterCron("minutely", "* * * * *", cronArgs{Tag: "m"}))
	}
	runCronLoop(t, first.Worker(), f)
	runCronLoop(t, second.Worker(), f)

	// Exactly one loop acquires the leadership.
	awaitCronLeader(t, f)

	// The next minute boundary comes due: with two live loops, only the leader
	// enqueues, once.
	clk.Set(time.Date(2026, 7, 22, 12, 1, 5, 0, time.UTC))
	is.Eventually(func() bool {
		return countJobs(t, f, "queue.cron") == 1
	}, 2*time.Second, 2*time.Millisecond)
	time.Sleep(30 * time.Millisecond) // several more ticks: still exactly one
	is.Equal(int64(1), countJobs(t, f, "queue.cron"))
}

func TestCronOccurrenceDedupeAcrossSkewedLeaders(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 30, 0, time.UTC))

	// Leader A enqueues the 12:01:00 occurrence.
	first := cronRuntime(t, f, clk)
	is.NoError(first.Worker().RegisterCron("minutely", "* * * * *", cronArgs{Tag: "m"}))
	stopFirst := runCronLoop(t, first.Worker(), f)
	awaitCronLeader(t, f)
	clk.Set(time.Date(2026, 7, 22, 12, 1, 5, 0, time.UTC))
	is.Eventually(func() bool {
		return countJobs(t, f, "queue.cron") == 1
	}, 2*time.Second, 2*time.Millisecond)
	stopFirst() // releases leadership

	// Leader B runs with a clock skewed behind the occurrence: it recomputes
	// 12:01:00 as pending and tries to enqueue it again. The occurrence
	// idempotency key must deduplicate it.
	clk.Set(time.Date(2026, 7, 22, 12, 0, 40, 0, time.UTC))
	second := cronRuntime(t, f, clk)
	is.NoError(second.Worker().RegisterCron("minutely", "* * * * *", cronArgs{Tag: "m"}))
	runCronLoop(t, second.Worker(), f)
	awaitCronLeader(t, f)

	clk.Set(time.Date(2026, 7, 22, 12, 1, 5, 0, time.UTC))
	time.Sleep(40 * time.Millisecond) // several ticks past the duplicate occurrence
	is.Equal(int64(1), countJobs(t, f, "queue.cron"),
		"the same occurrence enqueued by two leaders must produce exactly one job")
}

func TestCronDisabledWithoutLeaderElector(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	core, err := azyncNew(storeOnly{Store: f}) // masks LeaderElector (and Notifier)
	is.NoError(err)
	r, err := New(core, append(fastOptions(), WithCron(true), WithCronTick(5*time.Millisecond))...)
	is.NoError(err)
	is.NoError(r.Worker().RegisterCron("minutely", "* * * * *", cronArgs{}))

	done := make(chan struct{}, 1)
	is.NoError(Register(r.Worker(), func(context.Context, testArgs) error {
		done <- struct{}{}
		return nil
	}))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	// Cron silently disabled (a warning is logged); normal jobs keep flowing.
	_, err = r.Producer().Enqueue(context.Background(), testArgs{Value: "x"})
	is.NoError(err)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker without leader election stopped processing jobs")
	}
	is.Zero(countJobs(t, f, "queue.cron"))
}

func TestWithCronFalseNeverTakesLeadership(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 30, 0, time.UTC))
	r := newTestRuntime(t, f, WithCron(false), WithCronTick(5*time.Millisecond))
	r.Worker().now = clk.Now
	is.NoError(r.Worker().RegisterCron("minutely", "* * * * *", cronArgs{}))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())
	time.Sleep(30 * time.Millisecond)

	// Nobody holds the cron leadership and no occurrence was enqueued.
	release, acquired, err := f.AcquireLeadership(context.Background(), "cron")
	is.NoError(err)
	is.True(acquired, "a disabled cron must not take leadership")
	release()
	is.Zero(countJobs(t, f, "queue.cron"))
}

func TestCronRunsThroughWorkerStart(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 50, 0, time.UTC))
	r := cronRuntime(t, f, clk)
	is.NoError(r.Worker().RegisterCron("minutely", "* * * * *", cronArgs{Tag: "wired"}))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())
	awaitCronLeader(t, f)

	clk.Set(time.Date(2026, 7, 22, 12, 1, 5, 0, time.UTC))
	is.Eventually(func() bool {
		return countJobs(t, f, "queue.cron") == 1
	}, 2*time.Second, 2*time.Millisecond, "Worker.Start must run the cron scheduler")
}
