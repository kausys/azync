package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

type sleepGate struct{}

func (sleepGate) Kind() string { return "sched.gate" }

func TestSleepCompletesWhenDue(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f)

	var gateRuns atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, sleepGate) (None, error) {
		gateRuns.Add(1)
		return None{}, nil
	}))

	// gate -> nap: the timer only starts once its dependency succeeds.
	res, err := r.Client().Run(context.Background(),
		Define("timer").
			Task("gate", sleepGate{}).
			Sleep("nap", time.Hour, After("gate")))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		nap := taskByKey(t, f, res.ID, "nap")
		return nap.State == driver.StateScheduled && nap.RunAt.Equal(clk.Now().Add(time.Hour))
	}, 2*time.Second, 2*time.Millisecond, "the timer starts against the store clock once its dep succeeds")

	// Not due yet: the workflow must not complete.
	is.Never(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowSucceeded
	}, 60*time.Millisecond, 10*time.Millisecond, "the sleep must not complete before it is due")

	clk.Advance(2 * time.Hour)
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowSucceeded
	}, 2*time.Second, 2*time.Millisecond, "once due, CompleteDueSleeps finishes the timer and the workflow")
	is.Equal(int32(1), gateRuns.Load(), "the sleep runs no handler")
	is.Equal(driver.StateSucceeded, taskByKey(t, f, res.ID, "nap").State)
}

func TestSignalWakesSleepEarly(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f)

	// A root sleep far in the future: only an early signal can complete it,
	// since the clock never advances.
	res, err := r.Client().Run(context.Background(),
		Define("early").Sleep("nap", 24*time.Hour))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return taskByKey(t, f, res.ID, "nap").State == driver.StateScheduled
	}, 2*time.Second, 2*time.Millisecond)

	// A signal named after the sleep key wakes it (run_at = now), so
	// CompleteDueSleeps finishes it without advancing the clock.
	is.NoError(r.Client().Signal(context.Background(), res.ID, "nap", nil))
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowSucceeded
	}, 2*time.Second, 2*time.Millisecond, "the signal wakes the timer early")
}

func TestVacuumRemovesTerminalWorkflowsPastRetention(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f,
		WithWorkflowRetention(time.Hour),
		withSchedulerIntervals(2*time.Millisecond, 5*time.Millisecond))
	is.NoError(Register(r.Worker(), func(context.Context, sleepGate) (None, error) { return None{}, nil }))

	res, err := r.Client().Run(context.Background(), Define("vac").Task("t", sleepGate{}))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowSucceeded
	}, 2*time.Second, 2*time.Millisecond)

	// Age the workflow past the retention window; the vacuum tick removes it.
	clk.Advance(2 * time.Hour)
	is.Eventually(func() bool {
		v, err := r.Manager().Get(context.Background(), res.ID)
		is.NoError(err)
		return v == nil
	}, 2*time.Second, 2*time.Millisecond, "a terminal workflow past retention is vacuumed, cascading its tasks")

	tasks, err := r.Manager().Tasks(context.Background(), res.ID)
	is.NoError(err)
	is.Nil(tasks, "the vacuum cascades to the workflow's task jobs")
}

func TestZeroRetentionKeepsTerminalWorkflowsForever(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f,
		WithWorkflowRetention(0),
		withSchedulerIntervals(2*time.Millisecond, 5*time.Millisecond))
	is.NoError(Register(r.Worker(), func(context.Context, sleepGate) (None, error) { return None{}, nil }))

	res, err := r.Client().Run(context.Background(), Define("keep").Task("t", sleepGate{}))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowSucceeded
	}, 2*time.Second, 2*time.Millisecond)

	clk.Advance(1000 * time.Hour)
	is.Never(func() bool {
		v, err := r.Manager().Get(context.Background(), res.ID)
		is.NoError(err)
		return v == nil
	}, 80*time.Millisecond, 10*time.Millisecond, "explicit 0 retention never vacuums")
}
