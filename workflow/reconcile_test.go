package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

// TestReconcilerRevivesStalledExecution proves the reconciler
// (Worker.reconcileOnce, driven by ListStalledWorkflows) revives a running
// execution whose only task was removed out from under it — defense-in-depth
// for something outside StartWorkflow/SignalWorkflow's atomic guarantee (an
// operator deleting a job by hand, here) — by re-enqueuing a fresh
// workflow-task, letting the execution complete normally afterward.
func TestReconcilerRevivesStalledExecution(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	clk := drivertest.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f, WithLeaseTTL(time.Second))

	var calls int
	RegisterOperation(r.Worker(), "op", "1", func(context.Context, struct{}) (string, error) {
		calls++
		return "done", nil
	})
	RegisterWorkflow(r.Worker(), "wf-stalled", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "op", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	res, err := r.Client().Start(ctx, "wf-stalled", "1", nil)
	is.NoError(err)

	// Strand it: delete the task StartWorkflow scheduled atomically.
	jobs, _, err := f.ListJobs(ctx, driver.SourceWorkflow,
		driver.JobFilter{Kind: workflowTaskKind("wf-stalled")}, 0, 10)
	is.NoError(err)
	is.Len(jobs, 1, "StartWorkflow must have scheduled exactly one task")
	is.NoError(f.DeleteJob(ctx, driver.SourceWorkflow, jobs[0].ID, jobs[0].State))

	processed, err := r.Worker().ProcessNext(ctx)
	is.NoError(err)
	is.False(processed, "genuinely nothing left to process once the task is gone")

	// Past stalledAfter (5 * leaseTTL = 5s): the reconciler must now find it.
	clk.Advance(time.Hour)
	r.Worker().reconcileOnce(ctx)

	view := drainUntilTerminal(t, r, res.WorkflowID, 10)
	is.Equal(driver.WorkflowSucceeded, view.State)
	is.Equal(1, calls)
}

// TestReconcilerIgnoresHealthyAndTooRecentExecutions proves the reconciler
// leaves alone an execution with a live task, and one whose task went
// missing too recently to be past the stalledAfter window.
func TestReconcilerIgnoresHealthyAndTooRecentExecutions(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	clk := drivertest.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f, WithLeaseTTL(time.Second))

	RegisterWorkflow(r.Worker(), "wf-healthy", "1", func(Context, struct{}) (string, error) {
		return "ok", nil
	})
	healthy, err := r.Client().Start(ctx, "wf-healthy", "1", nil)
	is.NoError(err)

	RegisterWorkflow(r.Worker(), "wf-recent", "1", func(Context, struct{}) (string, error) {
		return "ok", nil
	})
	_, err = r.Client().Start(ctx, "wf-recent", "1", nil)
	is.NoError(err)
	jobs, _, err := f.ListJobs(ctx, driver.SourceWorkflow,
		driver.JobFilter{Kind: workflowTaskKind("wf-recent")}, 0, 10)
	is.NoError(err)
	is.Len(jobs, 1)
	is.NoError(f.DeleteJob(ctx, driver.SourceWorkflow, jobs[0].ID, jobs[0].State))

	// No time has passed: neither execution is past stalledAfter.
	r.Worker().reconcileOnce(ctx)

	v, err := r.Manager().Get(ctx, healthy.WorkflowID)
	is.NoError(err)
	is.Equal(driver.WorkflowRunning, v.State, "an untouched execution must be unaffected by reconciliation")

	stillGone, _, err := f.ListJobs(ctx, driver.SourceWorkflow,
		driver.JobFilter{Kind: workflowTaskKind("wf-recent")}, 0, 10)
	is.NoError(err)
	is.Empty(stillGone, "a too-recent stall must not be revived yet")
}

// TestReconcileLoopRunsOnItsOwnTicker proves reconcileLoop itself — the
// background ticker Start spawns, not just reconcileOnce — revives a
// stranded execution on its own cadence: withReconcileInterval shrinks that
// cadence so the test can observe a real sweep without advancing a clock by
// hand.
func TestReconcileLoopRunsOnItsOwnTicker(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithLeaseTTL(time.Millisecond), withReconcileInterval(20*time.Millisecond))

	var calls int
	RegisterOperation(r.Worker(), "op", "1", func(context.Context, struct{}) (string, error) {
		calls++
		return "done", nil
	})
	RegisterWorkflow(r.Worker(), "wf-loop-stalled", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "op", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	res, err := r.Client().Start(ctx, "wf-loop-stalled", "1", nil)
	is.NoError(err)

	jobs, _, err := f.ListJobs(ctx, driver.SourceWorkflow,
		driver.JobFilter{Kind: workflowTaskKind("wf-loop-stalled")}, 0, 10)
	is.NoError(err)
	is.Len(jobs, 1, "StartWorkflow must have scheduled exactly one task")
	is.NoError(f.DeleteJob(ctx, driver.SourceWorkflow, jobs[0].ID, jobs[0].State))

	workerCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = r.Worker().Start(workerCtx) }()

	view := drainUntilTerminal(t, r, res.WorkflowID, 200)
	is.Equal(driver.WorkflowSucceeded, view.State)
	is.Equal(1, calls)
}
