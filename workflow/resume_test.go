package workflow

import (
	"context"
	"testing"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestManagerSuspendThenResumeCompletesExecution proves the operator
// freeze/recover cycle end to end: Suspend parks the execution (the worker
// consumes its task without replaying), Resume flips it back AND re-enqueues
// the workflow-task at once — no reconciler wait — and the execution then
// runs to completion.
func TestManagerSuspendThenResumeCompletesExecution(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	RegisterWorkflow(r.Worker(), "wf-freeze", "1", func(Context, struct{}) (string, error) {
		return "done", nil
	})

	res, err := r.Client().Start(ctx, "wf-freeze", "1", nil)
	is.NoError(err)

	is.NoError(r.Manager().Suspend(ctx, res.WorkflowID, "provider outage"))
	view, err := r.Manager().Get(ctx, res.WorkflowID)
	is.NoError(err)
	is.Equal(driver.WorkflowSuspended, view.State)
	is.Equal("provider outage", view.FailureReason)

	// While suspended, the worker consumes the pending task without replay.
	processed, err := r.Worker().ProcessNext(ctx)
	is.NoError(err)
	is.True(processed, "the parked task is consumed, not replayed")
	view, err = r.Manager().Get(ctx, res.WorkflowID)
	is.NoError(err)
	is.Equal(driver.WorkflowSuspended, view.State, "consuming the task must not advance a suspended execution")

	// Resume re-enqueues immediately: the very next ProcessNext completes it,
	// with no dependence on the stalled-workflow reconciler.
	is.NoError(r.Manager().Resume(ctx, res.WorkflowID))
	jobs, _, err := f.ListJobs(ctx, driver.SourceWorkflow,
		driver.JobFilter{Kind: workflowTaskKind("wf-freeze")}, 0, 10)
	is.NoError(err)
	var live int
	for _, j := range jobs {
		if j.State == driver.StatePending || j.State == driver.StateScheduled {
			live++
		}
	}
	is.Equal(1, live, "Resume must re-enqueue the workflow-task itself")

	view = drainUntilTerminal(t, r, res.WorkflowID, 10)
	is.Equal(driver.WorkflowSucceeded, view.State)
	is.JSONEq(`"done"`, string(view.Result))
}

// TestManagerResumeRequiresSuspended pins the guard rails: resuming a
// running, terminal or missing execution is not-found.
func TestManagerResumeRequiresSuspended(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	RegisterWorkflow(r.Worker(), "wf-guard", "1", func(Context, struct{}) (string, error) {
		return "ok", nil
	})
	res, err := r.Client().Start(ctx, "wf-guard", "1", nil)
	is.NoError(err)

	err = r.Manager().Resume(ctx, res.WorkflowID)
	is.True(IsNotFound(err), "resuming a running execution is not-found")

	view := drainUntilTerminal(t, r, res.WorkflowID, 10)
	is.Equal(driver.WorkflowSucceeded, view.State)
	err = r.Manager().Resume(ctx, res.WorkflowID)
	is.True(IsNotFound(err), "resuming a terminal execution is not-found")

	err = r.Manager().Resume(ctx, uuid.New())
	is.True(IsNotFound(err), "resuming a missing execution is not-found")

	err = r.Manager().Suspend(ctx, uuid.New(), "x")
	is.True(IsNotFound(err), "suspending a missing execution is not-found")
}

// TestResumeAfterReplaySuspensionRecovers closes the loop the docs promise:
// a replay-error suspension (corrupted history exhausting the task budget)
// is recoverable with Manager.Resume once the underlying cause is fixed.
func TestResumeAfterReplaySuspensionRecovers(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithWorkerMode(WorkerModeWorkflowOnly))

	calls := 0
	RegisterWorkflow(r.Worker(), "wf-recover", "1", func(ctx Context, _ struct{}) (string, error) {
		calls++
		return "recovered", nil
	})
	res, err := r.Client().Start(ctx, "wf-recover", "1", nil)
	is.NoError(err)

	// Simulate the operator freeze after an incident.
	is.NoError(r.Manager().Suspend(ctx, res.WorkflowID, "replay violation under investigation"))
	processed, err := r.Worker().ProcessNext(ctx)
	is.NoError(err)
	is.True(processed) // the task is consumed while suspended

	is.NoError(r.Manager().Resume(ctx, res.WorkflowID))
	view := drainUntilTerminal(t, r, res.WorkflowID, 10)
	is.Equal(driver.WorkflowSucceeded, view.State)
	is.Positive(calls, "the resumed execution replayed and completed")
}
