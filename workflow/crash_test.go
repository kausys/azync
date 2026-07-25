package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

// TestCrashAfterOperationScheduledRecovers: if a workflow task parks after
// OperationScheduled, a subsequent ProcessNext runs the Operation and completes
// the workflow — simulating crash recovery between schedule and execute.
func TestCrashAfterOperationScheduledRecovers(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithLeaseTTL(time.Minute))

	var calls int
	RegisterOperation(r.Worker(), "step", "1", func(_ context.Context, _ struct{}) (string, error) {
		calls++
		return "done", nil
	})
	RegisterWorkflow(r.Worker(), "wf-crash", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "step", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	res, err := r.Client().Start(ctx, "wf-crash", "1", nil)
	is.NoError(err)

	// Pass 1: schedule Operation + park (workflow-task acked).
	runOneWorkflowTask(t, r.Worker())
	is.Equal(0, calls)

	events, err := f.ListHistory(ctx, res.WorkflowID)
	is.NoError(err)
	is.Equal("OperationScheduled", events[len(events)-1].Type)

	// Simulate a new worker picking up the Operation job, then the wake.
	view := drainUntilTerminal(t, r, res.WorkflowID, 10)
	is.Equal(1, calls)
	is.Equal(driver.WorkflowSucceeded, view.State)
}

// TestOperationRetryThenSucceed exercises retry_wait → active → completed.
func TestOperationRetryThenSucceed(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f,
		WithLeaseTTL(time.Minute),
		WithDefaultMaxRetries(3),
		WithOperationRetryDelay(5*time.Millisecond),
	)

	var calls int
	RegisterOperation(r.Worker(), "flaky", "1", func(_ context.Context, _ struct{}) (string, error) {
		calls++
		if calls < 2 {
			return "", errString("transient")
		}
		return "ok", nil
	})
	RegisterWorkflow(r.Worker(), "wf-retry", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "flaky", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	res, err := r.Client().Start(ctx, "wf-retry", "1", nil)
	is.NoError(err)

	view := drainUntilTerminal(t, r, res.WorkflowID, 30)
	is.GreaterOrEqual(calls, 2)
	is.Equal(driver.WorkflowSucceeded, view.State)
	is.JSONEq(`"ok"`, string(view.Result))
}

type errString string

func (e errString) Error() string { return string(e) }
