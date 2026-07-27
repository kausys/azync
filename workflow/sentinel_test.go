package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kausys/azync/dag"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

// These tests pin the cross-runtime sentinel contract (engine.OutcomeError):
// dag's error taxonomy keeps its declared semantics inside a workflow
// Operation instead of silently degrading to a budget-burning generic retry —
// which is exactly what used to happen.

// TestOperationNotReadySnoozesWithoutConsumingAttempt proves dag.NotReady(d)
// inside an Operation parks the job with the sentinel's own delay and hands
// the attempt back — the trap turned feature.
func TestOperationNotReadySnoozesWithoutConsumingAttempt(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	calls := 0
	RegisterOperation(r.Worker(), "poll", "1", func(context.Context, struct{}) (string, error) {
		calls++
		if calls == 1 {
			return "", dag.NotReady(45 * time.Minute)
		}
		return "ready", nil
	})
	RegisterWorkflow(r.Worker(), "wf-notready", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "poll", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	_, err := r.Client().Start(ctx, "wf-notready", "1", nil)
	is.NoError(err)
	runOneWorkflowTask(t, r.Worker()) // schedules the operation, parks

	// First pass: the handler reports NotReady — the job must snooze.
	processed, err := r.Worker().ProcessNext(ctx)
	is.NoError(err)
	is.True(processed)

	jobs, _, err := f.ListJobs(ctx, driver.SourceWorkflow,
		driver.JobFilter{Kind: "$op:poll@1"}, 0, 10)
	is.NoError(err)
	is.Len(jobs, 1)
	op := jobs[0]
	is.Equal(driver.StateScheduled, op.State, "NotReady snoozes the Operation job")
	is.Zero(op.Attempt, "the attempt is handed back: no budget burned")
	is.True(op.RunAt.After(time.Now().Add(30*time.Minute)),
		"the sentinel's own delay is honored, not operationRetryDelay")
	attempts, err := f.JobAttempts(ctx, driver.SourceWorkflow, op.ID)
	is.NoError(err)
	is.Empty(attempts, "a snooze records no attempt history")
}

// TestOperationDagAbortFailsImmediately proves dag.Abort inside an Operation
// skips the remaining retries: OperationFailed lands at once and the Future
// resolves failed on the very first attempt.
func TestOperationDagAbortFailsImmediately(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithDefaultMaxRetries(5))

	calls := 0
	RegisterOperation(r.Worker(), "doomed", "1", func(context.Context, struct{}) (string, error) {
		calls++
		return "", dag.Abort(errors.New("provider rejected: FAIL"))
	})
	failed := make(chan error, 1)
	RegisterWorkflow(r.Worker(), "wf-abort", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "doomed", "1", struct{}{}).Get(&out); err != nil {
			failed <- err
			return "", err
		}
		return out, nil
	})

	res, err := r.Client().Start(ctx, "wf-abort", "1", nil)
	is.NoError(err)

	view := drainUntilTerminal(t, r, res.WorkflowID, 10)
	is.Equal(driver.WorkflowFailed, view.State)
	is.Equal(1, calls, "Abort must not consume the retry budget one attempt at a time")
	select {
	case err := <-failed:
		is.Contains(err.Error(), "provider rejected")
	default:
		t.Fatal("the workflow never observed the failed Operation")
	}
}

// TestOperationDagSkipCompletesWithNullResult proves dag.Skip inside an
// Operation completes it: the Future resolves successfully with a null
// result, and the Operation job settles skipped — distinguishable in ops.
func TestOperationDagSkipCompletesWithNullResult(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	RegisterOperation(r.Worker(), "maybe", "1", func(context.Context, struct{}) (string, error) {
		return "", dag.Skip("already VERIFIED")
	})
	RegisterWorkflow(r.Worker(), "wf-skip", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "maybe", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return "done:" + out, nil
	})

	res, err := r.Client().Start(ctx, "wf-skip", "1", nil)
	is.NoError(err)
	view := drainUntilTerminal(t, r, res.WorkflowID, 10)
	is.Equal(driver.WorkflowSucceeded, view.State)
	is.JSONEq(`"done:"`, string(view.Result), "the skipped Operation resolves as success with a zero result")

	jobs, _, err := f.ListJobs(ctx, driver.SourceWorkflow,
		driver.JobFilter{Kind: "$op:maybe@1"}, 0, 10)
	is.NoError(err)
	is.Len(jobs, 1)
	is.Equal(driver.StateSkipped, jobs[0].State, "the ops surface shows the deliberate no-op")
}
