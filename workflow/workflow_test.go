package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// newTestRuntime composes a workflow Runtime over a fresh in-memory Fake,
// matching dag.newTestRuntime's pattern for its own sibling runtime.
func newTestRuntime(t *testing.T, f *drivertest.Fake, opts ...Option) *Runtime {
	t.Helper()
	is := require.New(t)
	core, err := azync.New(f, azync.WithLogger(discardLogger()))
	is.NoError(err)
	r, err := New(core, opts...)
	is.NoError(err)
	return r
}

// --- New / Open error paths --------------------------------------------------

// storeOnly narrows a driver.Store to the plain interface, hiding any
// optional capability (here, driver.WorkflowStore) a concrete Store also
// implements — the same technique dag_test.go uses to exercise the
// "capability missing" error path against the very same Fake.
type storeOnly struct{ driver.Store }

func TestNewRequiresWorkflowStore(t *testing.T) {
	is := require.New(t)
	core, err := azync.New(storeOnly{Store: drivertest.NewFake()}, azync.WithLogger(discardLogger()))
	is.NoError(err)

	_, err = New(core)
	is.Error(err)
	is.Contains(err.Error(), "workflow-as-code")
}

func TestNewNilCore(t *testing.T) {
	is := require.New(t)
	_, err := New(nil)
	is.Error(err)
}

// --- Client.Start dedupe ------------------------------------------------------

func TestClientStartDedupe(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	res, err := r.Client().Start(ctx, "wf-dedupe", "1", map[string]int{"n": 1},
		WithBusinessIdempotencyKey("biz-1"))
	is.NoError(err)
	is.False(res.Deduplicated)
	first := res.WorkflowID

	res2, err := r.Client().Start(ctx, "wf-dedupe", "1", map[string]int{"n": 2},
		WithBusinessIdempotencyKey("biz-1"))
	is.NoError(err)
	is.True(res2.Deduplicated, "a live execution holds the (name, key)")
	is.Equal(first, res2.WorkflowID)

	// A distinct business key starts a fresh execution.
	res3, err := r.Client().Start(ctx, "wf-dedupe", "1", nil, WithBusinessIdempotencyKey("biz-2"))
	is.NoError(err)
	is.False(res3.Deduplicated)
	is.NotEqual(first, res3.WorkflowID)

	// Start also durably records WorkflowStarted and enqueues the first
	// workflow-task job.
	events, err := f.ListHistory(ctx, first)
	is.NoError(err)
	is.Len(events, 1)
	is.Equal("WorkflowStarted", events[0].Type)

	jobs, err := f.DequeueBatch(ctx, driver.SourceWorkflow, driver.DequeueParams{
		Kind: workflowTaskKind("wf-dedupe"), Limit: 10, Lease: time.Minute,
	})
	is.NoError(err)
	is.Len(jobs, 2, "one task per newly-started execution, not the deduplicated one")
}

func TestClientStartWithWorkflowID(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	id := uuid.New()
	res, err := r.Client().Start(ctx, "wf-explicit-id", "1", nil, WithWorkflowID(id))
	is.NoError(err)
	is.Equal(id, res.WorkflowID)
}

// --- history append via Start/Signal ----------------------------------------

func TestClientSignalAppendsHistoryAndDedupes(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	res, err := r.Client().Start(ctx, "wf-signal", "1", nil)
	is.NoError(err)

	delivered, err := r.Client().Signal(ctx, res.WorkflowID, "approved", map[string]string{"by": "ops"},
		WithMessageID("m1"))
	is.NoError(err)
	is.True(delivered)

	// A repeated MessageID delivers nothing and appends nothing further.
	delivered, err = r.Client().Signal(ctx, res.WorkflowID, "approved", nil, WithMessageID("m1"))
	is.NoError(err)
	is.False(delivered)

	events, err := f.ListHistory(ctx, res.WorkflowID)
	is.NoError(err)
	// WorkflowStarted, then exactly one SignalReceived.
	is.Len(events, 2)
	is.Equal("WorkflowStarted", events[0].Type)
	is.Equal(int64(1), events[0].Seq)
	is.Equal("SignalReceived", events[1].Type)
	is.Equal(int64(2), events[1].Seq)
}

// --- end-to-end: one Operation, then complete --------------------------------

type checkInput struct {
	Ref string `json:"ref"`
}

type checkOutput struct {
	Status string `json:"status"`
}

// runOneWorkflowTask drains exactly one due job (workflow or Operation).
func runOneWorkflowTask(t *testing.T, w *Worker) {
	t.Helper()
	is := require.New(t)
	processed, err := w.ProcessNext(context.Background())
	is.NoError(err)
	is.True(processed, "expected one due workflow/operation job")
}

// drainUntilTerminal processes jobs until the execution is terminal or steps
// are exhausted (leased Operation + wake + complete needs several steps).
func drainUntilTerminal(t *testing.T, r *Runtime, id uuid.UUID, maxSteps int) driver.WorkflowExecutionView {
	t.Helper()
	is := require.New(t)
	ctx := context.Background()
	var view driver.WorkflowExecutionView
	var err error
	for range maxSteps {
		view, err = r.Manager().Get(ctx, id)
		is.NoError(err)
		switch view.State {
		case driver.WorkflowSucceeded, driver.WorkflowFailed, driver.WorkflowCancelled:
			return view
		default:
			// Still running or suspended — keep draining.
		}
		processed, err := r.Worker().ProcessNext(ctx)
		is.NoError(err)
		if !processed {
			time.Sleep(5 * time.Millisecond)
		}
	}
	view, err = r.Manager().Get(ctx, id)
	is.NoError(err)
	is.Failf("drain", "workflow %s still %s after %d steps", id, view.State, maxSteps)
	return view
}

func TestWorkerRunsOneOperationAndCompletes(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithLeaseTTL(time.Minute))

	var calls int
	RegisterOperation(r.Worker(), "check-status", "1", func(ctx context.Context, in checkInput) (checkOutput, error) {
		calls++
		is.Equal("abc", in.Ref)
		is.NotEmpty(ExecutionKey(ctx))
		return checkOutput{Status: "approved"}, nil
	})
	RegisterWorkflow(r.Worker(), "wf-onestep", "1", func(ctx Context, in checkInput) (checkOutput, error) {
		f := ExecuteOperation(ctx, "check-status", "1", in)
		var out checkOutput
		if err := f.Get(&out); err != nil {
			return checkOutput{}, err
		}
		return out, nil
	})

	res, err := r.Client().Start(ctx, "wf-onestep", "1", checkInput{Ref: "abc"})
	is.NoError(err)
	is.False(res.Deduplicated)

	view := drainUntilTerminal(t, r, res.WorkflowID, 10)
	is.Equal(1, calls)
	is.Equal(driver.WorkflowSucceeded, view.State)
	is.JSONEq(`{"status":"approved"}`, string(view.Result))

	events, err := f.ListHistory(ctx, res.WorkflowID)
	is.NoError(err)
	types := make([]string, len(events))
	for i, ev := range events {
		types[i] = ev.Type
	}
	is.Equal([]string{
		"WorkflowStarted", "OperationScheduled", "OperationCompleted", "WorkflowCompleted",
	}, types)

	processed, err := r.Worker().ProcessNext(ctx)
	is.NoError(err)
	is.False(processed)
}

// TestOperationPanicDoesNotCrashWorkerAndFlowsToFailure proves a panicking
// Operation handler is recovered (mirroring internal/engine.runHandler) and
// settles through the normal retry/dead path instead of taking down the
// worker process.
func TestOperationPanicDoesNotCrashWorkerAndFlowsToFailure(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithLeaseTTL(time.Minute), WithDefaultMaxRetries(1))

	RegisterOperation(r.Worker(), "boom", "1", func(context.Context, struct{}) (string, error) {
		panic("kaboom")
	})
	RegisterWorkflow(r.Worker(), "wf-panic", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "boom", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	res, err := r.Client().Start(ctx, "wf-panic", "1", nil)
	is.NoError(err)

	is.NotPanics(func() {
		runOneWorkflowTask(t, r.Worker()) // schedules the operation, parks
		runOneWorkflowTask(t, r.Worker()) // leases+runs it: panics internally, recovered
	}, "a panicking Operation handler must not crash the worker")

	view := drainUntilTerminal(t, r, res.WorkflowID, 10)
	is.Equal(driver.WorkflowFailed, view.State)
	is.Contains(view.FailureReason, "operation panic")
}

// TestReplayErrorBacksOffThenSuspendsAfterExhaustingRetries proves a
// determinism-violation-style replay failure (here: a corrupted
// OperationCompleted payload the cursor cannot decode) consumes the job's
// retry budget with exponential backoff instead of spinning immediately at
// full speed (the pre-fix behavior: Release, no budget consumed, run_at =
// now()), and that exhausting the budget dead-letters the job and suspends
// the execution — alertable and recoverable — rather than retrying forever.
func TestReplayErrorBacksOffThenSuspendsAfterExhaustingRetries(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	clk := drivertest.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	// WorkflowOnly: the real Operation job this test's ExecuteOperation call
	// schedules is deliberately left unprocessed (its OperationScheduled
	// history record is overwritten below with a corrupted OperationCompleted
	// instead) — a combined-mode worker would otherwise dequeue and complete
	// it first, waking the workflow itself and producing an extra task job
	// that confuses this test's single-job assertions.
	r := newTestRuntime(t, f, WithLeaseTTL(time.Minute), WithDefaultMaxRetries(2), WithWorkerMode(WorkerModeWorkflowOnly))

	RegisterOperation(r.Worker(), "op", "1", func(context.Context, struct{}) (string, error) {
		return "unused", nil
	})
	RegisterWorkflow(r.Worker(), "wf-corrupt", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "op", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	res, err := r.Client().Start(ctx, "wf-corrupt", "1", nil)
	is.NoError(err)
	runOneWorkflowTask(t, r.Worker()) // schedules the operation, parks

	// Corrupt history directly: an OperationCompleted record with a payload
	// the cursor cannot decode, simulating a determinism violation (e.g. the
	// registered code changed incompatibly under the same version) without
	// needing to actually reproduce one.
	_, err = f.AppendHistory(ctx, res.WorkflowID, "OperationCompleted", json.RawMessage(`{not valid json`))
	is.NoError(err)
	is.NoError(enqueueWorkflowTask(ctx, f, "wf-corrupt", res.WorkflowID, time.Time{}))

	// First replay attempt: fails, reschedules with backoff — not dead yet,
	// and the workflow is still running (not yet suspended).
	processed, err := r.Worker().ProcessNext(ctx)
	is.NoError(err)
	is.True(processed)

	// The original (now succeeded, exempt-from-vacuum) workflow-task job that
	// scheduled the operation still exists alongside the retried one — find
	// the one this replay-error path actually rescheduled.
	jobs, _, err := f.ListJobs(ctx, driver.SourceWorkflow,
		driver.JobFilter{Kind: workflowTaskKind("wf-corrupt"), State: driver.StateScheduled}, 0, 10)
	is.NoError(err)
	is.Len(jobs, 1, "a replay failure within budget must reschedule, not spin at run_at=now()")
	retriedJob := jobs[0]
	is.Equal(1, retriedJob.Attempt, "the attempt must be consumed, unlike Release")
	is.True(retriedJob.RunAt.After(clk.Now()), "backoff must push run_at into the future, not now()")

	view, err := r.Manager().Get(ctx, res.WorkflowID)
	is.NoError(err)
	is.Equal(driver.WorkflowRunning, view.State)

	// Advance past the backoff delay and let the second (final) attempt run.
	clk.Advance(time.Hour)
	processed, err = r.Worker().ProcessNext(ctx)
	is.NoError(err)
	is.True(processed)

	got, err := f.GetJob(ctx, driver.SourceWorkflow, retriedJob.ID)
	is.NoError(err)
	is.Equal(driver.StateDead, got.State, "exhausting the retry budget must dead-letter the job")

	view, err = r.Manager().Get(ctx, res.WorkflowID)
	is.NoError(err)
	is.Equal(driver.WorkflowSuspended, view.State, "exhaustion must suspend the execution, not fail or spin forever")
	is.Contains(view.FailureReason, "replay failed after 2 attempts")
}

func TestOperationUncertainAndResolveComplete(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithLeaseTTL(time.Minute), WithDefaultMaxRetries(1))

	RegisterOperation(r.Worker(), "mutate", "1", func(_ context.Context, _ struct{}) (string, error) {
		return "", fmt.Errorf("ambiguous send: %w", ErrUncertain)
	})
	RegisterWorkflow(r.Worker(), "wf-uncertain", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "mutate", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	res, err := r.Client().Start(ctx, "wf-uncertain", "1", nil)
	is.NoError(err)

	// workflow task schedules op and parks; then op marks uncertain.
	runOneWorkflowTask(t, r.Worker())
	runOneWorkflowTask(t, r.Worker())

	view, err := r.Manager().Get(ctx, res.WorkflowID)
	is.NoError(err)
	is.Equal(driver.WorkflowSuspended, view.State)

	jobs, _, err := f.ListJobs(ctx, driver.SourceWorkflow, driver.JobFilter{State: driver.StateUncertain}, 0, 10)
	is.NoError(err)
	is.NotEmpty(jobs, "expected an uncertain operation job")
	opJob := jobs[0]

	is.NoError(r.Manager().ResolveUncertain(ctx, opJob.ID, string(driver.UncertainComplete),
		json.RawMessage(`"ok"`)))

	view = drainUntilTerminal(t, r, res.WorkflowID, 10)
	is.Equal(driver.WorkflowSucceeded, view.State)
	is.JSONEq(`"ok"`, string(view.Result))
}

// TestWorkerSignalDuringParkedTimerSelect pins a real interleaving: a
// signal delivered while the workflow is parked in Select(WaitSignal,
// Sleep) is appended after the pending Sleep's TimerStarted event but
// before its TimerFired — the cursor that replays TimerStarted/TimerFired
// must not confuse the intervening SignalReceived event for TimerFired.
func TestWorkerSignalDuringParkedTimerSelect(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithLeaseTTL(time.Minute))

	RegisterWorkflow(r.Worker(), "wf-signal-race", "1", func(ctx Context, _ struct{}) (string, error) {
		sig := WaitSignal(ctx, "approval")
		timeout := Sleep(ctx, time.Hour)
		idx, err := Select(ctx, sig, timeout)
		if err != nil {
			return "", err
		}
		if idx == 1 {
			return "expired", nil
		}
		var decision string
		_ = sig.Get(&decision)
		return decision, nil
	})

	res, err := r.Client().Start(ctx, "wf-signal-race", "1", nil)
	is.NoError(err)

	// First pass: neither WaitSignal nor Sleep is ready; the workflow parks
	// with TimerStarted recorded but not yet fired.
	runOneWorkflowTask(t, r.Worker())
	view, err := r.Manager().Get(ctx, res.WorkflowID)
	is.NoError(err)
	is.Equal(driver.WorkflowRunning, view.State)

	// The signal lands after TimerStarted (seq-wise) but well before the
	// timer's one-hour deadline — the interleaving this test exists to
	// cover.
	delivered, err := r.Client().Signal(ctx, res.WorkflowID, "approval", "approved")
	is.NoError(err)
	is.True(delivered)

	runOneWorkflowTask(t, r.Worker())
	view, err = r.Manager().Get(ctx, res.WorkflowID)
	is.NoError(err)
	is.Equal(driver.WorkflowSucceeded, view.State)
	is.JSONEq(`"approved"`, string(view.Result))
}

func TestWorkerFailsOnUnknownWorkflow(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithLeaseTTL(time.Minute))

	// Register a workflow name so the worker dequeues its kind, but never
	// bind a handler for the version this execution actually names.
	RegisterWorkflow(r.Worker(), "wf-unknown", "1", func(ctx Context, in struct{}) (struct{}, error) {
		return struct{}{}, nil
	})

	res, err := r.Client().Start(ctx, "wf-unknown", "2", nil)
	is.NoError(err)

	runOneWorkflowTask(t, r.Worker())

	view, err := r.Manager().Get(ctx, res.WorkflowID)
	is.NoError(err)
	is.Equal(driver.WorkflowFailed, view.State)
	is.Contains(view.FailureReason, "unknown workflow")
}

func TestWorkerParksOnSleepThenWakes(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithLeaseTTL(time.Minute))

	const naptime = 50 * time.Millisecond
	RegisterWorkflow(r.Worker(), "wf-sleep", "1", func(ctx Context, _ struct{}) (struct{}, error) {
		_, err := Select(ctx, Sleep(ctx, naptime))
		return struct{}{}, err
	})

	res, err := r.Client().Start(ctx, "wf-sleep", "1", nil)
	is.NoError(err)

	runOneWorkflowTask(t, r.Worker())

	view, err := r.Manager().Get(ctx, res.WorkflowID)
	is.NoError(err)
	is.Equal(driver.WorkflowRunning, view.State, "the workflow parks; it has not completed")

	// No job is due yet.
	processed, err := r.Worker().ProcessNext(ctx)
	is.NoError(err)
	is.False(processed)

	// Once the timer's deadline passes, ProcessNext promotes the scheduled
	// follow-up job to pending and completes the replay.
	time.Sleep(naptime + 20*time.Millisecond)
	runOneWorkflowTask(t, r.Worker())

	view, err = r.Manager().Get(ctx, res.WorkflowID)
	is.NoError(err)
	is.Equal(driver.WorkflowSucceeded, view.State)
}
