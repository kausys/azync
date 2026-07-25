package drivertest_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The workflow-as-code tests drive the Fake through the driver.WorkflowStore
// capability, pinning the semantics the workflow runtime depends on: live
// business-key dedupe, monotonic history append and MessageID-deduped signal
// delivery.

func newWACFake(t *testing.T) *drivertest.Fake {
	t.Helper()
	return drivertest.NewFake()
}

func startWorkflow(t *testing.T, f *drivertest.Fake, p driver.WorkflowStartParams) uuid.UUID {
	t.Helper()
	is := require.New(t)
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	inserted, _, err := f.StartWorkflow(context.Background(), p)
	is.NoError(err)
	is.True(inserted)
	return p.ID
}

func TestWorkflowStartAndGet(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := newWACFake(t)
	ctx := context.Background()

	id := startWorkflow(t, f, driver.WorkflowStartParams{
		Name: "wac-start", Version: "1",
		Input: json.RawMessage(`{"n":1}`),
		Meta:  map[string]string{"tenant": "t1"},
	})

	view, err := f.GetWorkflowExecution(ctx, id)
	is.NoError(err)
	is.Equal(driver.WorkflowRunning, view.State)
	is.Equal("wac-start", view.Name)
	is.Equal("1", view.Version)
	is.Equal("default", view.TaskQueue, "an empty task queue defaults")
	is.JSONEq(`{"n":1}`, string(view.Input))
	is.Equal(map[string]string{"tenant": "t1"}, view.Meta)
	is.False(view.CreatedAt.IsZero())
	is.True(view.CompletedAt.IsZero())

	_, err = f.GetWorkflowExecution(ctx, uuid.New())
	is.True(driver.IsNotFound(err))
}

func TestWorkflowStartDuplicateIDErrors(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := newWACFake(t)
	ctx := context.Background()

	id := startWorkflow(t, f, driver.WorkflowStartParams{Name: "wac-dupid"})
	inserted, _, err := f.StartWorkflow(ctx, driver.WorkflowStartParams{ID: id, Name: "wac-dupid-other"})
	is.Error(err)
	is.False(inserted)
}

func TestWorkflowStartBusinessKeyDedupeLiveOnly(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := newWACFake(t)
	ctx := context.Background()

	params := func() driver.WorkflowStartParams {
		return driver.WorkflowStartParams{ID: uuid.New(), Name: "wac-dedupe", BusinessIdempotencyKey: "biz-1"}
	}
	first := startWorkflow(t, f, params())

	inserted, existing, err := f.StartWorkflow(ctx, params())
	is.NoError(err)
	is.False(inserted, "a live execution holds the (name, key)")
	is.Equal(first, existing)

	// A different name does not collide.
	inserted, _, err = f.StartWorkflow(ctx, driver.WorkflowStartParams{
		ID: uuid.New(), Name: "wac-dedupe-other", BusinessIdempotencyKey: "biz-1",
	})
	is.NoError(err)
	is.True(inserted, "dedupe scopes to (name, key)")

	// A terminal execution frees the key.
	is.NoError(f.CompleteWorkflow(ctx, first, nil))
	inserted, _, err = f.StartWorkflow(ctx, params())
	is.NoError(err)
	is.True(inserted, "a terminal workflow frees the business idempotency key")
}

func TestWorkflowAppendHistoryIsMonotonic(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := newWACFake(t)
	ctx := context.Background()

	id := startWorkflow(t, f, driver.WorkflowStartParams{Name: "wac-hist"})

	seq1, err := f.AppendHistory(ctx, id, "WorkflowStarted", nil)
	is.NoError(err)
	is.Equal(int64(1), seq1)
	seq2, err := f.AppendHistory(ctx, id, "OperationScheduled", json.RawMessage(`{"name":"op"}`))
	is.NoError(err)
	is.Equal(int64(2), seq2)

	events, err := f.ListHistory(ctx, id)
	is.NoError(err)
	is.Len(events, 2)
	is.Equal("WorkflowStarted", events[0].Type)
	is.Equal(int64(1), events[0].Seq)
	is.Equal("OperationScheduled", events[1].Type)
	is.Equal(int64(2), events[1].Seq)
	is.JSONEq(`{"name":"op"}`, string(events[1].Payload))

	_, err = f.AppendHistory(ctx, uuid.New(), "X", nil)
	is.True(driver.IsNotFound(err))
	_, err = f.ListHistory(ctx, uuid.New())
	is.True(driver.IsNotFound(err))
}

func TestWorkflowSignalMessageIDDedupe(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := newWACFake(t)
	ctx := context.Background()

	id := startWorkflow(t, f, driver.WorkflowStartParams{Name: "wac-signal"})

	inserted, err := f.SignalWorkflow(ctx, driver.SignalParams{
		WorkflowID: id, Name: "approved", MessageID: "m1", Payload: json.RawMessage(`{"by":"ops"}`),
	})
	is.NoError(err)
	is.True(inserted)

	inserted, err = f.SignalWorkflow(ctx, driver.SignalParams{
		WorkflowID: id, Name: "approved", MessageID: "m1", Payload: json.RawMessage(`{"by":"ops"}`),
	})
	is.NoError(err)
	is.False(inserted, "a repeated MessageID is deduplicated")

	// An empty MessageID never dedupes: every such signal is distinct.
	inserted, err = f.SignalWorkflow(ctx, driver.SignalParams{WorkflowID: id, Name: "approved"})
	is.NoError(err)
	is.True(inserted)
	inserted, err = f.SignalWorkflow(ctx, driver.SignalParams{WorkflowID: id, Name: "approved"})
	is.NoError(err)
	is.True(inserted, "an unset MessageID disables dedupe")

	_, err = f.SignalWorkflow(ctx, driver.SignalParams{WorkflowID: uuid.New(), Name: "x"})
	is.True(driver.IsNotFound(err), "signaling a missing workflow is not-found")
}

func TestWorkflowCompleteFailCancelSuspend(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := newWACFake(t)
	ctx := context.Background()

	completed := startWorkflow(t, f, driver.WorkflowStartParams{Name: "wac-complete"})
	is.NoError(f.CompleteWorkflow(ctx, completed, json.RawMessage(`{"ok":true}`)))
	v, err := f.GetWorkflowExecution(ctx, completed)
	is.NoError(err)
	is.Equal(driver.WorkflowSucceeded, v.State)
	is.JSONEq(`{"ok":true}`, string(v.Result))
	is.False(v.CompletedAt.IsZero())

	failed := startWorkflow(t, f, driver.WorkflowStartParams{Name: "wac-fail"})
	is.NoError(f.FailWorkflow(ctx, failed, "boom"))
	v, err = f.GetWorkflowExecution(ctx, failed)
	is.NoError(err)
	is.Equal(driver.WorkflowFailed, v.State)
	is.Equal("boom", v.FailureReason)

	cancelled := startWorkflow(t, f, driver.WorkflowStartParams{Name: "wac-cancel"})
	is.NoError(f.CancelWorkflowExecution(ctx, cancelled))
	v, err = f.GetWorkflowExecution(ctx, cancelled)
	is.NoError(err)
	is.Equal(driver.WorkflowCancelled, v.State)
	is.True(driver.IsNotFound(f.CancelWorkflowExecution(ctx, cancelled)), "cancelling a terminal workflow is not-found")

	suspended := startWorkflow(t, f, driver.WorkflowStartParams{Name: "wac-suspend"})
	is.NoError(f.SuspendWorkflow(ctx, suspended, "manual review"))
	v, err = f.GetWorkflowExecution(ctx, suspended)
	is.NoError(err)
	is.Equal(driver.WorkflowSuspended, v.State)
	is.Equal("manual review", v.FailureReason)

	is.True(driver.IsNotFound(f.CompleteWorkflow(ctx, uuid.New(), nil)))
	is.True(driver.IsNotFound(f.FailWorkflow(ctx, uuid.New(), "x")))
	is.True(driver.IsNotFound(f.SuspendWorkflow(ctx, uuid.New(), "x")))
}

func TestWorkflowResolveUncertainRequiresUncertainJob(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := newWACFake(t)
	_, _, err := f.ResolveUncertain(context.Background(), uuid.New(), "complete", json.RawMessage(`{}`))
	is.True(driver.IsNotFound(err))
}

func TestWorkflowExecutionCountHelper(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := newWACFake(t)
	is.Zero(f.WorkflowExecutionCount())
	startWorkflow(t, f, driver.WorkflowStartParams{Name: "wac-count"})
	is.Equal(1, f.WorkflowExecutionCount())
}
