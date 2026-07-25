package drivertest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// RunWorkflowConformance exercises the observable [driver.WorkflowStore]
// contract against the Store returned by newStore, skipping cleanly when the
// store does not implement the capability. newStore is called once; every
// subtest stays independent by using unique workflow names, so a backend
// need not reset between subtests.
func RunWorkflowConformance(t *testing.T, newStore func(t *testing.T) driver.Store) {
	t.Helper()
	store := newStore(t)
	ws, ok := store.(driver.WorkflowStore)
	if !ok {
		t.Skipf("store %T does not implement driver.WorkflowStore; skipping the workflow-as-code conformance suite", store)
	}

	t.Run("StartAndGet", func(t *testing.T) { runWACStartAndGet(t, ws) })
	t.Run("StartDedupe", func(t *testing.T) { runWACStartDedupe(t, ws) })
	t.Run("History", func(t *testing.T) { runWACHistory(t, ws) })
	t.Run("SignalDedupe", func(t *testing.T) { runWACSignalDedupe(t, ws) })
	t.Run("Complete", func(t *testing.T) { runWACComplete(t, ws) })
	t.Run("OperationUncertain", func(t *testing.T) { runWACOperationUncertain(t, store, ws) })
	t.Run("Vacuum", func(t *testing.T) { runWACVacuum(t, store, ws) })
}

// ---- shared helpers -------------------------------------------------------

// startWAC starts one workflow-as-code execution, filling in a fresh ID when
// unset, and requires that it was newly inserted.
func startWAC(ctx context.Context, t *testing.T, ws driver.WorkflowStore, p driver.WorkflowStartParams) uuid.UUID {
	t.Helper()
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	inserted, _, err := ws.StartWorkflow(ctx, p)
	require.NoError(t, err)
	require.True(t, inserted)
	return p.ID
}

// ---- StartWorkflow / GetWorkflowExecution ---------------------------------

func runWACStartAndGet(t *testing.T, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := startWAC(ctx, t, ws, driver.WorkflowStartParams{
		Name: "wfc_wac_start", Version: "1",
		Input: json.RawMessage(`{"n":1}`),
		Meta:  map[string]string{"tenant": "t1"},
	})

	view, err := ws.GetWorkflowExecution(ctx, id)
	is.NoError(err)
	is.Equal(driver.WorkflowRunning, view.State)
	is.Equal("wfc_wac_start", view.Name)
	is.Equal("1", view.Version)
	is.JSONEq(`{"n":1}`, string(view.Input))
	is.Equal(map[string]string{"tenant": "t1"}, view.Meta)
	is.False(view.CreatedAt.IsZero())
	is.True(view.CompletedAt.IsZero())

	_, err = ws.GetWorkflowExecution(ctx, uuid.New())
	is.True(driver.IsNotFound(err), "a missing execution is a typed not-found")
}

// ---- StartWorkflow dedupe --------------------------------------------------

func runWACStartDedupe(t *testing.T, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	params := func() driver.WorkflowStartParams {
		return driver.WorkflowStartParams{ID: uuid.New(), Name: "wfc_wac_dedupe", BusinessIdempotencyKey: "biz-1"}
	}
	first := startWAC(ctx, t, ws, params())

	inserted, existing, err := ws.StartWorkflow(ctx, params())
	is.NoError(err)
	is.False(inserted, "a live execution holds the (name, key)")
	is.Equal(first, existing, "the live execution's id is returned")

	// Terminal frees the key.
	is.NoError(ws.CompleteWorkflow(ctx, first, nil))
	inserted, _, err = ws.StartWorkflow(ctx, params())
	is.NoError(err)
	is.True(inserted, "a terminal workflow frees the business idempotency key")
}

// ---- AppendHistory / ListHistory -------------------------------------------

func runWACHistory(t *testing.T, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_hist"})

	seq1, err := ws.AppendHistory(ctx, id, "WorkflowStarted", nil)
	is.NoError(err)
	is.Equal(int64(1), seq1)
	seq2, err := ws.AppendHistory(ctx, id, "OperationScheduled", json.RawMessage(`{"name":"op"}`))
	is.NoError(err)
	is.Equal(int64(2), seq2)
	seq3, err := ws.AppendHistory(ctx, id, "OperationCompleted", json.RawMessage(`{"name":"op","ok":true}`))
	is.NoError(err)
	is.Equal(int64(3), seq3)

	events, err := ws.ListHistory(ctx, id)
	is.NoError(err)
	is.Len(events, 3)
	is.Equal([]int64{1, 2, 3}, []int64{events[0].Seq, events[1].Seq, events[2].Seq}, "history is returned in sequence order")
	is.Equal("WorkflowStarted", events[0].Type)
	is.Equal("OperationScheduled", events[1].Type)
	is.Equal("OperationCompleted", events[2].Type)
	is.JSONEq(`{"name":"op","ok":true}`, string(events[2].Payload))

	_, err = ws.AppendHistory(ctx, uuid.New(), "X", nil)
	is.True(driver.IsNotFound(err), "appending to a missing execution is not-found")
	_, err = ws.ListHistory(ctx, uuid.New())
	is.True(driver.IsNotFound(err))
}

// ---- SignalWorkflow dedupe --------------------------------------------------

func runWACSignalDedupe(t *testing.T, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_signal"})

	inserted, err := ws.SignalWorkflow(ctx, driver.SignalParams{
		WorkflowID: id, Name: "approved", MessageID: "m1", Payload: json.RawMessage(`{"by":"ops"}`),
	})
	is.NoError(err)
	is.True(inserted)

	inserted, err = ws.SignalWorkflow(ctx, driver.SignalParams{
		WorkflowID: id, Name: "approved", MessageID: "m1", Payload: json.RawMessage(`{"by":"ops"}`),
	})
	is.NoError(err)
	is.False(inserted, "a repeated MessageID is deduplicated")

	inserted, err = ws.SignalWorkflow(ctx, driver.SignalParams{
		WorkflowID: id, Name: "approved", MessageID: "m2", Payload: json.RawMessage(`{"by":"ops"}`),
	})
	is.NoError(err)
	is.True(inserted, "a distinct MessageID is a distinct message")

	_, err = ws.SignalWorkflow(ctx, driver.SignalParams{WorkflowID: uuid.New(), Name: "x"})
	is.True(driver.IsNotFound(err), "signaling a missing workflow is not-found")
}

// ---- CompleteWorkflow / FailWorkflow / CancelWorkflowExecution / SuspendWorkflow --

func runWACComplete(t *testing.T, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	completed := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_complete"})
	is.NoError(ws.CompleteWorkflow(ctx, completed, json.RawMessage(`{"ok":true}`)))
	v, err := ws.GetWorkflowExecution(ctx, completed)
	is.NoError(err)
	is.Equal(driver.WorkflowSucceeded, v.State)
	is.JSONEq(`{"ok":true}`, string(v.Result))
	is.False(v.CompletedAt.IsZero())

	failed := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_fail"})
	is.NoError(ws.FailWorkflow(ctx, failed, "boom"))
	v, err = ws.GetWorkflowExecution(ctx, failed)
	is.NoError(err)
	is.Equal(driver.WorkflowFailed, v.State)
	is.Equal("boom", v.FailureReason)

	cancelled := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_cancel"})
	is.NoError(ws.CancelWorkflowExecution(ctx, cancelled))
	v, err = ws.GetWorkflowExecution(ctx, cancelled)
	is.NoError(err)
	is.Equal(driver.WorkflowCancelled, v.State)
	is.True(driver.IsNotFound(ws.CancelWorkflowExecution(ctx, cancelled)), "cancelling a terminal workflow is not-found")

	suspended := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_suspend"})
	is.NoError(ws.SuspendWorkflow(ctx, suspended, "manual review"))
	v, err = ws.GetWorkflowExecution(ctx, suspended)
	is.NoError(err)
	is.Equal(driver.WorkflowSuspended, v.State)
	is.Equal("manual review", v.FailureReason)

	is.True(driver.IsNotFound(ws.CompleteWorkflow(ctx, uuid.New(), nil)))
	is.True(driver.IsNotFound(ws.FailWorkflow(ctx, uuid.New(), "x")))
	is.True(driver.IsNotFound(ws.SuspendWorkflow(ctx, uuid.New(), "x")))

	resumed := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_resume"})
	is.NoError(ws.SuspendWorkflow(ctx, resumed, "hold"))
	is.NoError(ws.ResumeWorkflow(ctx, resumed))
	v, err = ws.GetWorkflowExecution(ctx, resumed)
	is.NoError(err)
	is.Equal(driver.WorkflowRunning, v.State)
}

// ---- ScheduleOperation / MarkUncertain / ResolveUncertain --------------------

func runWACOperationUncertain(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_op_uncertain"})
	jobID, err := ws.ScheduleOperation(ctx, driver.ScheduleOperationParams{
		WorkflowID:   id,
		Kind:         "$op:probe@1",
		Payload:      json.RawMessage(`{"name":"probe","version":"1"}`),
		ExecutionKey: id.String() + ":1",
		Meta: map[string]string{
			"execution_key": id.String() + ":1",
			"workflow_name": "wfc_wac_op_uncertain",
			"op_name":       "probe",
			"op_version":    "1",
		},
		MaxAttempts: 3,
	})
	is.NoError(err)
	is.NotEqual(uuid.Nil, jobID)

	// Dedup by execution key.
	again, err := ws.ScheduleOperation(ctx, driver.ScheduleOperationParams{
		WorkflowID: id, Kind: "$op:probe@1", Payload: json.RawMessage(`{}`),
		ExecutionKey: id.String() + ":1",
	})
	is.NoError(err)
	is.Equal(jobID, again)

	leased, err := store.DequeueBatch(ctx, driver.SourceWorkflow, driver.DequeueParams{
		Kind: "$op:probe@1", Limit: 1, Lease: time.Minute, DefaultMaxAttempts: 3, OverrideDefault: true,
	})
	is.NoError(err)
	is.Len(leased, 1)
	is.Equal(jobID, leased[0].ID)

	is.NoError(ws.MarkUncertain(ctx, leased[0].ID, leased[0].LeaseToken, "ambiguous"))
	view, err := ws.GetWorkflowExecution(ctx, id)
	is.NoError(err)
	is.Equal(driver.WorkflowSuspended, view.State)

	got, err := store.GetJob(ctx, driver.SourceWorkflow, jobID)
	is.NoError(err)
	is.Equal(driver.StateUncertain, got.State)

	wfID, wfName, err := ws.ResolveUncertain(ctx, jobID, string(driver.UncertainComplete), json.RawMessage(`{"ok":true}`))
	is.NoError(err)
	is.Equal(id, wfID)
	is.Equal("wfc_wac_op_uncertain", wfName)

	view, err = ws.GetWorkflowExecution(ctx, id)
	is.NoError(err)
	is.Equal(driver.WorkflowRunning, view.State)

	got, err = store.GetJob(ctx, driver.SourceWorkflow, jobID)
	is.NoError(err)
	is.Equal(driver.StateSucceeded, got.State)
}

// ---- VacuumWorkflows + VacuumCompleted exemption ----------------------------

func runWACVacuum(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_vacuum"})
	_, err := ws.AppendHistory(ctx, id, "WorkflowStarted", json.RawMessage(`{}`))
	is.NoError(err)
	is.NoError(ws.ScheduleTask(ctx, id, "$wf:wfc_wac_vacuum", time.Time{}))
	is.NoError(ws.CompleteWorkflow(ctx, id, json.RawMessage(`{"ok":true}`)))

	removed, err := ws.VacuumWorkflows(ctx, 0)
	is.NoError(err)
	is.Zero(removed, "retention 0 retains forever")

	_, err = ws.GetWorkflowExecution(ctx, id)
	is.NoError(err)

	// Succeeded workflow jobs must not be swept by VacuumCompleted.
	time.Sleep(20 * time.Millisecond)
	jobs, err := store.DequeueBatch(ctx, driver.SourceWorkflow, driver.DequeueParams{
		Kind: "$wf:wfc_wac_vacuum", Limit: 1, Lease: time.Minute,
	})
	is.NoError(err)
	if len(jobs) == 1 {
		is.NoError(store.Ack(ctx, jobs[0].ID, jobs[0].LeaseToken))
	}
	swept, err := store.VacuumCompleted(ctx, driver.SourceWorkflow, time.Millisecond)
	is.NoError(err)
	is.Zero(swept, "workflow-as-code jobs are exempt from VacuumCompleted")

	removed, err = ws.VacuumWorkflows(ctx, time.Millisecond)
	is.NoError(err)
	is.GreaterOrEqual(removed, int64(1))

	_, err = ws.GetWorkflowExecution(ctx, id)
	is.True(driver.IsNotFound(err), "terminal execution removed by VacuumWorkflows")

	hist, err := ws.ListHistory(ctx, id)
	is.True(driver.IsNotFound(err) || len(hist) == 0)
}
