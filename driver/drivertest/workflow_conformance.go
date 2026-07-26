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
	t.Run("HistoryOperationResultDedupe", func(t *testing.T) { runWACHistoryOperationResultDedupe(t, ws) })
	t.Run("SignalDedupe", func(t *testing.T) { runWACSignalDedupe(t, ws) })
	t.Run("Complete", func(t *testing.T) { runWACComplete(t, ws) })
	t.Run("OperationUncertain", func(t *testing.T) { runWACOperationUncertain(t, store, ws) })
	t.Run("ResolveUncertainFencing", func(t *testing.T) { runWACResolveUncertainFencing(t, store, ws) })
	t.Run("Vacuum", func(t *testing.T) { runWACVacuum(t, store, ws) })
	t.Run("ListStalledWorkflows", func(t *testing.T) { runWACListStalledWorkflows(t, store, ws) })
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

	// StartWorkflow already records WorkflowStarted at seq 1 atomically (see
	// its doc comment), so this execution's history starts there.
	id := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_hist"})

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

// runWACHistoryOperationResultDedupe proves AppendHistory is idempotent for
// an OperationCompleted/OperationFailed payload carrying an execution_key
// already recorded: retrying the append (as a crash between the real
// AppendHistory call and the operation job's Ack would cause) returns the
// existing record's seq instead of creating a duplicate, which would
// otherwise let a later replay misattribute that stray event's outcome to an
// unrelated ExecuteOperation call.
func runWACHistoryOperationResultDedupe(t *testing.T, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	// StartWorkflow already records WorkflowStarted atomically; this test
	// only needs the execution to exist.
	id := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_hist_dedupe"})

	payload := json.RawMessage(`{"name":"op","version":"1","execution_key":"exec-1","result":"first"}`)
	seq1, err := ws.AppendHistory(ctx, id, "OperationCompleted", payload)
	is.NoError(err)

	// A retried append of the identical execution_key (simulating a crash
	// between the original append and the job's Ack) must not duplicate.
	seq2, err := ws.AppendHistory(ctx, id, "OperationCompleted", payload)
	is.NoError(err)
	is.Equal(seq1, seq2, "a retried append of the same execution_key must return the existing record")

	events, err := ws.ListHistory(ctx, id)
	is.NoError(err)
	var completedCount int
	for _, ev := range events {
		if ev.Type == "OperationCompleted" {
			completedCount++
		}
	}
	is.Equal(1, completedCount, "exactly one OperationCompleted record must exist for the execution key")

	// A different execution_key for the same type is a distinct record.
	seq3, err := ws.AppendHistory(ctx, id, "OperationCompleted",
		json.RawMessage(`{"name":"op","version":"1","execution_key":"exec-2","result":"second"}`))
	is.NoError(err)
	is.NotEqual(seq1, seq3)
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

// runWACResolveUncertainFencing proves ResolveUncertain is fenced to a job
// that is still in StateUncertain: resolving a job twice, or a job that has
// since moved on (retried, re-leased, and now active again), must return
// NotFound rather than silently reapplying — or worse, clobbering a lease
// another worker currently owns.
func runWACResolveUncertainFencing(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	scheduleAndMarkUncertain := func(name, kind string) uuid.UUID {
		id := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: name})
		jobID, err := ws.ScheduleOperation(ctx, driver.ScheduleOperationParams{
			WorkflowID: id, Kind: kind, Payload: json.RawMessage(`{}`), MaxAttempts: 3,
		})
		is.NoError(err)
		leased, err := store.DequeueBatch(ctx, driver.SourceWorkflow, driver.DequeueParams{
			Kind: kind, Limit: 1, Lease: time.Minute, DefaultMaxAttempts: 3, OverrideDefault: true,
		})
		is.NoError(err)
		is.Len(leased, 1)
		is.NoError(ws.MarkUncertain(ctx, leased[0].ID, leased[0].LeaseToken, "ambiguous"))
		return jobID
	}

	// Resolving an unknown job is not-found.
	_, _, err := ws.ResolveUncertain(ctx, uuid.New(), string(driver.UncertainComplete), json.RawMessage(`{}`))
	is.Error(err)
	is.True(driver.IsNotFound(err))

	// A second resolve, after the first already settled the job, must not
	// reapply: the job is succeeded, not uncertain.
	jobID := scheduleAndMarkUncertain("wfc_wac_resolve_twice", "$op:twice@1")
	_, _, err = ws.ResolveUncertain(ctx, jobID, string(driver.UncertainComplete), json.RawMessage(`{"n":1}`))
	is.NoError(err)
	_, _, err = ws.ResolveUncertain(ctx, jobID, string(driver.UncertainComplete), json.RawMessage(`{"n":2}`))
	is.Error(err, "resolving an already-settled job must fail")
	is.True(driver.IsNotFound(err))
	got, err := store.GetJob(ctx, driver.SourceWorkflow, jobID)
	is.NoError(err)
	is.Equal(driver.StateSucceeded, got.State)
	is.JSONEq(`{"n":1}`, string(got.Result), "the second, rejected resolve must not overwrite the first result")

	// A stale resolve arriving after the job was retried and re-leased (the
	// TOCTOU window a load-then-update implementation would clobber) must
	// fail instead of overwriting the live lease.
	jobID = scheduleAndMarkUncertain("wfc_wac_resolve_stale", "$op:stale@1")
	_, _, err = ws.ResolveUncertain(ctx, jobID, string(driver.UncertainRetry), nil)
	is.NoError(err)
	leased, err := store.DequeueBatch(ctx, driver.SourceWorkflow, driver.DequeueParams{
		Kind: "$op:stale@1", Limit: 1, Lease: time.Minute, DefaultMaxAttempts: 3, OverrideDefault: true,
	})
	is.NoError(err)
	is.Len(leased, 1, "the retried job must be leasable again")

	_, _, err = ws.ResolveUncertain(ctx, jobID, string(driver.UncertainComplete), json.RawMessage(`{"stale":true}`))
	is.Error(err, "a stale resolve against a re-leased, active job must fail")
	is.True(driver.IsNotFound(err))

	got, err = store.GetJob(ctx, driver.SourceWorkflow, jobID)
	is.NoError(err)
	is.Equal(driver.StateActive, got.State, "the live lease must survive the stale resolve untouched")
	is.Equal(leased[0].LeaseToken, got.LeaseToken)
}

// ---- VacuumWorkflows + VacuumCompleted exemption ----------------------------

func runWACVacuum(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	// StartWorkflow already records WorkflowStarted and schedules the first
	// $wf:wfc_wac_vacuum task atomically.
	id := startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_vacuum"})
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

// ---- ListStalledWorkflows ---------------------------------------------------

// runWACListStalledWorkflows proves ListStalledWorkflows finds a running
// execution whose only task was removed out from under it (simulating an
// operator deleting a job by hand, or any other way an execution ends up
// stranded despite StartWorkflow's atomic guarantee) and ignores healthy
// executions: one with a live task, and one not yet past olderThan.
func runWACListStalledWorkflows(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	// Healthy: StartWorkflow's own atomic task is still live.
	startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_stall_healthy"})

	// Stalled: its only task is deleted out from under it.
	startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_stall_gone"})
	jobs, _, err := store.ListJobs(ctx, driver.SourceWorkflow,
		driver.JobFilter{Kind: "$wf:wfc_wac_stall_gone"}, 0, 10)
	is.NoError(err)
	is.Len(jobs, 1, "StartWorkflow must have scheduled exactly one task")
	is.NoError(store.DeleteJob(ctx, driver.SourceWorkflow, jobs[0].ID, jobs[0].State))

	// Not yet stalled long enough: same shape as the stalled one, but this
	// test's olderThan window excludes it.
	startWAC(ctx, t, ws, driver.WorkflowStartParams{Name: "wfc_wac_stall_recent"})
	jobs, _, err = store.ListJobs(ctx, driver.SourceWorkflow,
		driver.JobFilter{Kind: "$wf:wfc_wac_stall_recent"}, 0, 10)
	is.NoError(err)
	is.Len(jobs, 1)
	is.NoError(store.DeleteJob(ctx, driver.SourceWorkflow, jobs[0].ID, jobs[0].State))

	stalledList, err := ws.ListStalledWorkflows(ctx, 0, 100)
	is.NoError(err)
	names := make(map[string]bool, len(stalledList))
	for _, sw := range stalledList {
		names[sw.Name] = true
	}
	is.True(names["wfc_wac_stall_gone"], "an execution with no live task must be reported stalled")
	is.False(names["wfc_wac_stall_healthy"], "an execution with a live task must not be reported stalled")

	// A very long olderThan window excludes everything (nothing is that old).
	none, err := ws.ListStalledWorkflows(ctx, 24*time.Hour, 100)
	is.NoError(err)
	for _, sw := range none {
		is.NotEqual("wfc_wac_stall_gone", sw.Name, "olderThan must gate out a too-recent stall")
	}
}
