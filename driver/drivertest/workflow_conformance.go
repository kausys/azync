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
// subtest shares that Store and stays independent by using unique workflow
// names and kinds, so a backend need not reset between subtests.
//
// The scheduler methods are set-based across the whole store, so subtests
// assert on the states of their own workflow's tasks (through WorkflowTasks
// and GetWorkflow), never on the global counts those methods return.
func RunWorkflowConformance(t *testing.T, newStore func(t *testing.T) driver.Store) {
	t.Helper()
	store := newStore(t)
	ws, ok := store.(driver.WorkflowStore)
	if !ok {
		t.Skipf("store %T does not implement driver.WorkflowStore; skipping the workflow conformance suite", store)
	}

	t.Run("Create", func(t *testing.T) { runWorkflowCreate(t, store, ws) })
	t.Run("Dedupe", func(t *testing.T) { runWorkflowDedupe(t, store, ws) })
	t.Run("PromotionCascade", func(t *testing.T) { runWorkflowPromotionCascade(t, store, ws) })
	t.Run("Sleep", func(t *testing.T) { runWorkflowSleep(t, store, ws) })
	t.Run("Signal", func(t *testing.T) { runWorkflowSignal(t, store, ws) })
	t.Run("FailurePolicyCancel", func(t *testing.T) { runWorkflowFailureCancel(t, store, ws) })
	t.Run("FailurePolicySuspendAndRetry", func(t *testing.T) { runWorkflowSuspendRetry(t, store, ws) })
	t.Run("FailurePolicyMixedIgnoreDeadDeps", func(t *testing.T) { runWorkflowMixedIgnoreDeadDeps(t, store, ws) })
	t.Run("FailurePolicyFullyIgnoredCompletesFailed", func(t *testing.T) { runWorkflowFullyIgnoredDeadDeps(t, store, ws) })
	t.Run("FailurePolicyDeadLeafTriggers", func(t *testing.T) { runWorkflowDeadLeafTriggers(t, store, ws) })
	t.Run("CompleteWorkflows", func(t *testing.T) { runWorkflowComplete(t, store, ws) })
	t.Run("CancelWorkflow", func(t *testing.T) { runWorkflowCancel(t, store, ws) })
	t.Run("CompensateWorkflowManual", func(t *testing.T) { runWorkflowCompensateManual(t, store, ws) })
	t.Run("RetryDuringCompensating", func(t *testing.T) { runWorkflowRetryDuringCompensating(t, store, ws) })
	t.Run("TaskResults", func(t *testing.T) { runWorkflowTaskResults(t, store, ws) })
	t.Run("AckTaskResultFencing", func(t *testing.T) { runWorkflowAckFencing(t, store, ws) })
	t.Run("InternalKindsStayInternal", func(t *testing.T) { runWorkflowInternalKinds(t, store, ws) })
	t.Run("Vacuum", func(t *testing.T) { runWorkflowVacuum(t, store, ws) })
}

// ---- shared helpers -------------------------------------------------------

func wfTask(key, kind string) driver.WorkflowTask {
	return driver.WorkflowTask{Key: key, Kind: kind, Payload: json.RawMessage(`{}`), MaxAttempts: 3}
}

func createWF(ctx context.Context, t *testing.T, ws driver.WorkflowStore, p driver.WorkflowParams) uuid.UUID {
	t.Helper()
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	inserted, _, err := ws.CreateWorkflow(ctx, p)
	require.NoError(t, err)
	require.True(t, inserted)
	return p.ID
}

func wfTaskByKey(ctx context.Context, t *testing.T, ws driver.WorkflowStore, id uuid.UUID, key string) driver.Job {
	t.Helper()
	tasks, err := ws.WorkflowTasks(ctx, id)
	require.NoError(t, err)
	for _, j := range tasks {
		if j.TaskKey == key {
			return j
		}
	}
	t.Fatalf("task %q not found in workflow %s", key, id)
	return driver.Job{}
}

func wfView(ctx context.Context, t *testing.T, ws driver.WorkflowStore, id uuid.UUID) driver.WorkflowView {
	t.Helper()
	w, err := ws.GetWorkflow(ctx, id)
	require.NoError(t, err)
	return *w
}

// finishWFTask leases the one due pending workflow task of the kind and acks
// it with the result.
func finishWFTask(ctx context.Context, t *testing.T, store driver.Store, ws driver.WorkflowStore, kind string, result json.RawMessage) driver.Job {
	t.Helper()
	leased := dequeueN(ctx, t, store, driver.SourceWorkflow, kind, 1, time.Minute)
	require.Len(t, leased, 1, "expected one due pending task of kind %q", kind)
	require.NoError(t, ws.AckTaskResult(ctx, leased[0].ID, leased[0].LeaseToken, result))
	return leased[0]
}

// killWFTask leases the one due pending workflow task of the kind and
// dead-letters it.
func killWFTask(ctx context.Context, t *testing.T, store driver.Store, kind string) driver.Job {
	t.Helper()
	leased := dequeueN(ctx, t, store, driver.SourceWorkflow, kind, 1, time.Minute)
	require.Len(t, leased, 1, "expected one due pending task of kind %q", kind)
	require.NoError(t, store.Dead(ctx, leased[0].ID, leased[0].LeaseToken, "boom"))
	return leased[0]
}

// applyPolicyFor runs ApplyFailurePolicy and returns this workflow's failure
// report; the pass is set-based, so other workflows may appear in the result.
func applyPolicyFor(ctx context.Context, t *testing.T, ws driver.WorkflowStore, id uuid.UUID) driver.WorkflowFailure {
	t.Helper()
	failures, err := ws.ApplyFailurePolicy(ctx)
	require.NoError(t, err)
	for _, fl := range failures {
		if fl.WorkflowID == id {
			return fl
		}
	}
	t.Fatalf("workflow %s not present in the ApplyFailurePolicy report", id)
	return driver.WorkflowFailure{}
}

// completeWorkflows runs one CompleteWorkflows pass, ignoring the global count.
func completeWorkflows(ctx context.Context, t *testing.T, ws driver.WorkflowStore) {
	t.Helper()
	_, err := ws.CompleteWorkflows(ctx)
	require.NoError(t, err)
}

// promoteUnblocked runs one PromoteUnblocked pass, ignoring the global count.
func promoteUnblocked(ctx context.Context, t *testing.T, ws driver.WorkflowStore) {
	t.Helper()
	_, err := ws.PromoteUnblocked(ctx)
	require.NoError(t, err)
}

// ---- Create ---------------------------------------------------------------

func runWorkflowCreate(t *testing.T, _ driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name: "wfc_create", OnFailure: driver.OnFailureCancel,
		Meta: map[string]string{"tenant": "t1"},
		Tasks: []driver.WorkflowTask{
			wfTask("a", "wfc_create_a"),
			wfTask("b", "wfc_create_b"),
			{Key: "s", Kind: driver.KindSleep, SleepFor: time.Hour},
			{Key: "g", Kind: driver.KindSignal, SignalName: "go"},
		},
		Deps: []driver.WorkflowDep{{TaskKey: "b", DependsOnKey: "a"}},
	})

	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "a").State, "a dependency-free task is pending")
	is.Equal(driver.StateBlocked, wfTaskByKey(ctx, t, ws, id, "b").State, "a task with deps is blocked")
	s := wfTaskByKey(ctx, t, ws, id, "s")
	is.Equal(driver.StateScheduled, s.State, "a root $sleep is scheduled")
	is.True(s.RunAt.After(time.Now().Add(30*time.Minute)), "the sleep timer reflects SleepFor on the backend clock")
	is.Equal(driver.StateWaiting, wfTaskByKey(ctx, t, ws, id, "g").State, "a root $signal waits")

	w := wfView(ctx, t, ws, id)
	is.Equal(driver.WorkflowRunning, w.State)
	is.Equal(driver.OnFailureCancel, w.OnFailure)
	is.Equal(map[string]string{"tenant": "t1"}, w.Meta)
	is.False(w.CreatedAt.IsZero())
	is.True(w.CompletedAt.IsZero())

	a := wfTaskByKey(ctx, t, ws, id, "a")
	is.Equal(driver.SourceWorkflow, a.Source)
	is.Equal(id, a.WorkflowID)
	is.Equal(map[string]string{"tenant": "t1"}, a.Meta, "workflow meta propagates onto task jobs")

	_, err := ws.GetWorkflow(ctx, uuid.New())
	is.True(driver.IsNotFound(err), "a missing workflow is a typed not-found")
	_, err = ws.WorkflowTasks(ctx, uuid.New())
	is.True(driver.IsNotFound(err))

	// The admin list finds it, newest-first, with a correct total.
	views, total, err := ws.ListWorkflows(ctx, driver.WorkflowFilter{Name: "wfc_create"}, 0, 10)
	is.NoError(err)
	is.Equal(int64(1), total)
	is.Len(views, 1)
	is.Equal(id, views[0].ID)
}

// ---- Dedupe ---------------------------------------------------------------

func runWorkflowDedupe(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	params := func() driver.WorkflowParams {
		return driver.WorkflowParams{
			ID: uuid.New(), Name: "wfc_dedupe", IdempotencyKey: "k1",
			Tasks: []driver.WorkflowTask{wfTask("a", "wfc_dedupe_a")},
		}
	}
	first := createWF(ctx, t, ws, params())

	inserted, existing, err := ws.CreateWorkflow(ctx, params())
	is.NoError(err)
	is.False(inserted, "a live execution holds the (name, key)")
	is.Equal(first, existing, "the live execution's id is returned")

	// Terminal frees the key.
	finishWFTask(ctx, t, store, ws, "wfc_dedupe_a", nil)
	completeWorkflows(ctx, t, ws)
	is.Equal(driver.WorkflowSucceeded, wfView(ctx, t, ws, first).State)

	inserted, _, err = ws.CreateWorkflow(ctx, params())
	is.NoError(err)
	is.True(inserted, "a terminal workflow frees the idempotency key")
}

// ---- Promotion cascade ----------------------------------------------------

func runWorkflowPromotionCascade(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name: "wfc_cascade",
		Tasks: []driver.WorkflowTask{
			wfTask("a", "wfc_cas_a"), wfTask("b", "wfc_cas_b"),
			wfTask("c", "wfc_cas_c"), wfTask("d", "wfc_cas_d"),
		},
		Deps: []driver.WorkflowDep{
			{TaskKey: "b", DependsOnKey: "a"},
			{TaskKey: "c", DependsOnKey: "a"},
			{TaskKey: "d", DependsOnKey: "b"},
			{TaskKey: "d", DependsOnKey: "c"},
		},
	})

	finishWFTask(ctx, t, store, ws, "wfc_cas_a", nil)
	promoteUnblocked(ctx, t, ws)
	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "b").State, "the fan-out promotes b")
	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "c").State, "the fan-out promotes c")
	is.Equal(driver.StateBlocked, wfTaskByKey(ctx, t, ws, id, "d").State)

	finishWFTask(ctx, t, store, ws, "wfc_cas_b", nil)
	promoteUnblocked(ctx, t, ws)
	is.Equal(driver.StateBlocked, wfTaskByKey(ctx, t, ws, id, "d").State, "the fan-in still waits for c")

	finishWFTask(ctx, t, store, ws, "wfc_cas_c", nil)
	promoteUnblocked(ctx, t, ws)
	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "d").State, "the fan-in promotes once every dep succeeded")
}

// ---- Sleep ----------------------------------------------------------------

func runWorkflowSleep(t *testing.T, _ driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name: "wfc_sleep",
		Tasks: []driver.WorkflowTask{
			{Key: "short", Kind: driver.KindSleep, SleepFor: reapLease},
			{Key: "long", Kind: driver.KindSleep, SleepFor: time.Hour, SignalName: "hurry"},
		},
	})

	// The long sleep is not due; the short one becomes due after a wait.
	time.Sleep(reapWait)
	_, err := ws.CompleteDueSleeps(ctx)
	is.NoError(err)
	short := wfTaskByKey(ctx, t, ws, id, "short")
	is.Equal(driver.StateSucceeded, short.State, "a due sleep completes without any handler")
	is.False(short.CompletedAt.IsZero())
	is.Equal(driver.StateScheduled, wfTaskByKey(ctx, t, ws, id, "long").State, "an unexpired sleep stays scheduled")

	// A signal wakes the named sleep early.
	matched, err := ws.Signal(ctx, id, "hurry", nil)
	is.NoError(err)
	is.Equal(int64(1), matched)
	_, err = ws.CompleteDueSleeps(ctx)
	is.NoError(err)
	is.Equal(driver.StateSucceeded, wfTaskByKey(ctx, t, ws, id, "long").State, "the woken sleep completes at once")
}

// ---- Signal ---------------------------------------------------------------

func runWorkflowSignal(t *testing.T, _ driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name:  "wfc_signal",
		Tasks: []driver.WorkflowTask{{Key: "g", Kind: driver.KindSignal, SignalName: "approved"}},
	})

	matched, err := ws.Signal(ctx, id, "other", json.RawMessage(`{}`))
	is.NoError(err)
	is.Zero(matched, "an unmatched signal name touches nothing")

	payload := json.RawMessage(`{"by":"ops"}`)
	matched, err = ws.Signal(ctx, id, "approved", payload)
	is.NoError(err)
	is.Equal(int64(1), matched)
	g := wfTaskByKey(ctx, t, ws, id, "g")
	is.Equal(driver.StateSucceeded, g.State)
	is.JSONEq(string(payload), string(g.Result), "the signal payload is the task's result")

	matched, err = ws.Signal(ctx, id, "approved", payload)
	is.NoError(err)
	is.Zero(matched, "a consumed signal has nothing left to match")

	results, err := ws.TaskResults(ctx, id, []string{"g"})
	is.NoError(err)
	is.JSONEq(string(payload), string(results["g"]), "the result is visible through TaskResults")
}

// ---- Failure policy: cancel -----------------------------------------------

func runWorkflowFailureCancel(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	a := wfTask("a", "wfc_fpc_a")
	a.CompensationKind = "wfc_fpc_undo_a"
	a.CompensationPayload = json.RawMessage(`{"undo":"a"}`)
	b := wfTask("b", "wfc_fpc_b")
	b.CompensationKind = "wfc_fpc_undo_b"
	b.CompensationPayload = json.RawMessage(`{"undo":"b"}`)
	c := wfTask("c", "wfc_fpc_c")
	c.MaxAttempts = 1
	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name: "wfc_fpc", OnFailure: driver.OnFailureCancel,
		Tasks: []driver.WorkflowTask{a, b, c, wfTask("d", "wfc_fpc_d")},
		Deps:  []driver.WorkflowDep{{TaskKey: "d", DependsOnKey: "c"}},
	})

	// a completes before b so the compensation order is provable.
	finishWFTask(ctx, t, store, ws, "wfc_fpc_a", nil)
	time.Sleep(tick)
	finishWFTask(ctx, t, store, ws, "wfc_fpc_b", nil)
	killWFTask(ctx, t, store, "wfc_fpc_c")

	failure := applyPolicyFor(ctx, t, ws, id)
	is.Equal(driver.OnFailureCancel, failure.Policy)
	is.Equal([]string{"c"}, failure.DeadTasks)

	w := wfView(ctx, t, ws, id)
	is.Equal(driver.WorkflowCompensating, w.State)
	is.Contains(w.FailureReason, "c", "the dead task is recorded")
	is.Equal(driver.StateCancelled, wfTaskByKey(ctx, t, ws, id, "d").State, "the blocked dependent is cancelled")

	// Reverse completion order: b finished last, so comp:b runs first.
	compB := wfTaskByKey(ctx, t, ws, id, "comp:b")
	is.Equal(driver.StatePending, compB.State)
	is.Equal("wfc_fpc_undo_b", compB.Kind)
	is.JSONEq(`{"undo":"b"}`, string(compB.Payload), "the compensation carries its declared payload")
	compA := wfTaskByKey(ctx, t, ws, id, "comp:a")
	is.Equal(driver.StateBlocked, compA.State, "the older compensation waits for the newer one")
	is.Equal("wfc_fpc_undo_a", compA.Kind)

	// Run the chain and settle the workflow failed.
	finishWFTask(ctx, t, store, ws, "wfc_fpc_undo_b", nil)
	promoteUnblocked(ctx, t, ws)
	finishWFTask(ctx, t, store, ws, "wfc_fpc_undo_a", nil)
	completeWorkflows(ctx, t, ws)
	w = wfView(ctx, t, ws, id)
	is.Equal(driver.WorkflowFailed, w.State)
	is.False(w.CompletedAt.IsZero())
}

// ---- Failure policy: suspend, then retry ----------------------------------

func runWorkflowSuspendRetry(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	a := wfTask("a", "wfc_fps_a")
	a.MaxAttempts = 1
	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name: "wfc_fps", OnFailure: driver.OnFailureSuspend,
		Tasks: []driver.WorkflowTask{a, wfTask("b", "wfc_fps_b")},
	})

	killWFTask(ctx, t, store, "wfc_fps_a")
	failure := applyPolicyFor(ctx, t, ws, id)
	is.Equal(driver.OnFailureSuspend, failure.Policy)
	is.Equal([]string{"a"}, failure.DeadTasks)

	w := wfView(ctx, t, ws, id)
	is.Equal(driver.WorkflowSuspended, w.State)
	is.Contains(w.FailureReason, "a")
	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "b").State, "suspend leaves the tasks untouched")

	is.NoError(ws.RetryWorkflow(ctx, id))
	is.Equal(driver.WorkflowRunning, wfView(ctx, t, ws, id).State)
	retried := wfTaskByKey(ctx, t, ws, id, "a")
	is.Equal(driver.StatePending, retried.State)
	is.Zero(retried.Attempt, "retry grants a fresh budget")
	is.Zero(retried.ReapCount)

	is.True(driver.IsNotFound(ws.RetryWorkflow(ctx, uuid.New())), "retrying a missing workflow is not-found")
}

// ---- Failure policy: IgnoreDeadDeps exemption -----------------------------

// runWorkflowMixedIgnoreDeadDeps pins that a dead task keeps triggering the
// policy as long as one of its dependents does not tolerate dead deps: the
// exemption requires every dependent to opt in.
func runWorkflowMixedIgnoreDeadDeps(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	dead := wfTask("dead", "wfc_mix_dead")
	dead.MaxAttempts = 1
	tol := wfTask("tol", "wfc_mix_tol")
	tol.IgnoreDeadDeps = true
	strict := wfTask("strict", "wfc_mix_strict") // does not tolerate dead deps
	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name: "wfc_mix", OnFailure: driver.OnFailureSuspend,
		Tasks: []driver.WorkflowTask{dead, tol, strict},
		Deps: []driver.WorkflowDep{
			{TaskKey: "tol", DependsOnKey: "dead"},
			{TaskKey: "strict", DependsOnKey: "dead"},
		},
	})

	killWFTask(ctx, t, store, "wfc_mix_dead")

	// One non-tolerant dependent is enough to trigger the policy.
	failure := applyPolicyFor(ctx, t, ws, id)
	is.Equal(driver.OnFailureSuspend, failure.Policy)
	is.Equal([]string{"dead"}, failure.DeadTasks)
	w := wfView(ctx, t, ws, id)
	is.Equal(driver.WorkflowSuspended, w.State)
	is.Contains(w.FailureReason, "dead")
}

// runWorkflowFullyIgnoredDeadDeps pins that a dead task every dependent
// tolerates does not trigger the policy — the tolerant branch runs to
// completion — yet the finished workflow settles failed, not succeeded.
func runWorkflowFullyIgnoredDeadDeps(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	dead := wfTask("dead", "wfc_all_dead")
	dead.MaxAttempts = 1
	tol := wfTask("tol", "wfc_all_tol")
	tol.IgnoreDeadDeps = true
	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name: "wfc_all", OnFailure: driver.OnFailureCancel,
		Tasks: []driver.WorkflowTask{dead, tol},
		Deps:  []driver.WorkflowDep{{TaskKey: "tol", DependsOnKey: "dead"}},
	})

	killWFTask(ctx, t, store, "wfc_all_dead")

	// Every dependent tolerates the death, so the policy leaves it running.
	failures, err := ws.ApplyFailurePolicy(ctx)
	is.NoError(err)
	for _, fl := range failures {
		is.NotEqual(id, fl.WorkflowID, "a fully-tolerated dead task must not trigger the policy")
	}
	is.Equal(driver.WorkflowRunning, wfView(ctx, t, ws, id).State)

	// The tolerant branch runs to completion.
	promoteUnblocked(ctx, t, ws)
	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "tol").State, "the tolerant dependent is promoted")
	finishWFTask(ctx, t, store, ws, "wfc_all_tol", nil)

	// Every task is terminal with one dead: the workflow settles failed.
	completeWorkflows(ctx, t, ws)
	w := wfView(ctx, t, ws, id)
	is.Equal(driver.WorkflowFailed, w.State, "a completed workflow with a tolerated dead task is failed, not succeeded")
	is.Contains(w.FailureReason, "dead", "the dead task key is recorded")
	is.False(w.CompletedAt.IsZero())
}

// runWorkflowDeadLeafTriggers pins that the exemption is never vacuous: a dead
// leaf (no dependents) always triggers the policy.
func runWorkflowDeadLeafTriggers(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	leaf := wfTask("leaf", "wfc_leaf_dead")
	leaf.MaxAttempts = 1
	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name: "wfc_leaf", OnFailure: driver.OnFailureSuspend,
		Tasks: []driver.WorkflowTask{leaf, wfTask("other", "wfc_leaf_other")},
	})

	killWFTask(ctx, t, store, "wfc_leaf_dead")

	// No dependent means no tolerant branch to preserve: the leaf triggers.
	failure := applyPolicyFor(ctx, t, ws, id)
	is.Equal(driver.OnFailureSuspend, failure.Policy)
	is.Equal([]string{"leaf"}, failure.DeadTasks)
	is.Equal(driver.WorkflowSuspended, wfView(ctx, t, ws, id).State)
}

// ---- CompleteWorkflows ----------------------------------------------------

func runWorkflowComplete(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name:  "wfc_done",
		Tasks: []driver.WorkflowTask{wfTask("a", "wfc_done_a"), wfTask("b", "wfc_done_b")},
	})

	finishWFTask(ctx, t, store, ws, "wfc_done_a", nil)
	completeWorkflows(ctx, t, ws)
	is.Equal(driver.WorkflowRunning, wfView(ctx, t, ws, id).State, "an unfinished workflow is left alone")

	finishWFTask(ctx, t, store, ws, "wfc_done_b", nil)
	completeWorkflows(ctx, t, ws)
	w := wfView(ctx, t, ws, id)
	is.Equal(driver.WorkflowSucceeded, w.State)
	is.False(w.CompletedAt.IsZero())
}

// ---- CancelWorkflow -------------------------------------------------------

func runWorkflowCancel(t *testing.T, _ driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	a := wfTask("a", "wfc_cn_a")
	a.CompensationKind = "wfc_cn_undo_a"
	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name:  "wfc_cn",
		Tasks: []driver.WorkflowTask{a, wfTask("b", "wfc_cn_b")},
		Deps:  []driver.WorkflowDep{{TaskKey: "b", DependsOnKey: "a"}},
	})

	is.NoError(ws.CancelWorkflow(ctx, id))
	w := wfView(ctx, t, ws, id)
	is.Equal(driver.WorkflowCancelled, w.State)
	is.False(w.CompletedAt.IsZero())
	is.Equal(driver.StateCancelled, wfTaskByKey(ctx, t, ws, id, "a").State)
	is.Equal(driver.StateCancelled, wfTaskByKey(ctx, t, ws, id, "b").State)
	tasks, err := ws.WorkflowTasks(ctx, id)
	is.NoError(err)
	is.Len(tasks, 2, "cancel inserts no compensations")

	is.True(driver.IsNotFound(ws.CancelWorkflow(ctx, id)), "a terminal workflow cannot be cancelled again")
	is.True(driver.IsNotFound(ws.CancelWorkflow(ctx, uuid.New())))
}

// ---- CompensateWorkflow (manual, from suspended) --------------------------

// runWorkflowCompensateManual pins a manual CompensateWorkflow on a suspended
// workflow: the succeeded task's compensation runs and the workflow settles
// failed.
func runWorkflowCompensateManual(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	a := wfTask("a", "wfc_cmp_a")
	a.CompensationKind = "wfc_cmp_undo_a"
	a.CompensationPayload = json.RawMessage(`{"undo":"a"}`)
	b := wfTask("b", "wfc_cmp_b")
	b.MaxAttempts = 1
	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name: "wfc_cmp", OnFailure: driver.OnFailureSuspend,
		Tasks: []driver.WorkflowTask{a, b},
	})

	// a succeeds (compensable), b dies: the suspend policy parks the workflow.
	finishWFTask(ctx, t, store, ws, "wfc_cmp_a", nil)
	killWFTask(ctx, t, store, "wfc_cmp_b")
	failure := applyPolicyFor(ctx, t, ws, id)
	is.Equal(driver.OnFailureSuspend, failure.Policy)
	is.Equal(driver.WorkflowSuspended, wfView(ctx, t, ws, id).State)

	// An operator compensates manually: the succeeded task's compensation is
	// inserted and the workflow moves to compensating.
	is.NoError(ws.CompensateWorkflow(ctx, id))
	is.Equal(driver.WorkflowCompensating, wfView(ctx, t, ws, id).State)
	compA := wfTaskByKey(ctx, t, ws, id, "comp:a")
	is.Equal(driver.StatePending, compA.State)
	is.Equal("wfc_cmp_undo_a", compA.Kind)
	is.JSONEq(`{"undo":"a"}`, string(compA.Payload), "the compensation carries its declared payload")

	finishWFTask(ctx, t, store, ws, "wfc_cmp_undo_a", nil)
	completeWorkflows(ctx, t, ws)
	is.Equal(driver.WorkflowFailed, wfView(ctx, t, ws, id).State)

	is.True(driver.IsNotFound(ws.CompensateWorkflow(ctx, id)), "a terminal workflow cannot be compensated")
	is.True(driver.IsNotFound(ws.CompensateWorkflow(ctx, uuid.New())))
}

// ---- RetryWorkflow during compensating ------------------------------------

// runWorkflowRetryDuringCompensating pins a retry issued while the workflow is
// still compensating: only the dead compensation tasks reset, the workflow
// stays compensating, and the original dead task is never resurrected.
func runWorkflowRetryDuringCompensating(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	a := wfTask("a", "wfc_rtc_a")
	a.CompensationKind = "wfc_rtc_undo_a"
	b := wfTask("b", "wfc_rtc_b")
	b.CompensationKind = "wfc_rtc_undo_b"
	c := wfTask("c", "wfc_rtc_c")
	c.MaxAttempts = 1
	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name: "wfc_rtc", OnFailure: driver.OnFailureCancel,
		Tasks: []driver.WorkflowTask{a, b, c, wfTask("d", "wfc_rtc_d")},
		Deps:  []driver.WorkflowDep{{TaskKey: "d", DependsOnKey: "c"}},
	})

	// a then b succeed (so comp:b runs first), c dies: the cancel policy inserts
	// the chain comp:b (pending) → comp:a (blocked) and moves to compensating.
	finishWFTask(ctx, t, store, ws, "wfc_rtc_a", nil)
	time.Sleep(tick)
	finishWFTask(ctx, t, store, ws, "wfc_rtc_b", nil)
	killWFTask(ctx, t, store, "wfc_rtc_c")
	failure := applyPolicyFor(ctx, t, ws, id)
	is.Equal(driver.OnFailureCancel, failure.Policy)
	is.Equal(driver.WorkflowCompensating, wfView(ctx, t, ws, id).State)

	// The first compensation dies. We retry WITHOUT settling, so the workflow is
	// still compensating when RetryWorkflow runs.
	killWFTask(ctx, t, store, "wfc_rtc_undo_b")
	is.Equal(driver.WorkflowCompensating, wfView(ctx, t, ws, id).State)

	is.NoError(ws.RetryWorkflow(ctx, id))
	is.Equal(driver.WorkflowCompensating, wfView(ctx, t, ws, id).State, "retry keeps a compensating workflow compensating")
	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "comp:b").State, "only the dead compensation is reset to pending")
	is.Equal(driver.StateBlocked, wfTaskByKey(ctx, t, ws, id, "comp:a").State, "the blocked compensation stays blocked")
	is.Equal(driver.StateDead, wfTaskByKey(ctx, t, ws, id, "c").State, "the original dead task is never resurrected")

	// The retried chain finishes and the workflow settles failed.
	finishWFTask(ctx, t, store, ws, "wfc_rtc_undo_b", nil)
	promoteUnblocked(ctx, t, ws)
	finishWFTask(ctx, t, store, ws, "wfc_rtc_undo_a", nil)
	completeWorkflows(ctx, t, ws)
	is.Equal(driver.WorkflowFailed, wfView(ctx, t, ws, id).State)
}

// ---- TaskResults ----------------------------------------------------------

func runWorkflowTaskResults(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name:  "wfc_res",
		Tasks: []driver.WorkflowTask{wfTask("a", "wfc_res_a"), wfTask("b", "wfc_res_b"), wfTask("c", "wfc_res_c")},
	})

	finishWFTask(ctx, t, store, ws, "wfc_res_a", json.RawMessage(`{"n":7}`))
	finishWFTask(ctx, t, store, ws, "wfc_res_b", nil)

	results, err := ws.TaskResults(ctx, id, []string{"a", "b", "c"})
	is.NoError(err)
	is.Len(results, 2, "an unfinished task has no result entry")
	is.JSONEq(`{"n":7}`, string(results["a"]))
	is.Nil(results["b"], "a succeeded task without a result maps to nil")

	results, err = ws.TaskResults(ctx, id, []string{"a"})
	is.NoError(err)
	is.Len(results, 1, "keys restrict the result set")

	results, err = ws.TaskResults(ctx, id, nil)
	is.NoError(err)
	is.Len(results, 2, "an empty key set returns every succeeded task")
}

// ---- AckTaskResult fencing ------------------------------------------------

func runWorkflowAckFencing(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name:  "wfc_fence",
		Tasks: []driver.WorkflowTask{wfTask("a", "wfc_fence_a")},
	})

	leased := dequeueN(ctx, t, store, driver.SourceWorkflow, "wfc_fence_a", 1, time.Minute)
	is.Len(leased, 1)
	err := ws.AckTaskResult(ctx, leased[0].ID, uuid.New(), json.RawMessage(`{}`))
	is.True(driver.IsNotFound(err), "a stale token is fenced")
	is.Equal(driver.StateActive, wfTaskByKey(ctx, t, ws, id, "a").State, "the fenced ack changed nothing")

	is.NoError(ws.AckTaskResult(ctx, leased[0].ID, leased[0].LeaseToken, json.RawMessage(`{"ok":true}`)))
	a := wfTaskByKey(ctx, t, ws, id, "a")
	is.Equal(driver.StateSucceeded, a.State)
	is.JSONEq(`{"ok":true}`, string(a.Result), "the result is persisted atomically with the ack")
}

// ---- Internal kinds stay out of PromoteDue --------------------------------

func runWorkflowInternalKinds(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.WorkflowParams{
		Name: "wfc_internal",
		Tasks: []driver.WorkflowTask{
			wfTask("a", "wfc_int_a"),
			{Key: "s", Kind: driver.KindSleep, SleepFor: reapLease},
		},
	})

	time.Sleep(reapWait) // the sleep's run_at is now overdue

	// The worker's maintenance loop promotes only registered kinds, and the
	// internal kinds are never registered: a due $sleep must stay scheduled.
	_, err := store.PromoteDue(ctx, driver.SourceWorkflow, []string{"wfc_int_a"})
	is.NoError(err)
	is.Equal(driver.StateScheduled, wfTaskByKey(ctx, t, ws, id, "s").State,
		"PromoteDue never touches an internal kind; the scheduler resolves it")

	// And a workflow-source dequeue of the registered kind never leases it.
	leased := dequeueN(ctx, t, store, driver.SourceWorkflow, driver.KindSleep, 10, time.Minute)
	is.Empty(leased, "a $sleep task is never pending, so it can never be leased")
}

// ---- Vacuum ---------------------------------------------------------------

func runWorkflowVacuum(t *testing.T, store driver.Store, ws driver.WorkflowStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	done := createWF(ctx, t, ws, driver.WorkflowParams{
		Name:  "wfc_vac_done",
		Tasks: []driver.WorkflowTask{wfTask("a", "wfc_vac_a")},
	})
	live := createWF(ctx, t, ws, driver.WorkflowParams{
		Name:  "wfc_vac_live",
		Tasks: []driver.WorkflowTask{wfTask("a", "wfc_vac_b")},
	})
	doneTask := finishWFTask(ctx, t, store, ws, "wfc_vac_a", nil)
	completeWorkflows(ctx, t, ws)
	is.Equal(driver.WorkflowSucceeded, wfView(ctx, t, ws, done).State)

	removed, err := ws.VacuumWorkflows(ctx, 0)
	is.NoError(err)
	is.Zero(removed, "a non-positive retention retains everything")

	time.Sleep(50 * time.Millisecond)
	removed, err = ws.VacuumWorkflows(ctx, time.Millisecond)
	is.NoError(err)
	is.GreaterOrEqual(removed, int64(1))
	_, err = ws.GetWorkflow(ctx, done)
	is.True(driver.IsNotFound(err), "the terminal workflow is gone")
	_, err = store.GetJob(ctx, driver.SourceWorkflow, doneTask.ID)
	is.True(driver.IsNotFound(err), "its task jobs cascade")
	_, err = ws.GetWorkflow(ctx, live)
	is.NoError(err, "a live workflow survives any retention")
}
