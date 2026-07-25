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

// RunDAGConformance exercises the observable [driver.DAGStore]
// contract against the Store returned by newStore, skipping cleanly when the
// store does not implement the capability. newStore is called once; every
// subtest shares that Store and stays independent by using unique workflow
// names and kinds, so a backend need not reset between subtests.
//
// The scheduler methods are set-based across the whole store, so subtests
// assert on the states of their own workflow's tasks (through DAGTasks
// and GetDAG), never on the global counts those methods return.
func RunDAGConformance(t *testing.T, newStore func(t *testing.T) driver.Store) {
	t.Helper()
	store := newStore(t)
	ws, ok := store.(driver.DAGStore)
	if !ok {
		t.Skipf("store %T does not implement driver.DAGStore; skipping the workflow conformance suite", store)
	}

	t.Run("Create", func(t *testing.T) { runDAGCreate(t, store, ws) })
	t.Run("Dedupe", func(t *testing.T) { runDAGDedupe(t, store, ws) })
	t.Run("PromotionCascade", func(t *testing.T) { runDAGPromotionCascade(t, store, ws) })
	t.Run("Sleep", func(t *testing.T) { runDAGSleep(t, store, ws) })
	t.Run("Signal", func(t *testing.T) { runDAGSignal(t, store, ws) })
	t.Run("FailurePolicyCancel", func(t *testing.T) { runDAGFailureCancel(t, store, ws) })
	t.Run("FailurePolicySuspendAndRetry", func(t *testing.T) { runDAGSuspendRetry(t, store, ws) })
	t.Run("FailurePolicyMixedIgnoreDeadDeps", func(t *testing.T) { runDAGMixedIgnoreDeadDeps(t, store, ws) })
	t.Run("FailurePolicyFullyIgnoredCompletesFailed", func(t *testing.T) { runDAGFullyIgnoredDeadDeps(t, store, ws) })
	t.Run("FailurePolicyDeadLeafTriggers", func(t *testing.T) { runDAGDeadLeafTriggers(t, store, ws) })
	t.Run("CompleteDAGs", func(t *testing.T) { runDAGComplete(t, store, ws) })
	t.Run("CompleteDAGsDefersNonToleratedDead", func(t *testing.T) { runDAGCompleteDefersNonToleratedDead(t, store, ws) })
	t.Run("CancelDAG", func(t *testing.T) { runDAGCancel(t, store, ws) })
	t.Run("CompensateDAGManual", func(t *testing.T) { runDAGCompensateManual(t, store, ws) })
	t.Run("RetryDuringCompensating", func(t *testing.T) { runDAGRetryDuringCompensating(t, store, ws) })
	t.Run("TaskResults", func(t *testing.T) { runDAGTaskResults(t, store, ws) })
	t.Run("AckTaskResultFencing", func(t *testing.T) { runDAGAckFencing(t, store, ws) })
	t.Run("InternalKindsStayInternal", func(t *testing.T) { runDAGInternalKinds(t, store, ws) })
	t.Run("Vacuum", func(t *testing.T) { runDAGVacuum(t, store, ws) })
	t.Run("CompletedVacuumExemptsDAGTasks", func(t *testing.T) { runDAGCompletedVacuumExemption(t, store, ws) })
}

// ---- shared helpers -------------------------------------------------------

func wfTask(key, kind string) driver.DAGTask {
	return driver.DAGTask{Key: key, Kind: kind, Payload: json.RawMessage(`{}`), MaxAttempts: 3}
}

func createWF(ctx context.Context, t *testing.T, ws driver.DAGStore, p driver.DAGParams) uuid.UUID {
	t.Helper()
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	inserted, _, err := ws.CreateDAG(ctx, p)
	require.NoError(t, err)
	require.True(t, inserted)
	return p.ID
}

func wfTaskByKey(ctx context.Context, t *testing.T, ws driver.DAGStore, id uuid.UUID, key string) driver.Job {
	t.Helper()
	tasks, err := ws.DAGTasks(ctx, id)
	require.NoError(t, err)
	for _, j := range tasks {
		if j.TaskKey == key {
			return j
		}
	}
	t.Fatalf("task %q not found in dag %s", key, id)
	return driver.Job{}
}

func wfView(ctx context.Context, t *testing.T, ws driver.DAGStore, id uuid.UUID) driver.DAGView {
	t.Helper()
	w, err := ws.GetDAG(ctx, id)
	require.NoError(t, err)
	return *w
}

// finishWFTask leases the one due pending workflow task of the kind and acks
// it with the result.
func finishWFTask(ctx context.Context, t *testing.T, store driver.Store, ws driver.DAGStore, kind string, result json.RawMessage) driver.Job {
	t.Helper()
	leased := dequeueN(ctx, t, store, driver.SourceDAG, kind, 1, time.Minute)
	require.Len(t, leased, 1, "expected one due pending task of kind %q", kind)
	require.NoError(t, ws.AckTaskResult(ctx, leased[0].ID, leased[0].LeaseToken, result))
	return leased[0]
}

// killWFTask leases the one due pending workflow task of the kind and
// dead-letters it.
func killWFTask(ctx context.Context, t *testing.T, store driver.Store, kind string) driver.Job {
	t.Helper()
	leased := dequeueN(ctx, t, store, driver.SourceDAG, kind, 1, time.Minute)
	require.Len(t, leased, 1, "expected one due pending task of kind %q", kind)
	require.NoError(t, store.Dead(ctx, leased[0].ID, leased[0].LeaseToken, "boom"))
	return leased[0]
}

// applyPolicyFor runs ApplyFailurePolicy and returns this workflow's failure
// report; the pass is set-based, so other dags may appear in the result.
func applyPolicyFor(ctx context.Context, t *testing.T, ws driver.DAGStore, id uuid.UUID) driver.DAGFailure {
	t.Helper()
	failures, err := ws.ApplyFailurePolicy(ctx)
	require.NoError(t, err)
	for _, fl := range failures {
		if fl.DAGID == id {
			return fl
		}
	}
	t.Fatalf("workflow %s not present in the ApplyFailurePolicy report", id)
	return driver.DAGFailure{}
}

// completeDAGs runs one CompleteDAGs pass, ignoring the global count.
func completeDAGs(ctx context.Context, t *testing.T, ws driver.DAGStore) {
	t.Helper()
	_, err := ws.CompleteDAGs(ctx)
	require.NoError(t, err)
}

// promoteUnblocked runs one PromoteUnblocked pass, ignoring the global count.
func promoteUnblocked(ctx context.Context, t *testing.T, ws driver.DAGStore) {
	t.Helper()
	_, err := ws.PromoteUnblocked(ctx)
	require.NoError(t, err)
}

// ---- Create ---------------------------------------------------------------

func runDAGCreate(t *testing.T, _ driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_create", OnFailure: driver.OnFailureCancel,
		Meta: map[string]string{"tenant": "t1"},
		Tasks: []driver.DAGTask{
			wfTask("a", "wfc_create_a"),
			wfTask("b", "wfc_create_b"),
			{Key: "s", Kind: driver.KindSleep, SleepFor: time.Hour},
			{Key: "s2", Kind: driver.KindSleep, SleepFor: time.Hour},
			{Key: "g", Kind: driver.KindSignal, SignalName: "go"},
		},
		Deps: []driver.DAGDep{
			{TaskKey: "b", DependsOnKey: "a"},
			{TaskKey: "s2", DependsOnKey: "a"},
		},
	})

	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "a").State, "a dependency-free task is pending")
	is.Equal(driver.StateBlocked, wfTaskByKey(ctx, t, ws, id, "b").State, "a task with deps is blocked")
	s := wfTaskByKey(ctx, t, ws, id, "s")
	is.Equal(driver.StateScheduled, s.State, "a root $sleep is scheduled")
	is.True(s.RunAt.After(time.Now().Add(30*time.Minute)), "the sleep timer reflects SleepFor on the backend clock")
	s2 := wfTaskByKey(ctx, t, ws, id, "s2")
	is.Equal(driver.StateBlocked, s2.State, "a $sleep with deps is blocked")
	is.True(s2.RunAt.Before(time.Now().Add(30*time.Minute)),
		"a blocked $sleep's timer has not started: run_at is resolved at promotion, not creation")
	is.Equal(driver.StateWaiting, wfTaskByKey(ctx, t, ws, id, "g").State, "a root $signal waits")

	w := wfView(ctx, t, ws, id)
	is.Equal(driver.DAGRunning, w.State)
	is.Equal(driver.OnFailureCancel, w.OnFailure)
	is.Equal(map[string]string{"tenant": "t1"}, w.Meta)
	is.False(w.CreatedAt.IsZero())
	is.True(w.CompletedAt.IsZero())

	a := wfTaskByKey(ctx, t, ws, id, "a")
	is.Equal(driver.SourceDAG, a.Source)
	is.Equal(id, a.DAGID)
	is.Equal(map[string]string{"tenant": "t1"}, a.Meta, "workflow meta propagates onto task jobs")

	_, err := ws.GetDAG(ctx, uuid.New())
	is.True(driver.IsNotFound(err), "a missing workflow is a typed not-found")
	_, err = ws.DAGTasks(ctx, uuid.New())
	is.True(driver.IsNotFound(err))

	// The admin list finds it, newest-first, with a correct total.
	views, total, err := ws.ListDAGs(ctx, driver.DAGFilter{Name: "wfc_create"}, 0, 10)
	is.NoError(err)
	is.Equal(int64(1), total)
	is.Len(views, 1)
	is.Equal(id, views[0].ID)
}

// ---- Dedupe ---------------------------------------------------------------

func runDAGDedupe(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	params := func() driver.DAGParams {
		return driver.DAGParams{
			ID: uuid.New(), Name: "wfc_dedupe", IdempotencyKey: "k1",
			Tasks: []driver.DAGTask{wfTask("a", "wfc_dedupe_a")},
		}
	}
	first := createWF(ctx, t, ws, params())
	is.Equal(driver.OnFailureCancel, wfView(ctx, t, ws, first).OnFailure,
		"an empty OnFailure normalizes to the cancel default")

	inserted, existing, err := ws.CreateDAG(ctx, params())
	is.NoError(err)
	is.False(inserted, "a live execution holds the (name, key)")
	is.Equal(first, existing, "the live execution's id is returned")

	// Terminal frees the key.
	finishWFTask(ctx, t, store, ws, "wfc_dedupe_a", nil)
	completeDAGs(ctx, t, ws)
	is.Equal(driver.DAGSucceeded, wfView(ctx, t, ws, first).State)

	inserted, _, err = ws.CreateDAG(ctx, params())
	is.NoError(err)
	is.True(inserted, "a terminal workflow frees the idempotency key")
}

// ---- Promotion cascade ----------------------------------------------------

func runDAGPromotionCascade(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_cascade",
		Tasks: []driver.DAGTask{
			wfTask("a", "wfc_cas_a"), wfTask("b", "wfc_cas_b"),
			wfTask("c", "wfc_cas_c"), wfTask("d", "wfc_cas_d"),
		},
		Deps: []driver.DAGDep{
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

func runDAGSleep(t *testing.T, _ driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_sleep",
		Tasks: []driver.DAGTask{
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

func runDAGSignal(t *testing.T, _ driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name:  "wfc_signal",
		Tasks: []driver.DAGTask{{Key: "g", Kind: driver.KindSignal, SignalName: "approved"}},
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

func runDAGFailureCancel(t *testing.T, store driver.Store, ws driver.DAGStore) {
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
	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_fpc", OnFailure: driver.OnFailureCancel,
		Tasks: []driver.DAGTask{a, b, c, wfTask("d", "wfc_fpc_d")},
		Deps:  []driver.DAGDep{{TaskKey: "d", DependsOnKey: "c"}},
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
	is.Equal(driver.DAGCompensating, w.State)
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
	completeDAGs(ctx, t, ws)
	w = wfView(ctx, t, ws, id)
	is.Equal(driver.DAGFailed, w.State)
	is.False(w.CompletedAt.IsZero())
}

// ---- Failure policy: suspend, then retry ----------------------------------

func runDAGSuspendRetry(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	a := wfTask("a", "wfc_fps_a")
	a.MaxAttempts = 1
	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_fps", OnFailure: driver.OnFailureSuspend,
		Tasks: []driver.DAGTask{a, wfTask("b", "wfc_fps_b")},
	})

	killWFTask(ctx, t, store, "wfc_fps_a")
	failure := applyPolicyFor(ctx, t, ws, id)
	is.Equal(driver.OnFailureSuspend, failure.Policy)
	is.Equal([]string{"a"}, failure.DeadTasks)

	w := wfView(ctx, t, ws, id)
	is.Equal(driver.DAGSuspended, w.State)
	is.Contains(w.FailureReason, "a")
	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "b").State, "suspend leaves the tasks untouched")

	is.NoError(ws.RetryDAG(ctx, id))
	is.Equal(driver.DAGRunning, wfView(ctx, t, ws, id).State)
	retried := wfTaskByKey(ctx, t, ws, id, "a")
	is.Equal(driver.StatePending, retried.State)
	is.Zero(retried.Attempt, "retry grants a fresh budget")
	is.Zero(retried.ReapCount)

	is.True(driver.IsNotFound(ws.RetryDAG(ctx, uuid.New())), "retrying a missing workflow is not-found")
}

// ---- Failure policy: IgnoreDeadDeps exemption -----------------------------

// runDAGMixedIgnoreDeadDeps pins that a dead task keeps triggering the
// policy as long as one of its dependents does not tolerate dead deps: the
// exemption requires every dependent to opt in.
func runDAGMixedIgnoreDeadDeps(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	dead := wfTask("dead", "wfc_mix_dead")
	dead.MaxAttempts = 1
	tol := wfTask("tol", "wfc_mix_tol")
	tol.IgnoreDeadDeps = true
	strict := wfTask("strict", "wfc_mix_strict") // does not tolerate dead deps
	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_mix", OnFailure: driver.OnFailureSuspend,
		Tasks: []driver.DAGTask{dead, tol, strict},
		Deps: []driver.DAGDep{
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
	is.Equal(driver.DAGSuspended, w.State)
	is.Contains(w.FailureReason, "dead")
}

// runDAGFullyIgnoredDeadDeps pins that a dead task every dependent
// tolerates does not trigger the policy — the tolerant branch runs to
// completion — yet the finished workflow settles failed, not succeeded.
func runDAGFullyIgnoredDeadDeps(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	dead := wfTask("dead", "wfc_all_dead")
	dead.MaxAttempts = 1
	tol := wfTask("tol", "wfc_all_tol")
	tol.IgnoreDeadDeps = true
	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_all", OnFailure: driver.OnFailureCancel,
		Tasks: []driver.DAGTask{dead, tol},
		Deps:  []driver.DAGDep{{TaskKey: "tol", DependsOnKey: "dead"}},
	})

	killWFTask(ctx, t, store, "wfc_all_dead")

	// Every dependent tolerates the death, so the policy leaves it running.
	failures, err := ws.ApplyFailurePolicy(ctx)
	is.NoError(err)
	for _, fl := range failures {
		is.NotEqual(id, fl.DAGID, "a fully-tolerated dead task must not trigger the policy")
	}
	is.Equal(driver.DAGRunning, wfView(ctx, t, ws, id).State)

	// The tolerant branch runs to completion.
	promoteUnblocked(ctx, t, ws)
	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "tol").State, "the tolerant dependent is promoted")
	finishWFTask(ctx, t, store, ws, "wfc_all_tol", nil)

	// Every task is terminal with one dead: the workflow settles failed.
	completeDAGs(ctx, t, ws)
	w := wfView(ctx, t, ws, id)
	is.Equal(driver.DAGFailed, w.State, "a completed workflow with a tolerated dead task is failed, not succeeded")
	is.Contains(w.FailureReason, "dead", "the dead task key is recorded")
	is.False(w.CompletedAt.IsZero())
}

// runDAGDeadLeafTriggers pins that the exemption is never vacuous: a dead
// leaf (no dependents) always triggers the policy.
func runDAGDeadLeafTriggers(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	leaf := wfTask("leaf", "wfc_leaf_dead")
	leaf.MaxAttempts = 1
	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_leaf", OnFailure: driver.OnFailureSuspend,
		Tasks: []driver.DAGTask{leaf, wfTask("other", "wfc_leaf_other")},
	})

	killWFTask(ctx, t, store, "wfc_leaf_dead")

	// No dependent means no tolerant branch to preserve: the leaf triggers.
	failure := applyPolicyFor(ctx, t, ws, id)
	is.Equal(driver.OnFailureSuspend, failure.Policy)
	is.Equal([]string{"leaf"}, failure.DeadTasks)
	is.Equal(driver.DAGSuspended, wfView(ctx, t, ws, id).State)
}

// ---- CompleteDAGs ----------------------------------------------------

func runDAGComplete(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name:  "wfc_done",
		Tasks: []driver.DAGTask{wfTask("a", "wfc_done_a"), wfTask("b", "wfc_done_b")},
	})

	finishWFTask(ctx, t, store, ws, "wfc_done_a", nil)
	completeDAGs(ctx, t, ws)
	is.Equal(driver.DAGRunning, wfView(ctx, t, ws, id).State, "an unfinished workflow is left alone")

	finishWFTask(ctx, t, store, ws, "wfc_done_b", nil)
	completeDAGs(ctx, t, ws)
	w := wfView(ctx, t, ws, id)
	is.Equal(driver.DAGSucceeded, w.State)
	is.False(w.CompletedAt.IsZero())
}

// runDAGCompleteDefersNonToleratedDead pins the tolerance re-check in the
// running-completion branch: when a workflow is all-terminal but carries a
// NON-tolerated dead task (here a dead leaf) and its OnFailure policy has not
// yet run, CompleteDAGs must leave it running — not settle it failed and
// skip the compensations. It reproduces the race where a task dies in the window
// between the separate ApplyFailurePolicy and CompleteDAGs transactions by
// driving completion BEFORE the policy pass. ApplyFailurePolicy then applies the
// cancel policy, inserting the compensation the premature settle would have
// dropped.
func runDAGCompleteDefersNonToleratedDead(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	a := wfTask("a", "wfc_defer_a")
	a.CompensationKind = "wfc_defer_undo_a"
	a.CompensationPayload = json.RawMessage(`{"undo":"a"}`)
	c := wfTask("c", "wfc_defer_c") // a dead leaf: no dependents, non-tolerated
	c.MaxAttempts = 1
	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_defer", OnFailure: driver.OnFailureCancel,
		Tasks: []driver.DAGTask{a, c},
	})

	// a succeeds (compensable) and the leaf c dies: every task is terminal, but
	// c is a non-tolerated dead leaf whose policy has not run.
	finishWFTask(ctx, t, store, ws, "wfc_defer_a", nil)
	killWFTask(ctx, t, store, "wfc_defer_c")

	// The death landed in the race window (completion runs before the policy).
	// CompleteDAGs must NOT settle the workflow failed — doing so would skip
	// the cancel compensations.
	completeDAGs(ctx, t, ws)
	is.Equal(driver.DAGRunning, wfView(ctx, t, ws, id).State,
		"an all-terminal workflow with a non-tolerated dead task is left running for the policy")

	// ApplyFailurePolicy now runs the cancel policy: it inserts the succeeded
	// task's compensation and moves the workflow to compensating.
	failure := applyPolicyFor(ctx, t, ws, id)
	is.Equal(driver.OnFailureCancel, failure.Policy)
	is.Equal([]string{"c"}, failure.DeadTasks)
	is.Equal(driver.DAGCompensating, wfView(ctx, t, ws, id).State)
	compA := wfTaskByKey(ctx, t, ws, id, "comp:a")
	is.Equal(driver.StatePending, compA.State, "the policy inserts the compensation a premature settle would have skipped")
	is.Equal("wfc_defer_undo_a", compA.Kind)

	// The chain runs and the workflow settles failed — now with compensation.
	finishWFTask(ctx, t, store, ws, "wfc_defer_undo_a", nil)
	completeDAGs(ctx, t, ws)
	w := wfView(ctx, t, ws, id)
	is.Equal(driver.DAGFailed, w.State)
	is.Contains(w.FailureReason, "c", "the dead task is recorded")
	is.False(w.CompletedAt.IsZero())
}

// ---- CancelDAG -------------------------------------------------------

func runDAGCancel(t *testing.T, _ driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	a := wfTask("a", "wfc_cn_a")
	a.CompensationKind = "wfc_cn_undo_a"
	id := createWF(ctx, t, ws, driver.DAGParams{
		Name:  "wfc_cn",
		Tasks: []driver.DAGTask{a, wfTask("b", "wfc_cn_b")},
		Deps:  []driver.DAGDep{{TaskKey: "b", DependsOnKey: "a"}},
	})

	is.NoError(ws.CancelDAG(ctx, id))
	w := wfView(ctx, t, ws, id)
	is.Equal(driver.DAGCancelled, w.State)
	is.False(w.CompletedAt.IsZero())
	is.Equal(driver.StateCancelled, wfTaskByKey(ctx, t, ws, id, "a").State)
	is.Equal(driver.StateCancelled, wfTaskByKey(ctx, t, ws, id, "b").State)
	tasks, err := ws.DAGTasks(ctx, id)
	is.NoError(err)
	is.Len(tasks, 2, "cancel inserts no compensations")

	is.True(driver.IsNotFound(ws.CancelDAG(ctx, id)), "a terminal workflow cannot be cancelled again")
	is.True(driver.IsNotFound(ws.CancelDAG(ctx, uuid.New())))
}

// ---- CompensateDAG (manual, from suspended) --------------------------

// runDAGCompensateManual pins a manual CompensateDAG on a suspended
// workflow: the succeeded task's compensation runs and the workflow settles
// failed.
func runDAGCompensateManual(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	a := wfTask("a", "wfc_cmp_a")
	a.CompensationKind = "wfc_cmp_undo_a"
	a.CompensationPayload = json.RawMessage(`{"undo":"a"}`)
	b := wfTask("b", "wfc_cmp_b")
	b.MaxAttempts = 1
	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_cmp", OnFailure: driver.OnFailureSuspend,
		Tasks: []driver.DAGTask{a, b},
	})

	// a succeeds (compensable), b dies: the suspend policy parks the workflow.
	finishWFTask(ctx, t, store, ws, "wfc_cmp_a", nil)
	killWFTask(ctx, t, store, "wfc_cmp_b")
	failure := applyPolicyFor(ctx, t, ws, id)
	is.Equal(driver.OnFailureSuspend, failure.Policy)
	is.Equal(driver.DAGSuspended, wfView(ctx, t, ws, id).State)

	// An operator compensates manually: the succeeded task's compensation is
	// inserted and the workflow moves to compensating.
	is.NoError(ws.CompensateDAG(ctx, id))
	is.Equal(driver.DAGCompensating, wfView(ctx, t, ws, id).State)
	compA := wfTaskByKey(ctx, t, ws, id, "comp:a")
	is.Equal(driver.StatePending, compA.State)
	is.Equal("wfc_cmp_undo_a", compA.Kind)
	is.JSONEq(`{"undo":"a"}`, string(compA.Payload), "the compensation carries its declared payload")

	finishWFTask(ctx, t, store, ws, "wfc_cmp_undo_a", nil)
	completeDAGs(ctx, t, ws)
	is.Equal(driver.DAGFailed, wfView(ctx, t, ws, id).State)

	is.True(driver.IsNotFound(ws.CompensateDAG(ctx, id)), "a terminal workflow cannot be compensated")
	is.True(driver.IsNotFound(ws.CompensateDAG(ctx, uuid.New())))
}

// ---- RetryDAG during compensating ------------------------------------

// runDAGRetryDuringCompensating pins a retry issued while the workflow is
// still compensating: only the dead compensation tasks reset, the workflow
// stays compensating, and the original dead task is never resurrected.
func runDAGRetryDuringCompensating(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	a := wfTask("a", "wfc_rtc_a")
	a.CompensationKind = "wfc_rtc_undo_a"
	b := wfTask("b", "wfc_rtc_b")
	b.CompensationKind = "wfc_rtc_undo_b"
	c := wfTask("c", "wfc_rtc_c")
	c.MaxAttempts = 1
	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_rtc", OnFailure: driver.OnFailureCancel,
		Tasks: []driver.DAGTask{a, b, c, wfTask("d", "wfc_rtc_d")},
		Deps:  []driver.DAGDep{{TaskKey: "d", DependsOnKey: "c"}},
	})

	// a then b succeed (so comp:b runs first), c dies: the cancel policy inserts
	// the chain comp:b (pending) → comp:a (blocked) and moves to compensating.
	finishWFTask(ctx, t, store, ws, "wfc_rtc_a", nil)
	time.Sleep(tick)
	finishWFTask(ctx, t, store, ws, "wfc_rtc_b", nil)
	killWFTask(ctx, t, store, "wfc_rtc_c")
	failure := applyPolicyFor(ctx, t, ws, id)
	is.Equal(driver.OnFailureCancel, failure.Policy)
	is.Equal(driver.DAGCompensating, wfView(ctx, t, ws, id).State)

	// The first compensation dies. We retry WITHOUT settling, so the workflow is
	// still compensating when RetryDAG runs.
	killWFTask(ctx, t, store, "wfc_rtc_undo_b")
	is.Equal(driver.DAGCompensating, wfView(ctx, t, ws, id).State)

	is.NoError(ws.RetryDAG(ctx, id))
	is.Equal(driver.DAGCompensating, wfView(ctx, t, ws, id).State, "retry keeps a compensating workflow compensating")
	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "comp:b").State, "only the dead compensation is reset to pending")
	is.Equal(driver.StateBlocked, wfTaskByKey(ctx, t, ws, id, "comp:a").State, "the blocked compensation stays blocked")
	is.Equal(driver.StateDead, wfTaskByKey(ctx, t, ws, id, "c").State, "the original dead task is never resurrected")

	// The retried chain finishes and the workflow settles failed.
	finishWFTask(ctx, t, store, ws, "wfc_rtc_undo_b", nil)
	promoteUnblocked(ctx, t, ws)
	finishWFTask(ctx, t, store, ws, "wfc_rtc_undo_a", nil)
	completeDAGs(ctx, t, ws)
	is.Equal(driver.DAGFailed, wfView(ctx, t, ws, id).State)
}

// ---- TaskResults ----------------------------------------------------------

func runDAGTaskResults(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name:  "wfc_res",
		Tasks: []driver.DAGTask{wfTask("a", "wfc_res_a"), wfTask("b", "wfc_res_b"), wfTask("c", "wfc_res_c")},
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

func runDAGAckFencing(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name:  "wfc_fence",
		Tasks: []driver.DAGTask{wfTask("a", "wfc_fence_a")},
	})

	leased := dequeueN(ctx, t, store, driver.SourceDAG, "wfc_fence_a", 1, time.Minute)
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

func runDAGInternalKinds(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_internal",
		Tasks: []driver.DAGTask{
			wfTask("a", "wfc_int_a"),
			{Key: "s", Kind: driver.KindSleep, SleepFor: reapLease},
		},
	})

	time.Sleep(reapWait) // the sleep's run_at is now overdue

	// The worker's maintenance loop promotes only registered kinds, and the
	// internal kinds are never registered: a due $sleep must stay scheduled.
	_, err := store.PromoteDue(ctx, driver.SourceDAG, []string{"wfc_int_a"})
	is.NoError(err)
	is.Equal(driver.StateScheduled, wfTaskByKey(ctx, t, ws, id, "s").State,
		"PromoteDue never touches an internal kind; the scheduler resolves it")

	// And a workflow-source dequeue of the registered kind never leases it.
	leased := dequeueN(ctx, t, store, driver.SourceDAG, driver.KindSleep, 10, time.Minute)
	is.Empty(leased, "a $sleep task is never pending, so it can never be leased")
}

// ---- Vacuum ---------------------------------------------------------------

func runDAGVacuum(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	done := createWF(ctx, t, ws, driver.DAGParams{
		Name:  "wfc_vac_done",
		Tasks: []driver.DAGTask{wfTask("a", "wfc_vac_a")},
	})
	live := createWF(ctx, t, ws, driver.DAGParams{
		Name:  "wfc_vac_live",
		Tasks: []driver.DAGTask{wfTask("a", "wfc_vac_b")},
	})
	doneTask := finishWFTask(ctx, t, store, ws, "wfc_vac_a", nil)
	completeDAGs(ctx, t, ws)
	is.Equal(driver.DAGSucceeded, wfView(ctx, t, ws, done).State)

	removed, err := ws.VacuumDAGs(ctx, 0)
	is.NoError(err)
	is.Zero(removed, "a non-positive retention retains everything")

	time.Sleep(50 * time.Millisecond)
	removed, err = ws.VacuumDAGs(ctx, time.Millisecond)
	is.NoError(err)
	is.GreaterOrEqual(removed, int64(1))
	_, err = ws.GetDAG(ctx, done)
	is.True(driver.IsNotFound(err), "the terminal workflow is gone")
	_, err = store.GetJob(ctx, driver.SourceDAG, doneTask.ID)
	is.True(driver.IsNotFound(err), "its task jobs cascade")
	_, err = ws.GetDAG(ctx, live)
	is.NoError(err, "a live workflow survives any retention")
}

// ---- VacuumCompleted exemption for workflow-owned jobs --------------------

// runDAGCompletedVacuumExemption pins the design ruling that
// VacuumCompleted must never touch workflow-owned jobs: a succeeded task can
// sit for as long as the workflow keeps running behind it (a long Sleep or
// WaitSignal further down the DAG), and the completed-job retention sweep must
// never delete it out from under a still-running workflow — that would blind
// ResultOf for downstream tasks and CompleteDAGs itself. Their lifecycle
// belongs to the workflow: only VacuumDAGs' terminal-workflow cascade
// removes them. A plain queue job of the same age is unaffected and is still
// vacuumed normally.
func runDAGCompletedVacuumExemption(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_vac_exempt",
		Tasks: []driver.DAGTask{
			wfTask("a", "wfc_vac_exempt_a"),
			wfTask("b", "wfc_vac_exempt_b"),
		},
		Deps: []driver.DAGDep{{TaskKey: "b", DependsOnKey: "a"}},
	})
	finishWFTask(ctx, t, store, ws, "wfc_vac_exempt_a", json.RawMessage(`{"n":1}`))
	is.Equal(driver.StateBlocked, wfTaskByKey(ctx, t, ws, id, "b").State,
		"the workflow is still running: the downstream task is parked on its dependency")

	queueID := enqueueDue(ctx, t, store, "wfc_vac_exempt_queue")
	leased := dequeueN(ctx, t, store, driver.SourceQueue, "wfc_vac_exempt_queue", 1, time.Minute)
	is.Len(leased, 1)
	is.NoError(store.Ack(ctx, leased[0].ID, leased[0].LeaseToken))

	time.Sleep(50 * time.Millisecond)

	removed, err := store.VacuumCompleted(ctx, driver.SourceDAG, time.Millisecond)
	is.NoError(err)
	is.Zero(removed, "a succeeded workflow task must never be vacuumed by VacuumCompleted, at any retention")

	removed, err = store.VacuumCompleted(ctx, driver.SourceQueue, time.Millisecond)
	is.NoError(err)
	is.GreaterOrEqual(removed, int64(1), "a plain queue job of the same age is vacuumed normally")

	a := wfTaskByKey(ctx, t, ws, id, "a")
	is.Equal(driver.StateSucceeded, a.State, "the succeeded upstream task row survives")
	results, err := ws.TaskResults(ctx, id, []string{"a"})
	is.NoError(err)
	is.JSONEq(`{"n":1}`, string(results["a"]), "its persisted result survives, so ResultOf still resolves it")

	_, err = store.GetJob(ctx, driver.SourceQueue, queueID)
	is.True(driver.IsNotFound(err), "the plain queue job of the same age is gone")
}
