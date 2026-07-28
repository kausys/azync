package drivertest

import (
	"context"
	"encoding/json"
	"strings"
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
	t.Run("SnoozeDeadline", func(t *testing.T) { runDAGSnoozeDeadline(t, store, ws) })
	t.Run("SignalBufferedBeforeWaitDeliversOnPromotion", func(t *testing.T) { runDAGSignalBuffered(t, store, ws) })
	t.Run("SignalDedupeByMessageID", func(t *testing.T) { runDAGSignalDedupe(t, store, ws) })
	t.Run("SignalMissingOrTerminalDAGIsNotFound", func(t *testing.T) { runDAGSignalTerminal(t, store, ws) })
	t.Run("SignalSleepEarlyWakeDeferred", func(t *testing.T) { runDAGSignalSleepBuffered(t, store, ws) })
	t.Run("SkipSettlesTerminalAndSatisfiesDeps", func(t *testing.T) { runDAGSkip(t, store, ws) })
	t.Run("PauseFreezesAndRetryResumes", func(t *testing.T) { runDAGPause(t, store, ws) })
	t.Run("FindDAGByKeyResolvesTheLiveRun", func(t *testing.T) { runDAGFindByKey(t, store, ws) })
	t.Run("DepsExposeTheGraph", func(t *testing.T) { runDAGDeps(t, store, ws) })
	t.Run("StateCounts", func(t *testing.T) { runDAGStateCounts(t, store, ws) })
	t.Run("TaskCounts", func(t *testing.T) { runDAGTaskCounts(t, store, ws) })
	t.Run("StartedAtTracksTheCurrentAttempt", func(t *testing.T) { runDAGStartedAt(t, store, ws) })
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
	delivered, dedup, err := ws.Signal(ctx, driver.DAGSignalParams{DAGID: id, Name: "hurry"})
	is.NoError(err)
	is.False(dedup)
	is.Equal(int64(1), delivered)
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

	delivered, dedup, err := ws.Signal(ctx, driver.DAGSignalParams{
		DAGID: id, Name: "other", Payload: json.RawMessage(`{}`),
	})
	is.NoError(err)
	is.False(dedup)
	is.Zero(delivered, "an unmatched signal name is buffered, not lost — and delivers nothing now")

	payload := json.RawMessage(`{"by":"ops"}`)
	delivered, dedup, err = ws.Signal(ctx, driver.DAGSignalParams{
		DAGID: id, Name: "approved", Payload: payload,
	})
	is.NoError(err)
	is.False(dedup)
	is.Equal(int64(1), delivered)
	g := wfTaskByKey(ctx, t, ws, id, "g")
	is.Equal(driver.StateSucceeded, g.State)
	is.JSONEq(string(payload), string(g.Result), "the signal payload is the task's result")

	delivered, _, err = ws.Signal(ctx, driver.DAGSignalParams{
		DAGID: id, Name: "approved", Payload: payload,
	})
	is.NoError(err)
	is.Zero(delivered, "a completed signal task never re-delivers; the repeat stays buffered")

	results, err := ws.TaskResults(ctx, id, []string{"g"})
	is.NoError(err)
	is.JSONEq(string(payload), string(results["g"].Result), "the result is visible through TaskResults")
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
	is.JSONEq(`{"n":7}`, string(results["a"].Result))
	is.Nil(results["b"].Result, "a succeeded task without a result maps to a nil Result")
	is.False(results["b"].Skipped, "succeeded-without-result is not skipped")

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
	is.JSONEq(`{"n":1}`, string(results["a"].Result), "its persisted result survives, so ResultOf still resolves it")

	_, err = store.GetJob(ctx, driver.SourceQueue, queueID)
	is.True(driver.IsNotFound(err), "the plain queue job of the same age is gone")
}

// ---- Snooze deadline --------------------------------------------------------

// runDAGSnoozeDeadline pins the bounded-NotReady contract: a task with a
// Deadline stamps deadline_at on its FIRST snooze (the budget measures time
// spent waiting, not the workflow's age), parks normally before the deadline,
// dead-letters atomically on a snooze past it (final attempt recorded, the
// failure policy reacts), and RetryDAG clears the stamped deadline so the
// retried wait starts with a fresh budget instead of instantly re-dying.
func runDAGSnoozeDeadline(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("escalates to dead past the deadline and the policy reacts", func(t *testing.T) {
		is := require.New(t)
		task := wfTask("wait", "wfc_ddl_wait")
		task.Deadline = reapLease // expires within the test
		id := createWF(ctx, t, ws, driver.DAGParams{
			Name: "wfc_ddl", OnFailure: driver.OnFailureSuspend,
			Tasks: []driver.DAGTask{task},
		})

		// First snooze stamps the deadline and parks normally.
		leased := dequeueN(ctx, t, store, driver.SourceDAG, "wfc_ddl_wait", 1, time.Minute)
		is.Len(leased, 1)
		is.True(leased[0].DeadlineAt.IsZero(), "no deadline is stamped before the first snooze")
		deadlined, err := store.Snooze(ctx, leased[0].ID, leased[0].LeaseToken, 0, "not ready")
		is.NoError(err)
		is.False(deadlined, "the stamping snooze itself never escalates")
		stamped := wfTaskByKey(ctx, t, ws, id, "wait")
		is.Equal(driver.StateScheduled, stamped.State)
		is.False(stamped.DeadlineAt.IsZero(), "the first snooze stamped the deadline")

		// Past the deadline, the next snooze dead-letters atomically.
		time.Sleep(reapWait)
		_, err = store.PromoteDue(ctx, driver.SourceDAG, []string{"wfc_ddl_wait"})
		is.NoError(err)
		leased = dequeueN(ctx, t, store, driver.SourceDAG, "wfc_ddl_wait", 1, time.Minute)
		is.Len(leased, 1)
		deadlined, err = store.Snooze(ctx, leased[0].ID, leased[0].LeaseToken, 0, "snooze deadline exceeded: still not ready")
		is.NoError(err)
		is.True(deadlined, "a snooze past the stamped deadline escalates")

		dead := wfTaskByKey(ctx, t, ws, id, "wait")
		is.Equal(driver.StateDead, dead.State)
		is.Contains(dead.LastError, "snooze deadline exceeded")
		attempts, err := store.JobAttempts(ctx, driver.SourceDAG, dead.ID)
		is.NoError(err)
		is.Len(attempts, 1, "the deadline death records the final attempt")

		failure := applyPolicyFor(ctx, t, ws, id)
		is.Equal(driver.OnFailureSuspend, failure.Policy)
		is.Equal(driver.DAGSuspended, wfView(ctx, t, ws, id).State)

		// Retry grants a fresh wait budget: the cleared deadline re-stamps on
		// the next first snooze instead of instantly re-killing the task.
		is.NoError(ws.RetryDAG(ctx, id))
		retried := wfTaskByKey(ctx, t, ws, id, "wait")
		is.Equal(driver.StatePending, retried.State)
		is.True(retried.DeadlineAt.IsZero(), "RetryDAG drops the stamped deadline")
		leased = dequeueN(ctx, t, store, driver.SourceDAG, "wfc_ddl_wait", 1, time.Minute)
		is.Len(leased, 1)
		deadlined, err = store.Snooze(ctx, leased[0].ID, leased[0].LeaseToken, 0, "not ready")
		is.NoError(err)
		is.False(deadlined, "the retried wait parks again instead of dying")
	})

	t.Run("parks normally before the deadline", func(t *testing.T) {
		is := require.New(t)
		task := wfTask("wait", "wfc_ddl_early")
		task.Deadline = time.Hour
		id := createWF(ctx, t, ws, driver.DAGParams{
			Name: "wfc_ddl_early", Tasks: []driver.DAGTask{task},
		})
		leased := dequeueN(ctx, t, store, driver.SourceDAG, "wfc_ddl_early", 1, time.Minute)
		is.Len(leased, 1)
		is.Equal(time.Hour, leased[0].SnoozeBudget, "the declared budget travels on the job")
		deadlined, err := store.Snooze(ctx, leased[0].ID, leased[0].LeaseToken, time.Minute, "not ready")
		is.NoError(err)
		is.False(deadlined)
		got := wfTaskByKey(ctx, t, ws, id, "wait")
		is.Equal(driver.StateScheduled, got.State)
		is.Zero(got.Attempt, "a pre-deadline snooze still hands the attempt back")
	})
}

// ---- Buffered signals -------------------------------------------------------

// runDAGSignalBuffered pins the durable-signal contract: a signal sent while
// its $signal task is still blocked is accepted and buffered (delivered=0,
// dedup=false, nil error), then handed to the task by DeliverBufferedSignals
// once promotion makes it deliverable — with the payload as the result. A
// second pass is idempotent.
func runDAGSignalBuffered(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_sigbuf",
		Tasks: []driver.DAGTask{
			wfTask("gate", "wfc_sigbuf_gate"),
			{Key: "approval", Kind: driver.KindSignal, SignalName: "approval"},
		},
		Deps: []driver.DAGDep{{TaskKey: "approval", DependsOnKey: "gate"}},
	})

	payload := json.RawMessage(`{"by":"ops"}`)
	delivered, dedup, err := ws.Signal(ctx, driver.DAGSignalParams{
		DAGID: id, Name: "approval", Payload: payload,
	})
	is.NoError(err, "a signal with nothing deliverable is accepted, not an error")
	is.False(dedup)
	is.Zero(delivered, "nothing was deliverable yet: the signal buffered")
	is.Equal(driver.StateBlocked, wfTaskByKey(ctx, t, ws, id, "approval").State,
		"the buffered signal must not touch a blocked task")

	// Unblock and promote: the wait becomes deliverable, and the buffered
	// signal lands on the very next DeliverBufferedSignals pass.
	finishWFTask(ctx, t, store, ws, "wfc_sigbuf_gate", nil)
	promoteUnblocked(ctx, t, ws)
	is.Equal(driver.StateWaiting, wfTaskByKey(ctx, t, ws, id, "approval").State)

	n, err := ws.DeliverBufferedSignals(ctx)
	is.NoError(err)
	is.GreaterOrEqual(n, int64(1))
	g := wfTaskByKey(ctx, t, ws, id, "approval")
	is.Equal(driver.StateSucceeded, g.State, "the buffered signal delivered on promotion")
	is.JSONEq(string(payload), string(g.Result))

	n, err = ws.DeliverBufferedSignals(ctx)
	is.NoError(err)
	is.Zero(n, "a second pass is idempotent: the signal was consumed")
}

// runDAGSignalDedupe pins MessageID dedupe: a repeat of the same id is
// accepted and dropped (deduplicated=true, no effects), the delivered result
// is the FIRST payload, and an empty MessageID disables dedupe entirely.
func runDAGSignalDedupe(t *testing.T, _ driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_sigdedupe",
		Tasks: []driver.DAGTask{
			{Key: "gate", Kind: driver.KindSignal, SignalName: "gate"},
			{Key: "late", Kind: driver.KindSignal, SignalName: "late"},
		},
		Deps: []driver.DAGDep{{TaskKey: "late", DependsOnKey: "gate"}},
	})

	// Dedupe on a buffered (not yet deliverable) signal: the repeat with a
	// different body must be dropped, and delivery uses the FIRST payload.
	first := json.RawMessage(`{"decision":"approve"}`)
	delivered, dedup, err := ws.Signal(ctx, driver.DAGSignalParams{
		DAGID: id, Name: "late", MessageID: "wh-1", Payload: first,
	})
	is.NoError(err)
	is.False(dedup)
	is.Zero(delivered)
	delivered, dedup, err = ws.Signal(ctx, driver.DAGSignalParams{
		DAGID: id, Name: "late", MessageID: "wh-1", Payload: json.RawMessage(`{"decision":"tampered"}`),
	})
	is.NoError(err)
	is.True(dedup, "a repeat MessageID is deduplicated")
	is.Zero(delivered)

	// Without a MessageID there is no dedupe: both inserts are accepted (the
	// first delivers immediately on the waiting gate).
	delivered, dedup, err = ws.Signal(ctx, driver.DAGSignalParams{DAGID: id, Name: "gate"})
	is.NoError(err)
	is.False(dedup)
	is.Equal(int64(1), delivered)
	_, dedup, err = ws.Signal(ctx, driver.DAGSignalParams{DAGID: id, Name: "gate"})
	is.NoError(err)
	is.False(dedup, "no MessageID, no dedupe")

	promoteUnblocked(ctx, t, ws)
	n, err := ws.DeliverBufferedSignals(ctx)
	is.NoError(err)
	is.GreaterOrEqual(n, int64(1))
	late := wfTaskByKey(ctx, t, ws, id, "late")
	is.Equal(driver.StateSucceeded, late.State)
	is.JSONEq(string(first), string(late.Result), "the delivered result is the FIRST payload, not the tampered repeat")
}

// runDAGSignalTerminal pins Signal's only error path: a missing or terminal
// workflow is not-found, and the rejected signal leaves no effective inbox
// row (a later pass delivers nothing).
func runDAGSignalTerminal(t *testing.T, _ driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	_, _, err := ws.Signal(ctx, driver.DAGSignalParams{DAGID: uuid.New(), Name: "ghost"})
	is.True(driver.IsNotFound(err), "a missing workflow is not-found")

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name:  "wfc_sigterm",
		Tasks: []driver.DAGTask{{Key: "g", Kind: driver.KindSignal, SignalName: "g"}},
	})
	is.NoError(ws.CancelDAG(ctx, id))
	_, _, err = ws.Signal(ctx, driver.DAGSignalParams{DAGID: id, Name: "g"})
	is.True(driver.IsNotFound(err), "a terminal workflow rejects signals as not-found")

	n, err := ws.DeliverBufferedSignals(ctx)
	is.NoError(err)
	is.Zero(n, "the rejected signal left nothing to deliver")
}

// runDAGSignalSleepBuffered pins the deferred early-wake: a wake signal
// buffered while its $sleep is still blocked shortens the timer at promotion
// time — DeliverBufferedSignals sets run_at to now, and CompleteDueSleeps
// finishes it in the same pass.
func runDAGSignalSleepBuffered(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_sigslp",
		Tasks: []driver.DAGTask{
			wfTask("gate", "wfc_sigslp_gate"),
			{Key: "nap", Kind: driver.KindSleep, SleepFor: time.Hour, SignalName: "nap"},
		},
		Deps: []driver.DAGDep{{TaskKey: "nap", DependsOnKey: "gate"}},
	})

	delivered, _, err := ws.Signal(ctx, driver.DAGSignalParams{DAGID: id, Name: "nap"})
	is.NoError(err)
	is.Zero(delivered, "the wake buffers while the sleep is blocked")

	finishWFTask(ctx, t, store, ws, "wfc_sigslp_gate", nil)
	promoteUnblocked(ctx, t, ws)
	is.Equal(driver.StateScheduled, wfTaskByKey(ctx, t, ws, id, "nap").State)

	n, err := ws.DeliverBufferedSignals(ctx)
	is.NoError(err)
	is.GreaterOrEqual(n, int64(1))
	_, err = ws.CompleteDueSleeps(ctx)
	is.NoError(err)
	is.Equal(driver.StateSucceeded, wfTaskByKey(ctx, t, ws, id, "nap").State,
		"the buffered wake shortened the hour-long timer to now")
}

// ---- Skip -------------------------------------------------------------------

// runDAGSkip pins the deliberate-no-op contract: Skip settles the task as
// StateSkipped (terminal, reason retained, no result, no attempt recorded),
// a skipped dependency satisfies its dependents like a succeeded one, the
// workflow completes succeeded, TaskResults reports the skip distinguishably,
// and a skipped task never enters a compensation chain.
func runDAGSkip(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	skipTask := wfTask("submit", "wfc_skip_submit")
	skipTask.CompensationKind = "wfc_skip_undo"
	skipTask.CompensationPayload = json.RawMessage(`{}`)
	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_skip",
		Tasks: []driver.DAGTask{
			skipTask,
			wfTask("downstream", "wfc_skip_down"),
		},
		Deps: []driver.DAGDep{{TaskKey: "downstream", DependsOnKey: "submit"}},
	})

	// Lease "submit" and settle it as skipped.
	leased := dequeueN(ctx, t, store, driver.SourceDAG, "wfc_skip_submit", 1, time.Minute)
	is.Len(leased, 1)
	is.NoError(store.Skip(ctx, leased[0].ID, leased[0].LeaseToken, "already VERIFIED"))

	skipped := wfTaskByKey(ctx, t, ws, id, "submit")
	is.Equal(driver.StateSkipped, skipped.State)
	is.Equal("already VERIFIED", skipped.LastError, "the reason is retained for ops")
	is.Nil(skipped.Result, "a skipped task has no result")
	is.False(skipped.CompletedAt.IsZero(), "skipped is a completion, not a failure")
	attempts, err := store.JobAttempts(ctx, driver.SourceDAG, skipped.ID)
	is.NoError(err)
	is.Empty(attempts, "a skip records no attempt history")

	// The skipped dependency satisfies its dependent.
	promoteUnblocked(ctx, t, ws)
	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "downstream").State,
		"a skipped dependency satisfies its dependents like a succeeded one")

	// TaskResults reports the skip distinguishably.
	results, err := ws.TaskResults(ctx, id, []string{"submit"})
	is.NoError(err)
	is.True(results["submit"].Skipped, "TaskResults must mark the skip, never a silent nil")
	is.Nil(results["submit"].Result)

	// The workflow settles succeeded with the skip in it.
	finishWFTask(ctx, t, store, ws, "wfc_skip_down", nil)
	completeDAGs(ctx, t, ws)
	is.Equal(driver.DAGSucceeded, wfView(ctx, t, ws, id).State,
		"a workflow of succeeded+skipped tasks settles succeeded")

	// A skipped task never compensates: kill a fresh workflow the same shape
	// after its submit was skipped, and no comp:submit chain appears.
	skipTask2 := wfTask("submit", "wfc_skip2_submit")
	skipTask2.CompensationKind = "wfc_skip2_undo"
	skipTask2.CompensationPayload = json.RawMessage(`{}`)
	doomed := wfTask("boom", "wfc_skip2_boom")
	doomed.MaxAttempts = 1
	id2 := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_skip2", OnFailure: driver.OnFailureCancel,
		Tasks: []driver.DAGTask{skipTask2, doomed},
		Deps:  []driver.DAGDep{{TaskKey: "boom", DependsOnKey: "submit"}},
	})
	leased = dequeueN(ctx, t, store, driver.SourceDAG, "wfc_skip2_submit", 1, time.Minute)
	is.Len(leased, 1)
	is.NoError(store.Skip(ctx, leased[0].ID, leased[0].LeaseToken, "already done"))
	promoteUnblocked(ctx, t, ws)
	killWFTask(ctx, t, store, "wfc_skip2_boom")
	applyPolicyFor(ctx, t, ws, id2)

	tasks, err := ws.DAGTasks(ctx, id2)
	is.NoError(err)
	for _, j := range tasks {
		is.False(strings.HasPrefix(j.TaskKey, driver.TaskKeyCompensationPrefix),
			"a skipped task did no work, so nothing compensates: found %q", j.TaskKey)
	}
	is.Equal(driver.DAGFailed, wfView(ctx, t, ws, id2).State,
		"nothing to compensate: the cancel policy settles the workflow failed")
}

// ---- Pause ------------------------------------------------------------------

// runDAGPause pins the operator-pause contract: PauseDAG freezes a running
// workflow (header paused with the reason, pending/scheduled tasks held out
// of the dequeue set), a signal arriving during the pause buffers, RetryDAG
// is the one resume verb (tasks back to the ready set, buffered signal
// delivered, workflow completes), pause outside running is not-found, and
// CancelDAG sweeps paused tasks too (the latent leak this feature closed).
func runDAGPause(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_pause",
		Tasks: []driver.DAGTask{
			wfTask("work", "wfc_pause_work"),
			{Key: "approval", Kind: driver.KindSignal, SignalName: "approval"},
			wfTask("after", "wfc_pause_after"),
		},
		Deps: []driver.DAGDep{{TaskKey: "after", DependsOnKey: "approval"}},
	})

	is.NoError(ws.PauseDAG(ctx, id, "provider outage"))
	w := wfView(ctx, t, ws, id)
	is.Equal(driver.DAGPaused, w.State)
	is.Equal("provider outage", w.FailureReason, "the pause reason is recorded")
	is.Equal(driver.StatePaused, wfTaskByKey(ctx, t, ws, id, "work").State)
	is.Equal(driver.StateWaiting, wfTaskByKey(ctx, t, ws, id, "approval").State,
		"a waiting signal task keeps its state; the paused header stops promotion instead")

	// Frozen: nothing dequeues, and a signal buffers instead of... well, it
	// would deliver to the waiting task — but its DEPENDENTS never promote
	// while paused, so the flow stays frozen either way.
	leased, err := store.DequeueBatch(ctx, driver.SourceDAG, driver.DequeueParams{
		Kind: "wfc_pause_work", Limit: 1, Lease: time.Minute,
	})
	is.NoError(err)
	is.Empty(leased, "a paused task never dequeues")
	delivered, _, err := ws.Signal(ctx, driver.DAGSignalParams{DAGID: id, Name: "approval"})
	is.NoError(err, "a paused workflow still accepts signals — it is live, just frozen")
	is.Equal(int64(1), delivered)
	promoteUnblocked(ctx, t, ws)
	is.Equal(driver.StateBlocked, wfTaskByKey(ctx, t, ws, id, "after").State,
		"nothing promotes while the header is paused")

	// Pause is not re-entrant and needs running: paused/terminal are not-found.
	is.True(driver.IsNotFound(ws.PauseDAG(ctx, id, "again")), "pausing a paused workflow is not-found")

	// Retry resumes: header running, tasks back in the ready set, and the
	// signal already delivered lets the successor promote.
	is.NoError(ws.RetryDAG(ctx, id))
	is.Equal(driver.DAGRunning, wfView(ctx, t, ws, id).State)
	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "work").State)
	promoteUnblocked(ctx, t, ws)
	is.Equal(driver.StatePending, wfTaskByKey(ctx, t, ws, id, "after").State)
	finishWFTask(ctx, t, store, ws, "wfc_pause_work", nil)
	finishWFTask(ctx, t, store, ws, "wfc_pause_after", nil)
	completeDAGs(ctx, t, ws)
	is.Equal(driver.DAGSucceeded, wfView(ctx, t, ws, id).State)
	is.True(driver.IsNotFound(ws.PauseDAG(ctx, id, "late")), "pausing a terminal workflow is not-found")

	// Cancel sweeps paused tasks: no zombie rows blocking completion forever.
	id2 := createWF(ctx, t, ws, driver.DAGParams{
		Name:  "wfc_pause_cancel",
		Tasks: []driver.DAGTask{wfTask("w", "wfc_pause_cx_w")},
	})
	is.NoError(ws.PauseDAG(ctx, id2, "freeze"))
	is.NoError(ws.CancelDAG(ctx, id2))
	is.Equal(driver.StateCancelled, wfTaskByKey(ctx, t, ws, id2, "w").State,
		"CancelDAG cancels operator-paused tasks too")
	is.Equal(driver.DAGCancelled, wfView(ctx, t, ws, id2).State)
}

// ---- FindDAGByKey ------------------------------------------------------------

// runDAGFindByKey pins the business-key lookup: with historical terminal
// runs holding the same (name, key), the lookup resolves the ONE live run;
// with no live run — or a wrong key — it is not-found (the right answer for
// a webhook arriving after the run settled).
func runDAGFindByKey(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	// First run with the key settles terminal, freeing it.
	first := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_bykey", IdempotencyKey: "acct-42",
		Tasks: []driver.DAGTask{wfTask("w", "wfc_bykey_w")},
	})
	finishWFTask(ctx, t, store, ws, "wfc_bykey_w", nil)
	completeDAGs(ctx, t, ws)
	is.Equal(driver.DAGSucceeded, wfView(ctx, t, ws, first).State)

	// Second (live) run reuses the freed key: the lookup must resolve IT.
	second := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_bykey", IdempotencyKey: "acct-42",
		Tasks: []driver.DAGTask{{Key: "g", Kind: driver.KindSignal, SignalName: "g"}},
	})
	got, err := ws.FindDAGByKey(ctx, "wfc_bykey", "acct-42")
	is.NoError(err)
	is.Equal(second, got, "the lookup resolves the live run, not the terminal one")

	// A paused run still holds the key (paused is live).
	is.NoError(ws.PauseDAG(ctx, second, "freeze"))
	got, err = ws.FindDAGByKey(ctx, "wfc_bykey", "acct-42")
	is.NoError(err)
	is.Equal(second, got)
	is.NoError(ws.RetryDAG(ctx, second))

	_, err = ws.FindDAGByKey(ctx, "wfc_bykey", "acct-ghost")
	is.True(driver.IsNotFound(err), "an unknown key is not-found")
	_, err = ws.FindDAGByKey(ctx, "wfc_bykey_other", "acct-42")
	is.True(driver.IsNotFound(err), "the key is scoped by definition name")
}

// ---- DAGDeps --------------------------------------------------------------

// runDAGDeps pins the edge read the admin graph rests on. The shape under test
// is a fan-out/fan-in — the case a timestamp ordering renders as a chain and
// gets silently wrong — plus the compensation links, which land in the same
// table later and must show up too.
func runDAGDeps(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	root := wfTask("root", "wfc_deps_root")
	root.CompensationKind = "wfc_deps_undo_root"
	fanA := wfTask("fan_a", "wfc_deps_fan_a")
	fanB := wfTask("fan_b", "wfc_deps_fan_b")
	sink := wfTask("sink", "wfc_deps_sink")
	sink.MaxAttempts = 1
	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_deps", OnFailure: driver.OnFailureSuspend,
		Tasks: []driver.DAGTask{root, fanA, fanB, sink},
		Deps: []driver.DAGDep{
			{TaskKey: "fan_a", DependsOnKey: "root"},
			{TaskKey: "fan_b", DependsOnKey: "root"},
			{TaskKey: "sink", DependsOnKey: "fan_a"},
			{TaskKey: "sink", DependsOnKey: "fan_b"},
		},
	})

	deps, err := ws.DAGDeps(ctx, id)
	is.NoError(err)
	is.Equal([]driver.DAGDep{
		{TaskKey: "fan_a", DependsOnKey: "root"},
		{TaskKey: "fan_b", DependsOnKey: "root"},
		{TaskKey: "sink", DependsOnKey: "fan_a"},
		{TaskKey: "sink", DependsOnKey: "fan_b"},
	}, deps, "every declared edge is readable, ordered by (task_key, depends_on_key)")

	got, err := ws.DAGDeps(ctx, uuid.New())
	is.NoError(err, "an unknown dag is not an error here — DAGTasks reports absence")
	is.Empty(got)

	// Compensation links land in the same table, so the graph of a compensated
	// run stays readable end to end.
	finishWFTask(ctx, t, store, ws, "wfc_deps_root", nil)
	promoteUnblocked(ctx, t, ws)
	finishWFTask(ctx, t, store, ws, "wfc_deps_fan_a", nil)
	finishWFTask(ctx, t, store, ws, "wfc_deps_fan_b", nil)
	promoteUnblocked(ctx, t, ws)
	killWFTask(ctx, t, store, "wfc_deps_sink")
	applyPolicyFor(ctx, t, ws, id)
	is.NoError(ws.CompensateDAG(ctx, id))

	deps, err = ws.DAGDeps(ctx, id)
	is.NoError(err)
	is.Len(deps, 4, "compensating a run with one compensable task adds no edge between comps")
	is.Equal("wfc_deps_undo_root", wfTaskByKey(ctx, t, ws, id, "comp:root").Kind,
		"the compensation task itself exists and is reachable by key")
}

// ---- DAGNameStateCounts ---------------------------------------------------

// runDAGStateCounts pins the histogram behind both the definition navigator
// and the state tabs. Per-definition counts are asserted as absolutes (the
// names are unique to this subtest); anything global is asserted as a DELTA,
// because the suite shares one store and totals belong to no single subtest.
func runDAGStateCounts(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	globalRunning := func(m map[string]map[driver.DAGState]int64, state driver.DAGState) int64 {
		var total int64
		for _, counts := range m {
			total += counts[state]
		}
		return total
	}

	before, err := ws.DAGNameStateCounts(ctx)
	is.NoError(err)
	is.NotContains(before, "wfc_counts_live", "a definition with no runs is absent, not zero-valued")

	live := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_counts_live", Tasks: []driver.DAGTask{wfTask("a", "wfc_counts_live_a")},
	})
	// Two runs of one definition: the navigator has to count runs per name, not
	// report the name once.
	createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_counts_live", Tasks: []driver.DAGTask{wfTask("a", "wfc_counts_live_b")},
	})
	done := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_counts_done", Tasks: []driver.DAGTask{wfTask("a", "wfc_counts_done_a")},
	})
	finishWFTask(ctx, t, store, ws, "wfc_counts_done_a", nil)
	completeDAGs(ctx, t, ws)
	is.Equal(driver.DAGSucceeded, wfView(ctx, t, ws, done).State)

	after, err := ws.DAGNameStateCounts(ctx)
	is.NoError(err)
	is.Equal(int64(2), after["wfc_counts_live"][driver.DAGRunning], "both runs of the definition count")
	is.Equal(int64(1), after["wfc_counts_done"][driver.DAGSucceeded])
	is.Equal(int64(2),
		globalRunning(after, driver.DAGRunning)-globalRunning(before, driver.DAGRunning),
		"summing the inner maps gives the global per-state count")

	// A state change is reflected, not just an insert — and it moves within the
	// definition, so a navigator's per-name numbers stay honest.
	is.NoError(ws.CancelDAG(ctx, live))
	final, err := ws.DAGNameStateCounts(ctx)
	is.NoError(err)
	is.Equal(int64(1), final["wfc_counts_live"][driver.DAGRunning], "the cancelled run left running")
	is.Equal(int64(1), final["wfc_counts_live"][driver.DAGCancelled])
}

// ---- DAGTaskCounts --------------------------------------------------------

func runDAGTaskCounts(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	first := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_tcounts_a",
		Tasks: []driver.DAGTask{
			wfTask("a", "wfc_tcounts_a_a"),
			wfTask("b", "wfc_tcounts_a_b"),
			wfTask("c", "wfc_tcounts_a_c"),
		},
		Deps: []driver.DAGDep{{TaskKey: "c", DependsOnKey: "a"}},
	})
	second := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_tcounts_b", Tasks: []driver.DAGTask{wfTask("only", "wfc_tcounts_b_only")},
	})

	counts, err := ws.DAGTaskCounts(ctx, []uuid.UUID{first, second})
	is.NoError(err)
	is.Equal(map[driver.JobState]int64{driver.StatePending: 2, driver.StateBlocked: 1}, counts[first])
	is.Equal(map[driver.JobState]int64{driver.StatePending: 1}, counts[second])

	// The breakdown follows the tasks, so a listing shows real progress.
	finishWFTask(ctx, t, store, ws, "wfc_tcounts_a_a", nil)
	counts, err = ws.DAGTaskCounts(ctx, []uuid.UUID{first})
	is.NoError(err)
	is.Equal(map[driver.JobState]int64{
		driver.StatePending: 1, driver.StateBlocked: 1, driver.StateSucceeded: 1,
	}, counts[first])

	// Unknown ids are absent rather than zero-valued, and an empty request is
	// not a query.
	counts, err = ws.DAGTaskCounts(ctx, []uuid.UUID{uuid.New()})
	is.NoError(err)
	is.Empty(counts)
	counts, err = ws.DAGTaskCounts(ctx, nil)
	is.NoError(err)
	is.Empty(counts)
}

// ---- started_at -----------------------------------------------------------

// runDAGStartedAt pins the stamp an admin surface reports execution time from.
// It must move with the attempt, not with the row: a retried task that reused
// its first stamp would report a duration spanning the backoff.
func runDAGStartedAt(t *testing.T, store driver.Store, ws driver.DAGStore) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	task := wfTask("a", "wfc_started_a")
	task.MaxAttempts = 3
	id := createWF(ctx, t, ws, driver.DAGParams{
		Name: "wfc_started", Tasks: []driver.DAGTask{task},
	})

	is.True(wfTaskByKey(ctx, t, ws, id, "a").StartedAt.IsZero(), "a task that never ran has no start")

	leased := dequeueN(ctx, t, store, driver.SourceDAG, "wfc_started_a", 1, time.Minute)
	is.Len(leased, 1)
	first := wfTaskByKey(ctx, t, ws, id, "a").StartedAt
	is.False(first.IsZero(), "the lease stamps the attempt's start")

	// Fail it and lease again: the second attempt overwrites the stamp, so
	// completed_at - started_at never spans a retry backoff.
	is.NoError(store.Reschedule(ctx, leased[0].ID, leased[0].LeaseToken, 0, "boom"))
	_, err := store.PromoteDue(ctx, driver.SourceDAG, []string{"wfc_started_a"})
	is.NoError(err)
	again := dequeueN(ctx, t, store, driver.SourceDAG, "wfc_started_a", 1, time.Minute)
	is.Len(again, 1)
	second := wfTaskByKey(ctx, t, ws, id, "a")
	is.Equal(2, second.Attempt)
	is.False(second.StartedAt.Before(first), "started_at moves forward with the attempt")

	is.NoError(store.Dead(ctx, again[0].ID, again[0].LeaseToken, "boom"))
	is.NoError(ws.RetryDAG(ctx, id))
	retried := wfTaskByKey(ctx, t, ws, id, "a")
	is.Equal(0, retried.Attempt)
	is.True(retried.StartedAt.IsZero(),
		"a retry clears the stamp with the attempt: nothing has started yet")
}
