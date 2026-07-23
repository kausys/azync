package drivertest_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The workflow tests drive the Fake through the driver.WorkflowStore capability
// with a manual clock, covering the full scheduler state machine the fake
// freezes as the oracle for the workflow runtime.

var testStart = time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

func newWorkflowFake(t *testing.T) (*drivertest.Fake, *drivertest.ManualClock) {
	t.Helper()
	f := drivertest.NewFake()
	clk := drivertest.NewManualClock(testStart)
	f.Clock = clk
	return f, clk
}

// task builds a WorkflowTask with common defaults.
func task(key, kind string) driver.WorkflowTask {
	return driver.WorkflowTask{Key: key, Kind: kind, Payload: json.RawMessage(`{}`), MaxAttempts: 3}
}

func createWorkflow(t *testing.T, f *drivertest.Fake, p driver.WorkflowParams) uuid.UUID {
	t.Helper()
	is := require.New(t)
	if p.ID == uuid.Nil {
		p.ID = uuid.New()
	}
	inserted, _, err := f.CreateWorkflow(context.Background(), p)
	is.NoError(err)
	is.True(inserted)
	return p.ID
}

// taskByKey finds one task job of the workflow through the contract surface.
func taskByKey(t *testing.T, f *drivertest.Fake, wfID uuid.UUID, key string) driver.Job {
	t.Helper()
	is := require.New(t)
	tasks, err := f.WorkflowTasks(context.Background(), wfID)
	is.NoError(err)
	for _, j := range tasks {
		if j.TaskKey == key {
			return j
		}
	}
	t.Fatalf("task %q not found in workflow %s", key, wfID)
	return driver.Job{}
}

// leaseKind leases exactly one due pending workflow task of the kind.
func leaseKind(t *testing.T, f *drivertest.Fake, kind string) driver.Job {
	t.Helper()
	is := require.New(t)
	jobs, err := f.DequeueBatch(context.Background(), driver.SourceWorkflow, driver.DequeueParams{
		Kind: kind, Limit: 1, Lease: time.Minute,
	})
	is.NoError(err)
	is.Len(jobs, 1, "expected one due pending task of kind %q", kind)
	return jobs[0]
}

// finishKind leases one task of the kind and acks it with the given result.
func finishKind(t *testing.T, f *drivertest.Fake, kind string, result json.RawMessage) driver.Job {
	t.Helper()
	is := require.New(t)
	j := leaseKind(t, f, kind)
	is.NoError(f.AckTaskResult(context.Background(), j.ID, j.LeaseToken, result))
	return j
}

// killKind leases one task of the kind and dead-letters it.
func killKind(t *testing.T, f *drivertest.Fake, kind string) driver.Job {
	t.Helper()
	is := require.New(t)
	j := leaseKind(t, f, kind)
	is.NoError(f.Dead(context.Background(), j.ID, j.LeaseToken, "boom"))
	return j
}

func getWorkflow(t *testing.T, f *drivertest.Fake, id uuid.UUID) driver.WorkflowView {
	t.Helper()
	is := require.New(t)
	w, err := f.GetWorkflow(context.Background(), id)
	is.NoError(err)
	return *w
}

// ---- creation -------------------------------------------------------------

func TestWorkflowCreateInitialStates(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, clk := newWorkflowFake(t)
	ctx := context.Background()

	id := createWorkflow(t, f, driver.WorkflowParams{
		Name: "wf-init", OnFailure: driver.OnFailureCancel,
		Meta: map[string]string{"tenant": "t1"},
		Tasks: []driver.WorkflowTask{
			task("a", "init_a"),
			task("b", "init_b"),
			{Key: "s", Kind: driver.KindSleep, SleepFor: 10 * time.Minute},
			{Key: "g", Kind: driver.KindSignal, SignalName: "go"},
			task("c", "init_c"),
		},
		Deps: []driver.WorkflowDep{
			{TaskKey: "b", DependsOnKey: "a"},
			{TaskKey: "c", DependsOnKey: "s"},
		},
	})

	is.Equal(driver.StatePending, taskByKey(t, f, id, "a").State, "a dependency-free task is pending")
	is.Equal(driver.StateBlocked, taskByKey(t, f, id, "b").State, "a task with deps is blocked")
	s := taskByKey(t, f, id, "s")
	is.Equal(driver.StateScheduled, s.State, "a root $sleep is scheduled")
	is.Equal(clk.Now().Add(10*time.Minute), s.RunAt, "the sleep timer starts at creation for a root sleep")
	is.Equal(driver.StateWaiting, taskByKey(t, f, id, "g").State, "a root $signal waits")
	is.Equal(driver.StateBlocked, taskByKey(t, f, id, "c").State)

	w := getWorkflow(t, f, id)
	is.Equal(driver.WorkflowRunning, w.State)
	is.Equal(driver.OnFailureCancel, w.OnFailure)
	is.Equal(map[string]string{"tenant": "t1"}, w.Meta)
	is.Equal(clk.Now(), w.CreatedAt)
	is.True(w.CompletedAt.IsZero())

	// Task jobs carry the workflow linkage and annotations.
	a := taskByKey(t, f, id, "a")
	is.Equal(id, a.WorkflowID)
	is.Equal(driver.SourceWorkflow, a.Source)
	is.Equal(map[string]string{"tenant": "t1"}, a.Meta)

	_ = ctx
}

func TestWorkflowCreateDuplicateTaskKeyErrors(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, _ := newWorkflowFake(t)

	_, _, err := f.CreateWorkflow(context.Background(), driver.WorkflowParams{
		ID: uuid.New(), Name: "wf-dup",
		Tasks: []driver.WorkflowTask{task("a", "dup_a"), task("a", "dup_b")},
	})
	is.Error(err, "task keys are unique per workflow")
}

func TestWorkflowCreateDedupeLiveOnly(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, _ := newWorkflowFake(t)
	ctx := context.Background()

	first := createWorkflow(t, f, driver.WorkflowParams{
		Name: "wf-dedupe", IdempotencyKey: "k1",
		Tasks: []driver.WorkflowTask{task("a", "dd_a")},
	})

	// A live execution holds the key.
	inserted, existing, err := f.CreateWorkflow(ctx, driver.WorkflowParams{
		ID: uuid.New(), Name: "wf-dedupe", IdempotencyKey: "k1",
		Tasks: []driver.WorkflowTask{task("a", "dd_a")},
	})
	is.NoError(err)
	is.False(inserted)
	is.Equal(first, existing, "the live execution's id is returned")

	// A different name does not collide.
	inserted, _, err = f.CreateWorkflow(ctx, driver.WorkflowParams{
		ID: uuid.New(), Name: "wf-dedupe-other", IdempotencyKey: "k1",
		Tasks: []driver.WorkflowTask{task("a", "dd_other")},
	})
	is.NoError(err)
	is.True(inserted, "dedupe scopes to (name, key)")

	// Completing the first execution frees the key.
	finishKind(t, f, "dd_a", nil)
	n, err := f.CompleteWorkflows(ctx)
	is.NoError(err)
	is.GreaterOrEqual(n, int64(1))
	is.Equal(driver.WorkflowSucceeded, getWorkflow(t, f, first).State)

	inserted, _, err = f.CreateWorkflow(ctx, driver.WorkflowParams{
		ID: uuid.New(), Name: "wf-dedupe", IdempotencyKey: "k1",
		Tasks: []driver.WorkflowTask{task("a", "dd_a")},
	})
	is.NoError(err)
	is.True(inserted, "a terminal workflow frees the idempotency key")
}

// ---- promotion ------------------------------------------------------------

func TestWorkflowPromoteUnblockedCascadeFanOutFanIn(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, _ := newWorkflowFake(t)
	ctx := context.Background()

	id := createWorkflow(t, f, driver.WorkflowParams{
		Name: "wf-cascade",
		Tasks: []driver.WorkflowTask{
			task("a", "cas_a"), task("b", "cas_b"), task("c", "cas_c"), task("d", "cas_d"),
		},
		Deps: []driver.WorkflowDep{
			{TaskKey: "b", DependsOnKey: "a"},
			{TaskKey: "c", DependsOnKey: "a"},
			{TaskKey: "d", DependsOnKey: "b"},
			{TaskKey: "d", DependsOnKey: "c"},
		},
	})

	finishKind(t, f, "cas_a", nil)
	n, err := f.PromoteUnblocked(ctx)
	is.NoError(err)
	is.Equal(int64(2), n, "the fan-out promotes b and c")
	is.Equal(driver.StatePending, taskByKey(t, f, id, "b").State)
	is.Equal(driver.StatePending, taskByKey(t, f, id, "c").State)
	is.Equal(driver.StateBlocked, taskByKey(t, f, id, "d").State)

	finishKind(t, f, "cas_b", nil)
	n, err = f.PromoteUnblocked(ctx)
	is.NoError(err)
	is.Zero(n, "the fan-in still waits for c")
	is.Equal(driver.StateBlocked, taskByKey(t, f, id, "d").State)

	finishKind(t, f, "cas_c", nil)
	n, err = f.PromoteUnblocked(ctx)
	is.NoError(err)
	is.Equal(int64(1), n)
	is.Equal(driver.StatePending, taskByKey(t, f, id, "d").State, "the fan-in promotes once every dep succeeded")
}

func TestWorkflowPromoteUnblockedInternalKindCase(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, clk := newWorkflowFake(t)
	ctx := context.Background()

	id := createWorkflow(t, f, driver.WorkflowParams{
		Name: "wf-case",
		Tasks: []driver.WorkflowTask{
			task("x", "case_x"),
			{Key: "s", Kind: driver.KindSleep, SleepFor: 5 * time.Minute},
			{Key: "g", Kind: driver.KindSignal, SignalName: "go"},
		},
		Deps: []driver.WorkflowDep{
			{TaskKey: "s", DependsOnKey: "x"},
			{TaskKey: "g", DependsOnKey: "x"},
		},
	})

	finishKind(t, f, "case_x", nil)
	clk.Advance(time.Minute)
	n, err := f.PromoteUnblocked(ctx)
	is.NoError(err)
	is.Equal(int64(2), n)
	s := taskByKey(t, f, id, "s")
	is.Equal(driver.StateScheduled, s.State, "an unblocked $sleep is scheduled")
	is.Equal(clk.Now().Add(5*time.Minute), s.RunAt, "the sleep timer starts at promotion")
	is.Equal(driver.StateWaiting, taskByKey(t, f, id, "g").State, "an unblocked $signal waits")
}

func TestWorkflowPromoteUnblockedIgnoreDeadDeps(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, _ := newWorkflowFake(t)
	ctx := context.Background()

	ignore := task("b", "idd_b")
	ignore.IgnoreDeadDeps = true
	id := createWorkflow(t, f, driver.WorkflowParams{
		Name: "wf-idd",
		Tasks: []driver.WorkflowTask{
			{Key: "a", Kind: "idd_a", Payload: json.RawMessage(`{}`), MaxAttempts: 1},
			ignore,
			task("c", "idd_c"),
		},
		Deps: []driver.WorkflowDep{
			{TaskKey: "b", DependsOnKey: "a"},
			{TaskKey: "c", DependsOnKey: "a"},
		},
	})

	killKind(t, f, "idd_a")
	n, err := f.PromoteUnblocked(ctx)
	is.NoError(err)
	is.Equal(int64(1), n)
	is.Equal(driver.StatePending, taskByKey(t, f, id, "b").State, "IgnoreDeadDeps treats the dead dep as satisfied")
	is.Equal(driver.StateBlocked, taskByKey(t, f, id, "c").State, "a dead dep never promotes without IgnoreDeadDeps")
}

// ---- sleeps and signals ---------------------------------------------------

func TestWorkflowCompleteDueSleeps(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, clk := newWorkflowFake(t)
	ctx := context.Background()

	id := createWorkflow(t, f, driver.WorkflowParams{
		Name:  "wf-sleep",
		Tasks: []driver.WorkflowTask{{Key: "s", Kind: driver.KindSleep, SleepFor: 10 * time.Minute}},
	})

	n, err := f.CompleteDueSleeps(ctx)
	is.NoError(err)
	is.Zero(n, "an unexpired sleep is left scheduled")

	clk.Advance(10 * time.Minute)
	n, err = f.CompleteDueSleeps(ctx)
	is.NoError(err)
	is.Equal(int64(1), n)
	s := taskByKey(t, f, id, "s")
	is.Equal(driver.StateSucceeded, s.State, "a due sleep completes without any handler")
	is.Equal(clk.Now(), s.CompletedAt)
}

func TestWorkflowSignalCompletesWaitingAndAdvancesSleep(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, clk := newWorkflowFake(t)
	ctx := context.Background()

	id := createWorkflow(t, f, driver.WorkflowParams{
		Name: "wf-signal",
		Tasks: []driver.WorkflowTask{
			{Key: "g", Kind: driver.KindSignal, SignalName: "approved"},
			{Key: "s", Kind: driver.KindSleep, SleepFor: time.Hour, SignalName: "approved"},
		},
	})

	matched, err := f.Signal(ctx, id, "other", json.RawMessage(`{}`))
	is.NoError(err)
	is.Zero(matched, "an unmatched signal name touches nothing")

	payload := json.RawMessage(`{"by":"ops"}`)
	matched, err = f.Signal(ctx, id, "approved", payload)
	is.NoError(err)
	is.Equal(int64(2), matched, "the waiting $signal and the named $sleep both match")

	g := taskByKey(t, f, id, "g")
	is.Equal(driver.StateSucceeded, g.State)
	is.JSONEq(string(payload), string(g.Result), "the signal payload is the task's result")

	s := taskByKey(t, f, id, "s")
	is.Equal(driver.StateScheduled, s.State)
	is.Equal(clk.Now(), s.RunAt, "the signal wakes the sleep early")
	n, err := f.CompleteDueSleeps(ctx)
	is.NoError(err)
	is.Equal(int64(1), n)

	matched, err = f.Signal(ctx, id, "approved", payload)
	is.NoError(err)
	is.Zero(matched, "a consumed signal has nothing left to match")

	results, err := f.TaskResults(ctx, id, []string{"g"})
	is.NoError(err)
	is.JSONEq(string(payload), string(results["g"]))
}

// ---- failure policies -----------------------------------------------------

// buildFailedCancelWorkflow creates a workflow with two compensable succeeded
// tasks (a then b, in that completion order), one dead task c and one blocked
// dependent d, then applies the cancel policy.
func buildFailedCancelWorkflow(t *testing.T, f *drivertest.Fake, clk *drivertest.ManualClock, name string) uuid.UUID {
	t.Helper()
	is := require.New(t)
	ctx := context.Background()

	a := task("a", name+"_a")
	a.CompensationKind = name + "_undo_a"
	a.CompensationPayload = json.RawMessage(`{"undo":"a"}`)
	b := task("b", name+"_b")
	b.CompensationKind = name + "_undo_b"
	b.CompensationPayload = json.RawMessage(`{"undo":"b"}`)
	c := task("c", name+"_c")
	c.MaxAttempts = 1
	id := createWorkflow(t, f, driver.WorkflowParams{
		Name: name, OnFailure: driver.OnFailureCancel,
		Tasks: []driver.WorkflowTask{a, b, c, task("d", name+"_d")},
		Deps:  []driver.WorkflowDep{{TaskKey: "d", DependsOnKey: "c"}},
	})

	finishKind(t, f, name+"_a", nil)
	clk.Advance(time.Second)
	finishKind(t, f, name+"_b", nil)
	clk.Advance(time.Second)
	killKind(t, f, name+"_c")

	failures, err := f.ApplyFailurePolicy(ctx)
	is.NoError(err)
	is.Len(failures, 1)
	is.Equal(id, failures[0].WorkflowID)
	is.Equal(driver.OnFailureCancel, failures[0].Policy)
	is.Equal([]string{"c"}, failures[0].DeadTasks)
	return id
}

func TestWorkflowFailurePolicyCancelCompensatesInReverseOrder(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, clk := newWorkflowFake(t)
	ctx := context.Background()

	id := buildFailedCancelWorkflow(t, f, clk, "fpc")

	w := getWorkflow(t, f, id)
	is.Equal(driver.WorkflowCompensating, w.State)
	is.Contains(w.FailureReason, "c", "the dead task is recorded")
	is.Equal(driver.StateCancelled, taskByKey(t, f, id, "d").State, "the blocked dependent is cancelled")

	// Reverse completion order: b finished last, so comp:b runs first.
	compB := taskByKey(t, f, id, "comp:b")
	is.Equal(driver.StatePending, compB.State)
	is.Equal("fpc_undo_b", compB.Kind)
	is.JSONEq(`{"undo":"b"}`, string(compB.Payload))
	compA := taskByKey(t, f, id, "comp:a")
	is.Equal(driver.StateBlocked, compA.State, "the older compensation waits for the newer one")
	is.Equal("fpc_undo_a", compA.Kind)

	// Run the chain: comp:b then comp:a, then the workflow settles failed.
	finishKind(t, f, "fpc_undo_b", nil)
	n, err := f.PromoteUnblocked(ctx)
	is.NoError(err)
	is.Equal(int64(1), n)
	finishKind(t, f, "fpc_undo_a", nil)
	n, err = f.CompleteWorkflows(ctx)
	is.NoError(err)
	is.Equal(int64(1), n)
	w = getWorkflow(t, f, id)
	is.Equal(driver.WorkflowFailed, w.State)
	is.False(w.CompletedAt.IsZero())
}

func TestWorkflowFailurePolicyCancelWithoutCompensationsFailsDirectly(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, _ := newWorkflowFake(t)
	ctx := context.Background()

	c := task("c", "fpn_c")
	c.MaxAttempts = 1
	id := createWorkflow(t, f, driver.WorkflowParams{
		Name: "wf-fpn", OnFailure: driver.OnFailureCancel,
		Tasks: []driver.WorkflowTask{c, task("d", "fpn_d")},
		Deps:  []driver.WorkflowDep{{TaskKey: "d", DependsOnKey: "c"}},
	})

	killKind(t, f, "fpn_c")
	failures, err := f.ApplyFailurePolicy(ctx)
	is.NoError(err)
	is.Len(failures, 1)
	w := getWorkflow(t, f, id)
	is.Equal(driver.WorkflowFailed, w.State, "nothing to compensate lands failed at once")
	is.False(w.CompletedAt.IsZero())
	is.Equal(driver.StateCancelled, taskByKey(t, f, id, "d").State)
}

func TestWorkflowFailurePolicySuspendAndRetry(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, _ := newWorkflowFake(t)
	ctx := context.Background()

	a := task("a", "fps_a")
	a.MaxAttempts = 1
	id := createWorkflow(t, f, driver.WorkflowParams{
		Name: "wf-fps", OnFailure: driver.OnFailureSuspend,
		Tasks: []driver.WorkflowTask{a, task("b", "fps_b")},
	})

	killKind(t, f, "fps_a")
	failures, err := f.ApplyFailurePolicy(ctx)
	is.NoError(err)
	is.Len(failures, 1)
	is.Equal(driver.OnFailureSuspend, failures[0].Policy)

	w := getWorkflow(t, f, id)
	is.Equal(driver.WorkflowSuspended, w.State)
	is.Contains(w.FailureReason, "a")
	is.Equal(driver.StatePending, taskByKey(t, f, id, "b").State, "suspend leaves the tasks untouched")

	// A second pass is idempotent: the workflow is no longer running.
	failures, err = f.ApplyFailurePolicy(ctx)
	is.NoError(err)
	is.Empty(failures)

	is.NoError(f.RetryWorkflow(ctx, id))
	w = getWorkflow(t, f, id)
	is.Equal(driver.WorkflowRunning, w.State)
	is.Empty(w.FailureReason)
	aJob := taskByKey(t, f, id, "a")
	is.Equal(driver.StatePending, aJob.State)
	is.Zero(aJob.Attempt, "retry grants a fresh budget")
	is.Zero(aJob.ReapCount)
}

func TestWorkflowFailurePolicySkipsDeadTaskIgnoredByAllDependents(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, _ := newWorkflowFake(t)
	ctx := context.Background()

	a := task("a", "fpi_a")
	a.MaxAttempts = 1
	b := task("b", "fpi_b")
	b.IgnoreDeadDeps = true
	id := createWorkflow(t, f, driver.WorkflowParams{
		Name: "wf-fpi", OnFailure: driver.OnFailureCancel,
		Tasks: []driver.WorkflowTask{a, b},
		Deps:  []driver.WorkflowDep{{TaskKey: "b", DependsOnKey: "a"}},
	})

	killKind(t, f, "fpi_a")
	failures, err := f.ApplyFailurePolicy(ctx)
	is.NoError(err)
	is.Empty(failures, "a dead task every dependent ignores does not trigger the policy")
	is.Equal(driver.WorkflowRunning, getWorkflow(t, f, id).State)

	n, err := f.PromoteUnblocked(ctx)
	is.NoError(err)
	is.Equal(int64(1), n, "the ignoring dependent still runs")
}

// ---- completion -----------------------------------------------------------

func TestWorkflowCompleteWorkflowsSucceeded(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, clk := newWorkflowFake(t)
	ctx := context.Background()

	id := createWorkflow(t, f, driver.WorkflowParams{
		Name: "wf-done",
		Tasks: []driver.WorkflowTask{
			task("a", "done_a"),
			{Key: "s", Kind: driver.KindSleep, SleepFor: time.Minute},
		},
	})

	n, err := f.CompleteWorkflows(ctx)
	is.NoError(err)
	is.Zero(n, "an unfinished workflow is left alone")

	finishKind(t, f, "done_a", nil)
	clk.Advance(time.Minute)
	_, err = f.CompleteDueSleeps(ctx)
	is.NoError(err)

	n, err = f.CompleteWorkflows(ctx)
	is.NoError(err)
	is.Equal(int64(1), n)
	w := getWorkflow(t, f, id)
	is.Equal(driver.WorkflowSucceeded, w.State)
	is.Equal(clk.Now(), w.CompletedAt)
}

func TestWorkflowCompensationDeadSuspendsThenRetryResumesCompensating(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, clk := newWorkflowFake(t)
	ctx := context.Background()

	id := buildFailedCancelWorkflow(t, f, clk, "cds")

	// The first compensation dies: the workflow suspends for a manual call.
	comp := leaseKind(t, f, "cds_undo_b")
	is.NoError(f.Dead(ctx, comp.ID, comp.LeaseToken, "undo failed"))
	n, err := f.CompleteWorkflows(ctx)
	is.NoError(err)
	is.Equal(int64(1), n)
	is.Equal(driver.WorkflowSuspended, getWorkflow(t, f, id).State)

	// Retry resumes the compensation, never the original tasks.
	is.NoError(f.RetryWorkflow(ctx, id))
	w := getWorkflow(t, f, id)
	is.Equal(driver.WorkflowCompensating, w.State, "a workflow with compensations resumes compensating")
	is.Equal(driver.StatePending, taskByKey(t, f, id, "comp:b").State)
	is.Equal(driver.StateDead, taskByKey(t, f, id, "c").State, "the original dead task is not resurrected")

	// Now the chain finishes and the workflow settles failed.
	finishKind(t, f, "cds_undo_b", nil)
	_, err = f.PromoteUnblocked(ctx)
	is.NoError(err)
	finishKind(t, f, "cds_undo_a", nil)
	n, err = f.CompleteWorkflows(ctx)
	is.NoError(err)
	is.Equal(int64(1), n)
	is.Equal(driver.WorkflowFailed, getWorkflow(t, f, id).State)
}

// ---- manager verbs --------------------------------------------------------

func TestWorkflowCompensateManual(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, _ := newWorkflowFake(t)
	ctx := context.Background()

	a := task("a", "cm_a")
	a.CompensationKind = "cm_undo_a"
	a.CompensationPayload = json.RawMessage(`{"undo":"a"}`)
	id := createWorkflow(t, f, driver.WorkflowParams{
		Name:  "wf-cm",
		Tasks: []driver.WorkflowTask{a, task("b", "cm_b")},
		Deps:  []driver.WorkflowDep{{TaskKey: "b", DependsOnKey: "a"}},
	})

	finishKind(t, f, "cm_a", nil)
	_, err := f.PromoteUnblocked(ctx)
	is.NoError(err)

	is.NoError(f.CompensateWorkflow(ctx, id))
	w := getWorkflow(t, f, id)
	is.Equal(driver.WorkflowCompensating, w.State)
	is.Equal(driver.StateCancelled, taskByKey(t, f, id, "b").State)
	is.Equal(driver.StatePending, taskByKey(t, f, id, "comp:a").State)

	finishKind(t, f, "cm_undo_a", nil)
	_, err = f.CompleteWorkflows(ctx)
	is.NoError(err)
	is.Equal(driver.WorkflowFailed, getWorkflow(t, f, id).State)

	is.True(driver.IsNotFound(f.CompensateWorkflow(ctx, id)), "a terminal workflow cannot be compensated")
	is.True(driver.IsNotFound(f.CompensateWorkflow(ctx, uuid.New())))
}

func TestWorkflowCancel(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, _ := newWorkflowFake(t)
	ctx := context.Background()

	a := task("a", "cn_a")
	a.CompensationKind = "cn_undo_a"
	id := createWorkflow(t, f, driver.WorkflowParams{
		Name:  "wf-cn",
		Tasks: []driver.WorkflowTask{a, task("b", "cn_b"), task("c", "cn_c")},
		Deps:  []driver.WorkflowDep{{TaskKey: "c", DependsOnKey: "b"}},
	})

	finishKind(t, f, "cn_a", nil)
	active := leaseKind(t, f, "cn_b") // b is active while we cancel

	is.NoError(f.CancelWorkflow(ctx, id))
	w := getWorkflow(t, f, id)
	is.Equal(driver.WorkflowCancelled, w.State)
	is.False(w.CompletedAt.IsZero())
	is.Equal(driver.StateCancelled, taskByKey(t, f, id, "c").State)
	is.Equal(driver.StateActive, taskByKey(t, f, id, "b").State, "an active task is left to settle")
	is.Equal(driver.StateSucceeded, taskByKey(t, f, id, "a").State, "a succeeded task is untouched")
	tasks, err := f.WorkflowTasks(ctx, id)
	is.NoError(err)
	is.Len(tasks, 3, "cancel inserts no compensations")

	is.NoError(f.Ack(ctx, active.ID, active.LeaseToken), "the in-flight lease still settles")
	is.True(driver.IsNotFound(f.CancelWorkflow(ctx, id)), "a terminal workflow cannot be cancelled again")
	is.True(driver.IsNotFound(f.CancelWorkflow(ctx, uuid.New())))
}

func TestWorkflowCancelDuringCompensatingLandsCancelled(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, clk := newWorkflowFake(t)
	ctx := context.Background()

	id := buildFailedCancelWorkflow(t, f, clk, "cnc")

	// Cancelling a compensating workflow aborts the not-yet-started
	// compensations and lets CompleteWorkflows settle the origin as cancelled.
	is.NoError(f.CancelWorkflow(ctx, id))
	w := getWorkflow(t, f, id)
	is.Equal(driver.WorkflowCompensating, w.State, "the in-flight compensation settles before the workflow lands")
	is.Equal(driver.StateCancelled, taskByKey(t, f, id, "comp:b").State)
	is.Equal(driver.StateCancelled, taskByKey(t, f, id, "comp:a").State)

	n, err := f.CompleteWorkflows(ctx)
	is.NoError(err)
	is.Equal(int64(1), n)
	is.Equal(driver.WorkflowCancelled, getWorkflow(t, f, id).State, "a compensation triggered through cancel lands cancelled, not failed")
}

func TestWorkflowRetryNotFoundForMissingOrTerminal(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, _ := newWorkflowFake(t)
	ctx := context.Background()

	is.True(driver.IsNotFound(f.RetryWorkflow(ctx, uuid.New())))

	id := createWorkflow(t, f, driver.WorkflowParams{
		Name:  "wf-retry-term",
		Tasks: []driver.WorkflowTask{task("a", "rt_a")},
	})
	finishKind(t, f, "rt_a", nil)
	_, err := f.CompleteWorkflows(ctx)
	is.NoError(err)
	is.True(driver.IsNotFound(f.RetryWorkflow(ctx, id)))
}

// ---- results and fencing --------------------------------------------------

func TestWorkflowAckTaskResultFencingAndTaskResults(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, _ := newWorkflowFake(t)
	ctx := context.Background()

	id := createWorkflow(t, f, driver.WorkflowParams{
		Name:  "wf-res",
		Tasks: []driver.WorkflowTask{task("a", "res_a"), task("b", "res_b")},
	})

	j := leaseKind(t, f, "res_a")
	is.True(driver.IsNotFound(f.AckTaskResult(ctx, j.ID, uuid.New(), json.RawMessage(`{}`))),
		"a stale token is fenced")

	is.NoError(f.AckTaskResult(ctx, j.ID, j.LeaseToken, json.RawMessage(`{"n":7}`)))
	a := taskByKey(t, f, id, "a")
	is.Equal(driver.StateSucceeded, a.State)
	is.JSONEq(`{"n":7}`, string(a.Result))

	results, err := f.TaskResults(ctx, id, []string{"a", "b"})
	is.NoError(err)
	is.Len(results, 1, "an unfinished task has no result entry")
	is.JSONEq(`{"n":7}`, string(results["a"]))

	// An empty key set returns every succeeded task.
	finishKind(t, f, "res_b", nil)
	results, err = f.TaskResults(ctx, id, nil)
	is.NoError(err)
	is.Len(results, 2)
	is.Nil(results["b"], "a succeeded task without a result maps to nil")
}

// ---- internal kinds stay out of PromoteDue --------------------------------

func TestWorkflowInternalKindsNeverPromotedByPromoteDue(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, clk := newWorkflowFake(t)
	ctx := context.Background()

	id := createWorkflow(t, f, driver.WorkflowParams{
		Name: "wf-internal",
		Tasks: []driver.WorkflowTask{
			task("a", "int_a"),
			{Key: "s", Kind: driver.KindSleep, SleepFor: time.Minute},
		},
	})

	clk.Advance(time.Hour) // the sleep is long overdue
	promoted, err := f.PromoteDue(ctx, driver.SourceWorkflow, []string{"int_a"})
	is.NoError(err)
	is.Zero(promoted, "only registered kinds are promoted, and $sleep is never registered")
	is.Equal(driver.StateScheduled, taskByKey(t, f, id, "s").State,
		"the scheduler, not PromoteDue, resolves internal kinds")
}

// ---- admin ----------------------------------------------------------------

func TestWorkflowAdminSurface(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, clk := newWorkflowFake(t)
	ctx := context.Background()

	_, err := f.GetWorkflow(ctx, uuid.New())
	is.True(driver.IsNotFound(err))
	_, err = f.WorkflowTasks(ctx, uuid.New())
	is.True(driver.IsNotFound(err))

	first := createWorkflow(t, f, driver.WorkflowParams{
		Name:  "wf-admin",
		Tasks: []driver.WorkflowTask{task("a", "adm_a"), task("b", "adm_b")},
		Deps:  []driver.WorkflowDep{{TaskKey: "b", DependsOnKey: "a"}},
	})
	clk.Advance(time.Second)
	second := createWorkflow(t, f, driver.WorkflowParams{
		Name:  "wf-admin",
		Tasks: []driver.WorkflowTask{task("a", "adm_a2")},
	})
	clk.Advance(time.Second)
	other := createWorkflow(t, f, driver.WorkflowParams{
		Name:  "wf-admin-other",
		Tasks: []driver.WorkflowTask{task("a", "adm_a3")},
	})

	// Newest first, filters, totals.
	views, total, err := f.ListWorkflows(ctx, driver.WorkflowFilter{}, 0, 0)
	is.NoError(err)
	is.Equal(int64(3), total)
	is.Equal([]uuid.UUID{other, second, first}, []uuid.UUID{views[0].ID, views[1].ID, views[2].ID})

	views, total, err = f.ListWorkflows(ctx, driver.WorkflowFilter{Name: "wf-admin"}, 0, 1)
	is.NoError(err)
	is.Equal(int64(2), total)
	is.Len(views, 1, "limit bounds the page")
	is.Equal(second, views[0].ID)

	views, total, err = f.ListWorkflows(ctx, driver.WorkflowFilter{State: driver.WorkflowRunning}, 1, 0)
	is.NoError(err)
	is.Equal(int64(3), total)
	is.Len(views, 2, "offset skips")

	// Tasks come back in creation order.
	tasks, err := f.WorkflowTasks(ctx, first)
	is.NoError(err)
	is.Equal([]string{"a", "b"}, []string{tasks[0].TaskKey, tasks[1].TaskKey})
}

func TestWorkflowVacuum(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f, clk := newWorkflowFake(t)
	ctx := context.Background()

	done := createWorkflow(t, f, driver.WorkflowParams{
		Name:  "wf-vac-done",
		Tasks: []driver.WorkflowTask{task("a", "vac_a")},
	})
	live := createWorkflow(t, f, driver.WorkflowParams{
		Name:  "wf-vac-live",
		Tasks: []driver.WorkflowTask{task("a", "vac_b")},
	})
	doneTask := taskByKey(t, f, done, "a")

	finishKind(t, f, "vac_a", nil)
	_, err := f.CompleteWorkflows(ctx)
	is.NoError(err)

	removed, err := f.VacuumWorkflows(ctx, 0)
	is.NoError(err)
	is.Zero(removed, "a non-positive retention retains everything")

	clk.Advance(48 * time.Hour)
	removed, err = f.VacuumWorkflows(ctx, 24*time.Hour)
	is.NoError(err)
	is.Equal(int64(1), removed)
	_, err = f.GetWorkflow(ctx, done)
	is.True(driver.IsNotFound(err), "the terminal workflow is gone")
	_, err = f.GetJob(ctx, driver.SourceWorkflow, doneTask.ID)
	is.True(driver.IsNotFound(err), "its task jobs cascade")
	_, err = f.GetWorkflow(ctx, live)
	is.NoError(err, "a live workflow survives any retention")
}
