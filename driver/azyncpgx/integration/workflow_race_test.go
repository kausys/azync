package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// These tests pin the manager verbs and the failure policy against concurrent
// workflow transitions on a live PostgreSQL. Each one opens its own transaction
// that performs (and holds uncommitted) the transition another scheduler pass or
// operator would make, races the verb under test against it, and asserts the
// loser observes the committed state instead of overwriting it — the same
// serialization the in-memory fake gets for free from its mutex.

// raceTask builds one plain workflow task for the race fixtures.
func raceTask(key, kind string, maxAttempts int) driver.WorkflowTask {
	return driver.WorkflowTask{Key: key, Kind: kind, Payload: json.RawMessage(`{}`), MaxAttempts: maxAttempts}
}

// raceStore casts the harness Store to its workflow capability.
func raceStore(t *testing.T, h *harness) driver.WorkflowStore {
	t.Helper()
	ws, ok := h.core.Store().(driver.WorkflowStore)
	require.True(t, ok, "the pgx store implements driver.WorkflowStore")
	return ws
}

// settleTask leases the one due pending task of the kind and settles it:
// succeeded (dead=false) or dead-lettered (dead=true).
func settleTask(ctx context.Context, t *testing.T, h *harness, kind string, dead bool) {
	t.Helper()
	is := require.New(t)
	jobs, err := h.core.Store().DequeueBatch(ctx, driver.SourceWorkflow, driver.DequeueParams{
		Kind: kind, Limit: 1, Lease: time.Minute,
	})
	is.NoError(err)
	is.Len(jobs, 1, "expected one due pending task of kind %q", kind)
	if dead {
		is.NoError(h.core.Store().Dead(ctx, jobs[0].ID, jobs[0].LeaseToken, "boom"))
	} else {
		is.NoError(raceStore(t, h).AckTaskResult(ctx, jobs[0].ID, jobs[0].LeaseToken, nil))
	}
}

// beginCompensateTx opens a transaction that performs the cancel policy's
// transition on the workflow — header locked, comp:<key> task inserted, state
// moved to compensating — and leaves it uncommitted so the caller controls when
// the racing verb observes it.
func beginCompensateTx(ctx context.Context, t *testing.T, pool *pgxpool.Pool, id uuid.UUID, compKey, compKind string) interface {
	Commit(context.Context) error
} {
	t.Helper()
	is := require.New(t)
	tx, err := pool.Begin(ctx)
	is.NoError(err)
	var state string
	is.NoError(tx.QueryRow(ctx, `SELECT state FROM azync_workflows WHERE id = $1 FOR UPDATE`, id).Scan(&state))
	is.Equal("running", state)
	_, err = tx.Exec(ctx, `
INSERT INTO azync_jobs (id, source, kind, state, run_at, max_attempts, payload, enqueued_at, workflow_id, task_key)
VALUES ($1, 'workflow', $2, 'pending', now(), 3, '{}'::jsonb, now(), $3, $4)`,
		uuid.New(), compKind, id, compKey)
	is.NoError(err)
	_, err = tx.Exec(ctx,
		`UPDATE azync_workflows SET state = 'compensating', failure_reason = 'dead tasks: c', updated_at = now() WHERE id = $1`, id)
	is.NoError(err)
	return tx
}

// TestCancelWorkflowLosingTheSettleRaceIsNotFound races CancelWorkflow against
// a CompleteWorkflows-style settle of the same workflow: the settle commits
// succeeded while the cancel is in flight. The cancel must observe the terminal
// state and report not-found — never flip a succeeded workflow to cancelled.
func TestCancelWorkflowLosingTheSettleRaceIsNotFound(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	ws := raceStore(t, h)
	pool := newPool(t, h.base, h.schema)
	ctx := context.Background()

	id := uuid.New()
	inserted, _, err := ws.CreateWorkflow(ctx, driver.WorkflowParams{
		ID: id, Name: "race-cancel",
		Tasks: []driver.WorkflowTask{raceTask("a", "race_cn_a", 3)},
	})
	is.NoError(err)
	is.True(inserted)
	settleTask(ctx, t, h, "race_cn_a", false)

	// The settle transaction holds the header lock with the workflow already
	// moved to succeeded, exactly as CompleteWorkflows does mid-transaction.
	tx, err := pool.Begin(ctx)
	is.NoError(err)
	_, err = tx.Exec(ctx,
		`UPDATE azync_workflows SET state = 'succeeded', completed_at = now(), updated_at = now() WHERE id = $1`, id)
	is.NoError(err)

	cancelErr := make(chan error, 1)
	go func() { cancelErr <- ws.CancelWorkflow(ctx, id) }()

	time.Sleep(300 * time.Millisecond) // let the cancel reach the header lock
	is.NoError(tx.Commit(ctx))

	select {
	case err := <-cancelErr:
		is.True(driver.IsNotFound(err), "a cancel that lost the settle race must be not-found, got %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("CancelWorkflow never returned")
	}
	view, err := ws.GetWorkflow(ctx, id)
	is.NoError(err)
	is.Equal(driver.WorkflowSucceeded, view.State, "the settled terminal state must stand")
}

// TestRetryWorkflowLosingThePolicyRaceResetsOnlyCompensations races
// RetryWorkflow against the cancel policy's transition on the same workflow:
// the policy commits the compensation chain while the retry is in flight. The
// retry must observe the compensating state and reset only dead compensation
// tasks — never resurrect the original dead task once compensation started.
func TestRetryWorkflowLosingThePolicyRaceResetsOnlyCompensations(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	ws := raceStore(t, h)
	pool := newPool(t, h.base, h.schema)
	ctx := context.Background()

	id := uuid.New()
	a := raceTask("a", "race_rt_a", 3)
	a.CompensationKind = "race_rt_undo_a"
	a.CompensationPayload = json.RawMessage(`{"undo":"a"}`)
	inserted, _, err := ws.CreateWorkflow(ctx, driver.WorkflowParams{
		ID: id, Name: "race-retry", OnFailure: driver.OnFailureCancel,
		Tasks: []driver.WorkflowTask{a, raceTask("c", "race_rt_c", 1)},
	})
	is.NoError(err)
	is.True(inserted)
	settleTask(ctx, t, h, "race_rt_a", false)
	settleTask(ctx, t, h, "race_rt_c", true) // c is dead; the policy will react

	tx := beginCompensateTx(ctx, t, pool, id, "comp:a", "race_rt_undo_a")

	retryErr := make(chan error, 1)
	go func() { retryErr <- ws.RetryWorkflow(ctx, id) }()

	time.Sleep(300 * time.Millisecond) // let the retry reach the header lock
	is.NoError(tx.Commit(ctx))

	select {
	case err := <-retryErr:
		is.NoError(err)
	case <-time.After(10 * time.Second):
		t.Fatal("RetryWorkflow never returned")
	}

	view, err := ws.GetWorkflow(ctx, id)
	is.NoError(err)
	is.Equal(driver.WorkflowCompensating, view.State, "the retry must keep the compensating workflow compensating")
	tasks, err := ws.WorkflowTasks(ctx, id)
	is.NoError(err)
	for _, task := range tasks {
		if task.TaskKey == "c" {
			is.Equal(driver.StateDead, task.State,
				"the original dead task must never be resurrected once compensation started")
		}
	}
}

// TestApplyFailurePolicyLosingTheCompensateRaceSkips races a suspend-policy
// pass against a manual CompensateWorkflow-style transition: the compensate
// commits while the policy pass is in flight. The pass must observe that the
// workflow already left running and skip it — never overwrite compensating
// with suspended (or resurrect a terminal state).
func TestApplyFailurePolicyLosingTheCompensateRaceSkips(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	ws := raceStore(t, h)
	pool := newPool(t, h.base, h.schema)
	ctx := context.Background()

	id := uuid.New()
	a := raceTask("a", "race_sp_a", 3)
	a.CompensationKind = "race_sp_undo_a"
	inserted, _, err := ws.CreateWorkflow(ctx, driver.WorkflowParams{
		ID: id, Name: "race-suspend", OnFailure: driver.OnFailureSuspend,
		Tasks: []driver.WorkflowTask{a, raceTask("c", "race_sp_c", 1)},
	})
	is.NoError(err)
	is.True(inserted)
	settleTask(ctx, t, h, "race_sp_a", false)
	settleTask(ctx, t, h, "race_sp_c", true) // c is dead; the suspend policy would fire

	tx := beginCompensateTx(ctx, t, pool, id, "comp:a", "race_sp_undo_a")

	type policyResult struct {
		failures []driver.WorkflowFailure
		err      error
	}
	policyDone := make(chan policyResult, 1)
	go func() {
		failures, err := ws.ApplyFailurePolicy(ctx)
		policyDone <- policyResult{failures: failures, err: err}
	}()

	time.Sleep(300 * time.Millisecond) // let the pass read 'running' and block on the header
	is.NoError(tx.Commit(ctx))

	select {
	case res := <-policyDone:
		is.NoError(res.err)
		for _, failure := range res.failures {
			is.NotEqual(id, failure.WorkflowID,
				"a policy pass that lost the race must not report the workflow as acted on")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ApplyFailurePolicy never returned")
	}

	view, err := ws.GetWorkflow(ctx, id)
	is.NoError(err)
	is.Equal(driver.WorkflowCompensating, view.State,
		"the manual compensate's state must stand; the policy pass must not overwrite it with suspended")
}
