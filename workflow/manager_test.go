package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type mgrArgs struct{}

func (mgrArgs) Kind() string { return "mgr.arg" }

// mgrFlaky aborts on its first execution and succeeds after a retry, so the
// Suspend policy + Manager.Retry round-trip can be exercised deterministically.
type mgrFlaky struct{}

func (mgrFlaky) Kind() string { return "mgr.flaky" }

func TestManagerGetAndTasksReturnNilWhenAbsent(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())

	view, err := r.Manager().Get(context.Background(), uuid.New())
	is.NoError(err)
	is.Nil(view, "a missing workflow is absence, not an error")

	tasks, err := r.Manager().Tasks(context.Background(), uuid.New())
	is.NoError(err)
	is.Nil(tasks)
}

func TestManagerListPaginatesNewestFirst(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	clk := drivertest.NewManualClock(time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC))
	f.Clock = clk
	r := newTestRuntime(t, f)

	// Five workflows created at strictly increasing times so "newest first" is
	// unambiguous.
	ids := make([]uuid.UUID, 0, 5)
	for range 5 {
		res, err := r.Client().Run(context.Background(), Define("page").Task("t", mgrArgs{}))
		is.NoError(err)
		ids = append(ids, res.ID)
		clk.Advance(time.Second)
	}

	first, err := r.Manager().List(context.Background(), Filter{Name: "page"}, 0, 2)
	is.NoError(err)
	is.Equal(int64(5), first.Total)
	is.Len(first.Items, 2)
	is.Equal(ids[4], first.Items[0].ID, "newest first")
	is.Equal(ids[3], first.Items[1].ID)

	third, err := r.Manager().List(context.Background(), Filter{Name: "page"}, 2, 2)
	is.NoError(err)
	is.Len(third.Items, 1, "0-based paging: the third page holds the remainder")
	is.Equal(ids[0], third.Items[0].ID)

	// size <= 0 defaults to 50.
	all, err := r.Manager().List(context.Background(), Filter{Name: "page"}, 0, 0)
	is.NoError(err)
	is.Equal(50, all.Size)
	is.Len(all.Items, 5)
}

func TestSuspendPolicyParksAndManagerRetryResumes(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	var runs atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, mgrFlaky) (None, error) {
		if runs.Add(1) == 1 {
			return None{}, Abort(testError("first run aborts"))
		}
		return None{}, nil
	}))

	res, err := r.Client().Run(context.Background(),
		Define("suspend", OnFailure(Suspend)).Task("t", mgrFlaky{}))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowSuspended
	}, 3*time.Second, 2*time.Millisecond, "the Suspend policy parks the workflow, leaving tasks untouched")
	is.Equal(driver.StateDead, taskByKey(t, f, res.ID, "t").State)

	// The operator resumes it; the reset task reruns and succeeds this time.
	is.NoError(r.Manager().Retry(context.Background(), res.ID))
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowSucceeded
	}, 3*time.Second, 2*time.Millisecond, "Retry resets the dead task with a fresh budget and resumes")
	is.Equal(int32(2), runs.Load())
}

func TestManagerCompensateUnwindsARunningWorkflow(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	var undone atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, sagaDo) (None, error) { return None{}, nil }))
	is.NoError(Register(r.Worker(), func(context.Context, sagaUndo) (None, error) {
		undone.Add(1)
		return None{}, nil
	}))

	// a succeeds (with a compensation), then the flow parks on a signal.
	res, err := r.Client().Run(context.Background(),
		Define("mgr-comp").
			Task("a", sagaDo{Step: "a"}, Compensate(sagaUndo{Step: "a"})).
			WaitSignal("wait", After("a")))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return taskByKey(t, f, res.ID, "wait").State == driver.StateWaiting
	}, 3*time.Second, 2*time.Millisecond, "the flow reaches the signal wait")

	is.NoError(r.Manager().Compensate(context.Background(), res.ID))
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowFailed
	}, 3*time.Second, 2*time.Millisecond, "Compensate cancels the rest and runs the saga to failed")

	is.Equal(int32(1), undone.Load(), "the declared compensation ran")
	is.Equal(driver.StateCancelled, taskByKey(t, f, res.ID, "wait").State)
	is.Equal(driver.StateSucceeded, taskByKey(t, f, res.ID, "comp:a").State)
}

func TestManagerCancelStopsARunningWorkflow(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	is.NoError(Register(r.Worker(), func(context.Context, sagaDo) (None, error) { return None{}, nil }))

	res, err := r.Client().Run(context.Background(),
		Define("mgr-cancel").
			Task("a", sagaDo{Step: "a"}).
			WaitSignal("wait", After("a")))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return taskByKey(t, f, res.ID, "wait").State == driver.StateWaiting
	}, 3*time.Second, 2*time.Millisecond)

	is.NoError(r.Manager().Cancel(context.Background(), res.ID))
	is.Equal(driver.WorkflowCancelled, workflowState(t, f, res.ID))
	is.Equal(driver.StateCancelled, taskByKey(t, f, res.ID, "wait").State)

	// Cancel on a missing workflow is a not-found error.
	err = r.Manager().Cancel(context.Background(), uuid.New())
	is.Error(err)
	is.True(IsNotFound(err))
}
