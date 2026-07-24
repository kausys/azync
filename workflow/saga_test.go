package workflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

// sagaDo is the forward step; step "c" aborts, killing the workflow.
type sagaDo struct {
	Step string `json:"step"`
}

func (sagaDo) Kind() string { return "saga.do" }

// sagaUndo is the compensation step; its handler records the order in which the
// saga unwinds.
type sagaUndo struct {
	Step string `json:"step"`
}

func (sagaUndo) Kind() string { return "saga.undo" }

func TestCancelPolicyRunsCompensationsInReverseOrder(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	var mu sync.Mutex
	var order []string
	is.NoError(Register(r.Worker(), func(_ context.Context, d sagaDo) (None, error) {
		if d.Step == "c" {
			return None{}, Abort(testError("step c is doomed"))
		}
		return None{}, nil
	}))
	is.NoError(Register(r.Worker(), func(_ context.Context, u sagaUndo) (None, error) {
		mu.Lock()
		order = append(order, u.Step)
		mu.Unlock()
		return None{}, nil
	}))

	// a -> b -> c: a and b succeed (both declare a compensation), c aborts.
	def := Define("saga"). // default policy is Cancel
				Task("a", sagaDo{Step: "a"}, Compensate(sagaUndo{Step: "a"})).
				Task("b", sagaDo{Step: "b"}, After("a"), Compensate(sagaUndo{Step: "b"})).
				Task("c", sagaDo{Step: "c"}, After("b"))
	res, err := r.Client().Run(context.Background(), def)
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowFailed
	}, 3*time.Second, 2*time.Millisecond, "an aborted task under the Cancel policy fails the workflow")

	// The compensation chain unwinds newest-completion-first: b completed after
	// a, so comp:b runs before comp:a.
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	is.Equal([]string{"b", "a"}, got, "compensations run in reverse completion order")

	is.Equal(driver.StateSucceeded, taskByKey(t, f, res.ID, "a").State)
	is.Equal(driver.StateSucceeded, taskByKey(t, f, res.ID, "b").State)
	is.Equal(driver.StateDead, taskByKey(t, f, res.ID, "c").State)
	is.Equal(driver.StateSucceeded, taskByKey(t, f, res.ID, "comp:a").State)
	is.Equal(driver.StateSucceeded, taskByKey(t, f, res.ID, "comp:b").State)

	view := getWorkflow(t, f, res.ID)
	is.Contains(view.FailureReason, "c", "the failure reason names the dead task")
}

// testError is a tiny error helper so the saga test avoids importing errors
// just for a sentinel.
type testError string

func (e testError) Error() string { return string(e) }
