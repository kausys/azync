package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

type iddRoot struct{}

func (iddRoot) Kind() string { return "idd.root" }

type iddTol struct{}

func (iddTol) Kind() string { return "idd.tol" }

// TestIgnoreDeadDepsMixedTriggersPolicy: a dead task with one non-tolerant
// dependent triggers the failure policy even though another dependent tolerates.
func TestIgnoreDeadDepsMixedTriggersPolicy(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	is.NoError(Register(r.Worker(), func(context.Context, iddRoot) (None, error) {
		return None{}, Abort(testError("root dies"))
	}))
	// t1 (tolerant) and t2 (strict) are intentionally unregistered so they can
	// never race the policy: they simply sit until it settles them.
	def := Define("mixed").
		Task("root", iddRoot{}).
		Task("t1", iddTol{}, After("root"), IgnoreDeadDeps()).
		Task("t2", iddTol{}, After("root"))
	res, err := r.Client().Run(context.Background(), def)
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowFailed
	}, 3*time.Second, 2*time.Millisecond, "a non-tolerant dependent makes the death trigger the policy")

	is.Equal(driver.StateCancelled, taskByKey(t, f, res.ID, "t1").State, "the Cancel policy cancels the remaining tasks")
	is.Equal(driver.StateCancelled, taskByKey(t, f, res.ID, "t2").State)
	is.Contains(getWorkflow(t, f, res.ID).FailureReason, "root")
}

// TestIgnoreDeadDepsFullyToleratedRunsButFails: when every dependent of a dead
// task tolerates it, the policy does not fire, the tolerant branch runs to
// completion, yet the workflow still settles failed (never succeeded).
func TestIgnoreDeadDepsFullyToleratedRunsButFails(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	var tolRan atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, iddRoot) (None, error) {
		return None{}, Abort(testError("root dies"))
	}))
	is.NoError(Register(r.Worker(), func(context.Context, iddTol) (None, error) {
		tolRan.Add(1)
		return None{}, nil
	}))

	// root dies; its only dependent tolerates it, so the branch runs.
	def := Define("tolerated").
		Task("root", iddRoot{}).
		Task("t1", iddTol{}, After("root"), IgnoreDeadDeps())
	res, err := r.Client().Run(context.Background(), def)
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowFailed
	}, 3*time.Second, 2*time.Millisecond, "a completed run with a dead task settles failed, not succeeded")

	is.Equal(int32(1), tolRan.Load(), "the tolerant branch actually ran")
	is.Equal(driver.StateSucceeded, taskByKey(t, f, res.ID, "t1").State,
		"the tolerant task succeeded: proof the Cancel policy never fired")
	is.Equal(driver.StateDead, taskByKey(t, f, res.ID, "root").State)
	is.Contains(getWorkflow(t, f, res.ID).FailureReason, "root",
		"completion records the dead task honestly")
}

// TestIgnoreDeadDepsLeafAlwaysTriggers: a dead task with no dependents triggers
// the policy even when it declares IgnoreDeadDeps itself — the exemption is
// never vacuous.
func TestIgnoreDeadDepsLeafAlwaysTriggers(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	is.NoError(Register(r.Worker(), func(context.Context, iddRoot) (None, error) {
		return None{}, Abort(testError("leaf dies"))
	}))
	// A single tolerant leaf: IgnoreDeadDeps concerns its (absent) upstream deps,
	// so it must not exempt the leaf's own death from the policy.
	res, err := r.Client().Run(context.Background(),
		Define("leaf").Task("solo", iddRoot{}, IgnoreDeadDeps()))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowFailed
	}, 3*time.Second, 2*time.Millisecond, "a dead leaf always triggers the policy")
	is.Contains(getWorkflow(t, f, res.ID).FailureReason, "solo")
}
