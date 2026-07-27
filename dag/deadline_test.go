package dag

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

// pollTask is a handler that always reports NotReady: the polling-wait shape
// whose unbounded variant reproduced the V2 incident (a rejection polling
// forever with no dead-letter).
type pollTask struct{}

func (pollTask) Kind() string { return "ddl.poll" }

// TestNotReadyPastDeadlineDeadLettersAndAppliesPolicy proves the Deadline
// task option bounds the NotReady loop: once the stamped deadline passes, the
// next NotReady dead-letters the task instead of re-parking and the
// workflow's Suspend policy fires with an alertable reason.
func TestNotReadyPastDeadlineDeadLettersAndAppliesPolicy(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f)

	is.NoError(Register(r.Worker(), func(context.Context, pollTask) (None, error) {
		return None{}, NotReady(time.Millisecond)
	}))

	res, err := r.Client().Run(context.Background(),
		Define("ddl", OnFailure(Suspend)).
			Task("wait", pollTask{}, Deadline(30*time.Second)))
	is.NoError(err)

	startWorker(t, r.Worker())

	// The first NotReady stamps the deadline and parks.
	is.Eventually(func() bool {
		wait := taskByKey(t, f, res.ID, "wait")
		return wait.State == driver.StateScheduled && !wait.DeadlineAt.IsZero()
	}, 2*time.Second, 2*time.Millisecond, "the first NotReady stamps the deadline and parks")

	// Within budget the task keeps polling — never dead.
	is.Never(func() bool {
		return taskByKey(t, f, res.ID, "wait").State == driver.StateDead
	}, 60*time.Millisecond, 10*time.Millisecond, "within budget NotReady keeps parking")

	clk.Advance(time.Hour) // far past the 30s budget
	is.Eventually(func() bool {
		return dagState(t, f, res.ID) == driver.DAGSuspended
	}, 2*time.Second, 2*time.Millisecond, "past the deadline the task dies and Suspend fires")

	wait := taskByKey(t, f, res.ID, "wait")
	is.Equal(driver.StateDead, wait.State)
	is.Contains(wait.LastError, "snooze deadline exceeded")
	is.Contains(wait.LastError, "not ready", "the handler's own NotReady text survives in the reason")
}

// TestNotReadyPastDeadlineWithCancelPolicyCompensates proves the deadline
// death flows through the ordinary failure path: with the Cancel policy, a
// succeeded upstream task's compensation runs.
func TestNotReadyPastDeadlineWithCancelPolicyCompensates(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f)

	compensated := make(chan struct{})
	is.NoError(Register(r.Worker(), func(_ context.Context, d sagaDo) (None, error) {
		return None{}, nil
	}))
	is.NoError(Register(r.Worker(), func(_ context.Context, u sagaUndo) (None, error) {
		close(compensated)
		return None{}, nil
	}))
	is.NoError(Register(r.Worker(), func(context.Context, pollTask) (None, error) {
		return None{}, NotReady(time.Millisecond)
	}))

	res, err := r.Client().Run(context.Background(),
		Define("ddl_cancel"). // default policy: Cancel
					Task("create", sagaDo{Step: "create"}, Compensate(sagaUndo{Step: "create"})).
					Task("wait", pollTask{}, After("create"), Deadline(30*time.Second)))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		wait := taskByKey(t, f, res.ID, "wait")
		return wait.State == driver.StateScheduled && !wait.DeadlineAt.IsZero()
	}, 2*time.Second, 2*time.Millisecond)

	clk.Advance(time.Hour)
	select {
	case <-compensated:
	case <-time.After(2 * time.Second):
		t.Fatal("the deadline death must trigger the Cancel policy's compensation")
	}
	is.Eventually(func() bool {
		s := dagState(t, f, res.ID)
		return s == driver.DAGFailed || s == driver.DAGCancelled
	}, 2*time.Second, 2*time.Millisecond)
}

// TestDeadlineOptionTravelsToDriverTask pins the plumbing: the option lands
// on driver.DAGTask.Deadline, and non-positive values are ignored.
func TestDeadlineOptionTravelsToDriverTask(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	def := Define("ddl_opt").
		Task("bounded", pollTask{}, Deadline(72*time.Hour)).
		Task("zero", pollTask{}, Deadline(0)).
		Task("negative", pollTask{}, Deadline(-time.Minute))
	params, err := r.Client().makeParams(context.Background(), def)
	is.NoError(err)

	byKey := map[string]driver.DAGTask{}
	for _, tk := range params.Tasks {
		byKey[tk.Key] = tk
	}
	is.Equal(72*time.Hour, byKey["bounded"].Deadline)
	is.Zero(byKey["zero"].Deadline, "Deadline(0) is ignored")
	is.Zero(byKey["negative"].Deadline, "a negative Deadline is ignored")
}
