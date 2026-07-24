package workflow

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

// recordingStore wraps the fake and records the scheduler calls, so the pass
// order — the completion semantics depends on the policy running first — can
// be asserted.
type recordingStore struct {
	*drivertest.Fake

	mu    sync.Mutex
	calls []string
}

func (s *recordingStore) record(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, name)
}

func (s *recordingStore) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *recordingStore) PromoteUnblocked(ctx context.Context) (int64, error) {
	s.record("promote")
	return s.Fake.PromoteUnblocked(ctx)
}

func (s *recordingStore) CompleteDueSleeps(ctx context.Context) (int64, error) {
	s.record("sleeps")
	return s.Fake.CompleteDueSleeps(ctx)
}

func (s *recordingStore) ApplyFailurePolicy(ctx context.Context) ([]driver.WorkflowFailure, error) {
	s.record("policy")
	return s.Fake.ApplyFailurePolicy(ctx)
}

func (s *recordingStore) CompleteWorkflows(ctx context.Context) (int64, error) {
	s.record("complete")
	return s.Fake.CompleteWorkflows(ctx)
}

func TestSchedulerPassRunsInContractOrder(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	rec := &recordingStore{Fake: drivertest.NewFake()}
	core, err := azync.New(rec, azync.WithLogger(discardLogger()))
	is.NoError(err)
	r, err := New(core, fastOptions()...)
	is.NoError(err)

	stop := startWorker(t, r.Worker())
	is.Eventually(func() bool { return len(rec.snapshot()) >= 8 },
		2*time.Second, 2*time.Millisecond, "the scheduler loop must tick")
	_ = stop()

	calls := rec.snapshot()
	want := []string{"promote", "sleeps", "policy", "complete"}
	for i, call := range calls {
		is.Equal(want[i%4], call,
			"scheduler order must be PromoteUnblocked -> CompleteDueSleeps -> ApplyFailurePolicy -> CompleteWorkflows (call %d of %v)", i, calls)
	}
}

// pollArgs is the polling-wait task of the NotReady tests.
type pollArgs struct {
	Value string `json:"value"`
}

func (pollArgs) Kind() string { return "wf.poll" }

func TestNotReadyReChecksWithoutConsumingBudget(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f)

	// Budget 1: if NotReady consumed an attempt, the first re-check would
	// exhaust the budget and dead-letter the task instead of re-polling.
	var runs atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, pollArgs) (None, error) {
		if runs.Add(1) == 1 {
			return None{}, NotReady(time.Hour)
		}
		return None{}, nil
	}, WithMaxRetries(1)))

	res, err := r.Client().Run(context.Background(), Define("poll-wf").Task("poll", pollArgs{Value: "x"}))
	is.NoError(err)
	is.False(res.Deduplicated)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return taskByKey(t, f, res.ID, "poll").State == driver.StateScheduled
	}, 2*time.Second, 2*time.Millisecond, "NotReady must park the task as scheduled")

	parked := taskByKey(t, f, res.ID, "poll")
	is.Equal(0, parked.Attempt, "NotReady must hand the attempt back: the budget is not consumed")
	is.True(parked.RunAt.Equal(clk.Now().Add(time.Hour)), "the re-check delay resolves against the store clock")
	attempts, err := f.JobAttempts(context.Background(), driver.SourceWorkflow, parked.ID)
	is.NoError(err)
	is.Empty(attempts, "NotReady is not a failure: no attempt history")

	// Make the re-check due: the task runs again and succeeds within budget 1.
	clk.Advance(2 * time.Hour)
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowSucceeded
	}, 2*time.Second, 2*time.Millisecond, "the re-check must run and complete the workflow")
	is.Equal(int32(2), runs.Load())

	done := taskByKey(t, f, res.ID, "poll")
	is.Equal(1, done.Attempt, "the successful run is attempt 1: the snooze never counted")
	attempts, err = f.JobAttempts(context.Background(), driver.SourceWorkflow, done.ID)
	is.NoError(err)
	is.Empty(attempts)
}

// The diamond DAG of the results test: a -> (b, c) -> d, where b and c read
// a's result and d reads both.
type (
	diamondA struct {
		Base int `json:"base"`
	}
	diamondB struct{}
	diamondC struct{}
	diamondD struct{}
	sum      struct {
		Sum int `json:"sum"`
	}
)

func (diamondA) Kind() string { return "wf.diamond.a" }
func (diamondB) Kind() string { return "wf.diamond.b" }
func (diamondC) Kind() string { return "wf.diamond.c" }
func (diamondD) Kind() string { return "wf.diamond.d" }

func TestDiamondPromotesCascadeAndFlowsResults(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	missingErrs := make(chan error, 1)
	finals := make(chan sum, 1)
	is.NoError(Register(r.Worker(), func(_ context.Context, a diamondA) (sum, error) {
		return sum{Sum: a.Base + 1}, nil
	}))
	is.NoError(Register(r.Worker(), func(ctx context.Context, _ diamondB) (sum, error) {
		if _, err := ResultOf[sum](ctx, "never-declared"); err != nil {
			select {
			case missingErrs <- err:
			default:
			}
		}
		a, err := ResultOf[sum](ctx, "a")
		if err != nil {
			return sum{}, err
		}
		return sum{Sum: a.Sum * 10}, nil
	}))
	is.NoError(Register(r.Worker(), func(ctx context.Context, _ diamondC) (sum, error) {
		a, err := ResultOf[sum](ctx, "a")
		if err != nil {
			return sum{}, err
		}
		return sum{Sum: a.Sum * 100}, nil
	}))
	is.NoError(Register(r.Worker(), func(ctx context.Context, _ diamondD) (sum, error) {
		b, err := ResultOf[sum](ctx, "b")
		if err != nil {
			return sum{}, err
		}
		c, err := ResultOf[sum](ctx, "c")
		if err != nil {
			return sum{}, err
		}
		out := sum{Sum: b.Sum + c.Sum}
		finals <- out
		return out, nil
	}))

	def := Define("diamond").
		Task("a", diamondA{Base: 1}).
		Task("b", diamondB{}, After("a")).
		Task("c", diamondC{}, After("a")).
		Task("d", diamondD{}, After("b", "c"))
	res, err := r.Client().Run(context.Background(), def)
	is.NoError(err)

	startWorker(t, r.Worker())
	select {
	case got := <-finals:
		// a = 1+1 = 2; b = 2*10 = 20; c = 2*100 = 200; d = 20+200.
		is.Equal(220, got.Sum, "d must see the real values of both branches")
	case <-time.After(4 * time.Second):
		t.Fatal("the diamond did not cascade to d")
	}
	select {
	case err := <-missingErrs:
		is.Contains(err.Error(), "never-declared")
		is.Contains(err.Error(), "no persisted result")
	case <-time.After(2 * time.Second):
		t.Fatal("ResultOf on an unknown key must error")
	}

	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowSucceeded
	}, 2*time.Second, 2*time.Millisecond)

	results, err := f.TaskResults(context.Background(), res.ID, nil)
	is.NoError(err)
	is.JSONEq(`{"sum":2}`, string(results["a"]))
	is.JSONEq(`{"sum":20}`, string(results["b"]))
	is.JSONEq(`{"sum":200}`, string(results["c"]))
	is.JSONEq(`{"sum":220}`, string(results["d"]))
}

// The barrier test: n upstream workflows all try to start the same downstream
// workflow; the idempotency key must let exactly one through.
type (
	barrierUp struct {
		Index int `json:"index"`
	}
	barrierDown struct{}
)

func (barrierUp) Kind() string   { return "wf.barrier.up" }
func (barrierDown) Kind() string { return "wf.barrier.down" }

func TestBarrierPatternStartsDownstreamExactlyOnce(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	const n = 5
	var inserted, deduplicated atomic.Int32
	upDone := make(chan struct{}, n)
	downDef := Define("barrier-down").Task("start", barrierDown{})

	is.NoError(Register(r.Worker(), func(ctx context.Context, _ barrierUp) (None, error) {
		res, err := r.Client().Run(ctx, downDef, WithIdempotencyKey("kyb-1"))
		if err != nil {
			return None{}, err
		}
		if res.Deduplicated {
			deduplicated.Add(1)
		} else {
			inserted.Add(1)
		}
		upDone <- struct{}{}
		return None{}, nil
	}, WithConcurrency(n)))
	is.NoError(Register(r.Worker(), func(context.Context, barrierDown) (None, error) {
		return None{}, nil
	}))

	for i := range n {
		_, err := r.Client().Run(context.Background(),
			Define("barrier-up").Task("check", barrierUp{Index: i}))
		is.NoError(err)
	}

	startWorker(t, r.Worker())
	for range n {
		select {
		case <-upDone:
		case <-time.After(4 * time.Second):
			t.Fatal("an upstream barrier task did not run")
		}
	}

	is.Equal(int32(1), inserted.Load(), "exactly one Run must win the barrier")
	is.Equal(int32(n-1), deduplicated.Load(), "the other runs must observe Deduplicated")
	page, err := r.Manager().List(context.Background(), Filter{Name: "barrier-down"}, 0, 50)
	is.NoError(err)
	is.Equal(int64(1), page.Total, "the downstream workflow must exist exactly once")

	is.Eventually(func() bool {
		return workflowState(t, f, page.Items[0].ID) == driver.WorkflowSucceeded
	}, 2*time.Second, 2*time.Millisecond, "the single downstream workflow must complete")
}
