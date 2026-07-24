package workflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

type cliArgs struct {
	V string `json:"v"`
}

func (cliArgs) Kind() string { return "wf.cli" }

// cliBad fails to marshal, so Run must surface the payload error.
type cliBad struct{}

func (cliBad) Kind() string                 { return "wf.cli.bad" }
func (cliBad) MarshalJSON() ([]byte, error) { return nil, errors.New("boom") }

func TestRunInsertsHeaderTasksAndDepsAtomically(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	def := Define("atomic").
		Task("root", cliArgs{V: "r"}).
		Task("child", cliArgs{V: "c"}, After("root")).
		Sleep("nap", time.Hour).
		WaitSignal("sig")
	res, err := r.Client().Run(context.Background(), def)
	is.NoError(err)
	is.False(res.Deduplicated)

	view := getWorkflow(t, f, res.ID)
	is.Equal("atomic", view.Name)
	is.Equal(driver.WorkflowRunning, view.State)

	// Dependency-free tasks are immediately runnable; the internal kinds land in
	// their kind-specific runnable state; a task with a dependency starts blocked.
	is.Equal(driver.StatePending, taskByKey(t, f, res.ID, "root").State)
	is.Equal(driver.StateBlocked, taskByKey(t, f, res.ID, "child").State)
	is.Equal(driver.StateScheduled, taskByKey(t, f, res.ID, "nap").State)
	is.Equal(driver.StateWaiting, taskByKey(t, f, res.ID, "sig").State)

	tasks, err := r.Manager().Tasks(context.Background(), res.ID)
	is.NoError(err)
	is.Len(tasks, 4, "every declared task is inserted")
}

func TestRunStampsExplicitTaskBudget(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	res, err := r.Client().Run(context.Background(),
		Define("budget").
			Task("pinned", cliArgs{}, MaxRetries(7)).
			Task("default", cliArgs{}))
	is.NoError(err)
	is.Equal(7, taskByKey(t, f, res.ID, "pinned").MaxAttempts, "an explicit task budget is stamped durably")
	is.Zero(taskByKey(t, f, res.ID, "default").MaxAttempts, "without a task budget the value stays zero until the first lease resolves it")
}

func TestTaskBudgetResolvesOnFirstLease(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithDefaultMaxRetries(4))

	seen := make(chan int, 2)
	is.NoError(Register(r.Worker(), func(ctx context.Context, _ cliArgs) (None, error) {
		seen <- MaxAttempts(ctx)
		return None{}, nil
	}))

	_, err := r.Client().Run(context.Background(),
		Define("resolve").
			Task("p", cliArgs{V: "pinned"}, MaxRetries(9)).
			Task("d", cliArgs{V: "default"}))
	is.NoError(err)

	startWorker(t, r.Worker())
	got := map[int]bool{}
	for range 2 {
		select {
		case v := <-seen:
			got[v] = true
		case <-time.After(3 * time.Second):
			t.Fatal("a task did not run")
		}
	}
	is.True(got[9], "the task-level MaxRetries wins")
	is.True(got[4], "a task without one resolves the runtime default on its first lease")
}

func TestRunMergesDefinitionAndRunMeta(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	def := Define("meta", WithMeta("a", "1"), WithMeta("b", "def")).
		Task("t", cliArgs{})
	res, err := r.Client().Run(context.Background(), def,
		WithRunMeta("b", "run"), WithRunMeta("c", "3"))
	is.NoError(err)

	view := getWorkflow(t, f, res.ID)
	is.Equal(map[string]string{"a": "1", "b": "run", "c": "3"}, view.Meta,
		"run meta overrides definition meta on a key conflict")
	is.Equal(map[string]string{"a": "1", "b": "run", "c": "3"},
		taskByKey(t, f, res.ID, "t").Meta, "meta is stamped onto every task job")
}

func TestRunStampsActiveTrace(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	traceID := trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	spanID := trace.SpanID{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	}))

	res, err := r.Client().Run(ctx, Define("traced").Task("t", cliArgs{}))
	is.NoError(err)

	job := taskByKey(t, f, res.ID, "t")
	is.Equal(traceID.String(), job.TraceID)
	is.Equal(spanID.String(), job.SpanID)
	is.Equal(int16(trace.FlagsSampled), job.TraceFlags)
}

func TestRunMarshalFailureSurfacesTheKey(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	_, err := r.Client().Run(context.Background(), Define("bad").Task("boom", cliBad{}))
	is.Error(err)
	is.Contains(err.Error(), "marshal task")
	is.Contains(err.Error(), `"boom"`)

	page, err := r.Manager().List(context.Background(), Filter{}, 0, 50)
	is.NoError(err)
	is.Zero(page.Total)
}

func TestRunIdempotencyKeyDeduplicatesLiveExecutions(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	def := Define("idem").Task("t", cliArgs{})

	first, err := r.Client().Run(context.Background(), def, WithIdempotencyKey("k"))
	is.NoError(err)
	is.False(first.Deduplicated)

	second, err := r.Client().Run(context.Background(), def, WithIdempotencyKey("k"))
	is.NoError(err)
	is.True(second.Deduplicated, "a live execution with the same key must dedupe")
	is.Equal(first.ID, second.ID, "the caller gets the live execution's id")

	page, err := r.Manager().List(context.Background(), Filter{Name: "idem"}, 0, 50)
	is.NoError(err)
	is.Equal(int64(1), page.Total, "nothing new is inserted")
}

func TestTerminalWorkflowFreesTheIdempotencyKey(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	is.NoError(Register(r.Worker(), func(context.Context, cliArgs) (None, error) { return None{}, nil }))
	def := Define("free").Task("t", cliArgs{})

	first, err := r.Client().Run(context.Background(), def, WithIdempotencyKey("k"))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return workflowState(t, f, first.ID) == driver.WorkflowSucceeded
	}, 2*time.Second, 2*time.Millisecond, "the first execution must finish and free the key")

	second, err := r.Client().Run(context.Background(), def, WithIdempotencyKey("k"))
	is.NoError(err)
	is.False(second.Deduplicated, "a terminal workflow frees the key for a fresh run")
	is.NotEqual(first.ID, second.ID)
}

func TestConcurrentRunsWithSameKeyInsertExactlyOnce(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	def := Define("race").Task("t", cliArgs{})

	const n = 16
	var inserted, deduped atomic.Int32
	ids := make(chan uuid.UUID, n)
	var wg sync.WaitGroup
	var gate sync.WaitGroup
	gate.Add(1)
	for range n {
		wg.Go(func() {
			gate.Wait()
			res, err := r.Client().Run(context.Background(), def, WithIdempotencyKey("k"))
			if err != nil {
				return
			}
			if res.Deduplicated {
				deduped.Add(1)
			} else {
				inserted.Add(1)
			}
			ids <- res.ID
		})
	}
	gate.Done()
	wg.Wait()
	close(ids)

	is.Equal(int32(1), inserted.Load(), "exactly one racer inserts")
	is.Equal(int32(n-1), deduped.Load(), "the rest observe Deduplicated")
	first := <-ids
	for id := range ids {
		is.Equal(first, id, "every racer converges on the single live execution id")
	}
	page, err := r.Manager().List(context.Background(), Filter{Name: "race"}, 0, 50)
	is.NoError(err)
	is.Equal(int64(1), page.Total)
}

func TestSignalDeliversPayloadAndDownstreamReadsIt(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	type approval struct {
		By string `json:"by"`
	}
	got := make(chan string, 1)
	is.NoError(Register(r.Worker(), func(ctx context.Context, _ cliArgs) (None, error) {
		a, err := ResultOf[approval](ctx, "approved")
		if err != nil {
			return None{}, err
		}
		got <- a.By
		return None{}, nil
	}))

	res, err := r.Client().Run(context.Background(),
		Define("sig-flow").
			WaitSignal("approved").
			Task("act", cliArgs{}, After("approved")))
	is.NoError(err)

	startWorker(t, r.Worker())
	// The signal task is a root, so it parks waiting immediately.
	is.Eventually(func() bool {
		return taskByKey(t, f, res.ID, "approved").State == driver.StateWaiting
	}, 2*time.Second, 2*time.Millisecond)

	is.NoError(r.Client().Signal(context.Background(), res.ID, "approved", approval{By: "ops"}))

	select {
	case by := <-got:
		is.Equal("ops", by, "the downstream task reads the signal payload via ResultOf")
	case <-time.After(3 * time.Second):
		t.Fatal("the signalled task never completed")
	}
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowSucceeded
	}, 2*time.Second, 2*time.Millisecond)
}

func TestSignalWithNothingWaitingReturnsErrNoSignalMatched(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	res, err := r.Client().Run(context.Background(), Define("no-wait").Task("t", cliArgs{}))
	is.NoError(err)

	err = r.Client().Signal(context.Background(), res.ID, "ghost", nil)
	is.Error(err)
	is.ErrorIs(err, ErrNoSignalMatched, "the sentinel must be testable with errors.Is")
	is.Contains(err.Error(), "ghost")
}

// --- transactional creation ------------------------------------------------

// txWorkflowFake adds a trivial driver.TxWorkflowStore[struct{}] over the fake
// so the positive TxRunner path runs without a real transactional backend.
type txWorkflowFake struct {
	*drivertest.Fake
}

func (f *txWorkflowFake) CreateWorkflowTx(ctx context.Context, _ struct{}, p driver.WorkflowParams) (bool, uuid.UUID, error) {
	return f.CreateWorkflow(ctx, p)
}

var _ driver.TxWorkflowStore[struct{}] = (*txWorkflowFake)(nil)

func TestTxRunnerRequiresTxWorkflowStore(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake()) // no TxWorkflowStore

	_, err := TxRunner[struct{}](r)
	is.Error(err)
	is.Contains(err.Error(), "does not support transactional workflow creation")
	is.Contains(err.Error(), "struct {}")
}

func TestTxRunnerCreatesThroughTx(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := &txWorkflowFake{Fake: drivertest.NewFake()}
	core, err := azync.New(f, azync.WithLogger(discardLogger()))
	is.NoError(err)
	r, err := New(core, fastOptions()...)
	is.NoError(err)

	tr, err := TxRunner[struct{}](r)
	is.NoError(err)

	res, err := tr.RunTx(context.Background(), struct{}{},
		Define("tx").Task("t", cliArgs{V: "x"}, MaxRetries(3)))
	is.NoError(err)
	is.False(res.Deduplicated)

	view := getWorkflow(t, f.Fake, res.ID)
	is.Equal("tx", view.Name)
	is.Equal(3, taskByKey(t, f.Fake, res.ID, "t").MaxAttempts)
}
