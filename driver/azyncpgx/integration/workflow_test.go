package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync/event"
	"github.com/kausys/azync/queue"
	"github.com/kausys/azync/workflow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// wfSettle is the budget a DAG gets to reach a state. The scheduler tick and
// the engine's scheduled->pending promotion both default to one second (their
// faster test seams are package-private to workflow), so the graph advances
// about one level per second; every wait below is sized generously against
// that cadence rather than against the handlers, which are instant.
const wfSettle = 30 * time.Second

// newWorkflow composes a workflow runtime over the harness Core. The runtime
// inherits the harness's fast fetch cadences (poll/cooldown/backoff) from the
// Core defaults, so runnable tasks are leased promptly; only the DAG scheduler
// runs on the fixed one-second tick.
func newWorkflow(t *testing.T, h *harness, opts ...workflow.Option) *workflow.Runtime {
	t.Helper()
	r, err := workflow.New(h.core, opts...)
	require.NoError(t, err)
	return r
}

// awaitWorkflow blocks until the workflow reaches want or the settle budget
// runs out.
func awaitWorkflow(t *testing.T, m *workflow.Manager, id uuid.UUID, want workflow.WorkflowState) {
	t.Helper()
	is := require.New(t)
	require.Eventually(t, func() bool {
		v, err := m.Get(context.Background(), id)
		is.NoError(err)
		return v != nil && v.State == want
	}, wfSettle, 50*time.Millisecond, "workflow %s never reached %q", id, want)
}

// taskView reads one task projection of a workflow by its DAG key.
func taskView(t *testing.T, m *workflow.Manager, id uuid.UUID, key string) workflow.TaskView {
	t.Helper()
	is := require.New(t)
	tasks, err := m.Tasks(context.Background(), id)
	is.NoError(err)
	for _, tk := range tasks {
		if tk.Key == key {
			return tk
		}
	}
	t.Fatalf("workflow %s has no task %q", id, key)
	return workflow.TaskView{}
}

// --- diamond -----------------------------------------------------------------

type (
	diamondSeed struct {
		Base int `json:"base"`
	}
	diamondUp   struct{}
	diamondSide struct{}
	diamondSink struct{}
	wfNum       struct {
		N int `json:"n"`
	}
)

func (diamondSeed) Kind() string { return "wf.it.diamond.seed" }
func (diamondUp) Kind() string   { return "wf.it.diamond.up" }
func (diamondSide) Kind() string { return "wf.it.diamond.side" }
func (diamondSink) Kind() string { return "wf.it.diamond.sink" }

// TestWorkflowDiamondFlowsResults runs a typed diamond A -> (B, C) -> D against
// a live PostgreSQL: B and C read A's persisted result, D reads both, and the
// final value proves every edge carried the real upstream output. It asserts
// the concrete values, every task succeeded and the workflow itself succeeded.
func TestWorkflowDiamondFlowsResults(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	r := newWorkflow(t, h)
	m := r.Manager()
	ctx := context.Background()

	final := make(chan int, 1)
	is.NoError(workflow.Register(r.Worker(), func(_ context.Context, a diamondSeed) (wfNum, error) {
		return wfNum{N: a.Base + 1}, nil
	}))
	is.NoError(workflow.Register(r.Worker(), func(ctx context.Context, _ diamondUp) (wfNum, error) {
		a, err := workflow.ResultOf[wfNum](ctx, "a")
		if err != nil {
			return wfNum{}, err
		}
		return wfNum{N: a.N * 10}, nil
	}))
	is.NoError(workflow.Register(r.Worker(), func(ctx context.Context, _ diamondSide) (wfNum, error) {
		a, err := workflow.ResultOf[wfNum](ctx, "a")
		if err != nil {
			return wfNum{}, err
		}
		return wfNum{N: a.N * 100}, nil
	}))
	is.NoError(workflow.Register(r.Worker(), func(ctx context.Context, _ diamondSink) (wfNum, error) {
		b, err := workflow.ResultOf[wfNum](ctx, "b")
		if err != nil {
			return wfNum{}, err
		}
		c, err := workflow.ResultOf[wfNum](ctx, "c")
		if err != nil {
			return wfNum{}, err
		}
		out := wfNum{N: b.N + c.N}
		final <- out.N
		return out, nil
	}))

	def := workflow.Define("it-diamond").
		Task("a", diamondSeed{Base: 1}).
		Task("b", diamondUp{}, workflow.After("a")).
		Task("c", diamondSide{}, workflow.After("a")).
		Task("d", diamondSink{}, workflow.After("b", "c"))
	res, err := r.Client().Run(ctx, def)
	is.NoError(err)
	is.False(res.Deduplicated)

	startWorker(t, r.Worker())
	select {
	case got := <-final:
		// a = 1+1 = 2; b = 2*10 = 20; c = 2*100 = 200; d = 20+200 = 220.
		is.Equal(220, got, "d must see the real persisted values of both branches")
	case <-time.After(wfSettle):
		t.Fatal("the diamond never cascaded to d")
	}

	awaitWorkflow(t, m, res.ID, workflow.StateSucceeded)
	for _, key := range []string{"a", "b", "c", "d"} {
		is.Equal(workflow.TaskSucceeded, taskView(t, m, res.ID, key).State, "task %q must have succeeded", key)
		is.True(taskView(t, m, res.ID, key).HasResult, "task %q persisted a result", key)
	}
}

// --- polling wait ------------------------------------------------------------

type provisionPoll struct {
	Ref string `json:"ref"`
}

func (provisionPoll) Kind() string { return "wf.it.poll" }

// TestWorkflowNotReadyPollsWithoutConsumingBudget proves the polling-wait
// primitive over a live backend: a task that returns NotReady several times
// before succeeding re-checks each time without burning an attempt, so it
// completes within a retry budget of one — the value braid's CIP-status poll
// depends on.
func TestWorkflowNotReadyPollsWithoutConsumingBudget(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	r := newWorkflow(t, h)
	m := r.Manager()
	ctx := context.Background()

	var runs atomic.Int32
	// Budget 1: were NotReady a real attempt, the first re-check would exhaust
	// the budget and dead-letter the task instead of polling on.
	is.NoError(workflow.Register(r.Worker(), func(_ context.Context, _ provisionPoll) (workflow.None, error) {
		if runs.Add(1) <= 3 {
			return workflow.None{}, workflow.NotReady(100 * time.Millisecond)
		}
		return workflow.None{}, nil
	}, workflow.WithMaxRetries(1)))

	res, err := r.Client().Run(ctx, workflow.Define("it-poll").Task("poll", provisionPoll{Ref: "x"}))
	is.NoError(err)

	startWorker(t, r.Worker())
	awaitWorkflow(t, m, res.ID, workflow.StateSucceeded)

	is.EqualValues(4, runs.Load(), "the handler polled three times then succeeded on the fourth run")
	done := taskView(t, m, res.ID, "poll")
	is.Equal(workflow.TaskSucceeded, done.State)
	is.Equal(1, done.Attempt, "the successful run is attempt 1: no NotReady snooze counted against the budget")
}

// --- sleep advanced by a signal ---------------------------------------------

type sleepFinish struct{}

func (sleepFinish) Kind() string { return "wf.it.sleep.finish" }

// TestWorkflowSleepAdvancedBySignal proves a durable timer is interruptible: a
// 24h Sleep is woken early by a signal named after it, so the workflow settles
// in seconds instead of a day — reaching a terminal state at all is dispositive
// proof the signal short-circuited the timer.
func TestWorkflowSleepAdvancedBySignal(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	r := newWorkflow(t, h)
	m := r.Manager()
	ctx := context.Background()

	is.NoError(workflow.Register(r.Worker(), func(context.Context, sleepFinish) (workflow.None, error) {
		return workflow.None{}, nil
	}))

	def := workflow.Define("it-sleep").
		Sleep("cool", 24*time.Hour).
		Task("finish", sleepFinish{}, workflow.After("cool"))
	res, err := r.Client().Run(ctx, def)
	is.NoError(err)

	startWorker(t, r.Worker())
	// The root timer is scheduled the instant Run inserts it; wait for that so
	// the signal has a scheduled $sleep row to wake.
	require.Eventually(t, func() bool {
		return taskView(t, m, res.ID, "cool").State == workflow.TaskScheduled
	}, wfSettle, 50*time.Millisecond, "the sleep timer must be scheduled before it is signalled")

	start := time.Now()
	is.NoError(r.Client().Signal(ctx, res.ID, "cool", nil))

	awaitWorkflow(t, m, res.ID, workflow.StateSucceeded)
	is.Less(time.Since(start), 10*time.Second, "the signal woke the 24h timer early; the workflow must not wait it out")
	is.Equal(workflow.TaskSucceeded, taskView(t, m, res.ID, "cool").State)
	is.Equal(workflow.TaskSucceeded, taskView(t, m, res.ID, "finish").State)
}

// --- wait for signal ---------------------------------------------------------

type approvalAct struct{}

func (approvalAct) Kind() string { return "wf.it.approval.act" }

type approvalPayload struct {
	By string `json:"by"`
}

// TestWorkflowWaitSignalDeliversPayload parks a branch on WaitSignal, proves an
// unmatched Signal returns ErrNoSignalMatched, then delivers the real signal
// and asserts the downstream task read its payload through ResultOf and the
// workflow succeeded.
func TestWorkflowWaitSignalDeliversPayload(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	r := newWorkflow(t, h)
	m := r.Manager()
	ctx := context.Background()

	gotBy := make(chan string, 1)
	is.NoError(workflow.Register(r.Worker(), func(ctx context.Context, _ approvalAct) (workflow.None, error) {
		a, err := workflow.ResultOf[approvalPayload](ctx, "approved")
		if err != nil {
			return workflow.None{}, err
		}
		gotBy <- a.By
		return workflow.None{}, nil
	}))

	def := workflow.Define("it-wait").
		WaitSignal("approved").
		Task("act", approvalAct{}, workflow.After("approved"))
	res, err := r.Client().Run(ctx, def)
	is.NoError(err)

	startWorker(t, r.Worker())
	require.Eventually(t, func() bool {
		return taskView(t, m, res.ID, "approved").State == workflow.TaskWaiting
	}, wfSettle, 50*time.Millisecond, "the WaitSignal task must park before it is signalled")

	// A signal no task is waiting for is a typed, testable error.
	err = r.Client().Signal(ctx, res.ID, "ghost", nil)
	is.ErrorIs(err, workflow.ErrNoSignalMatched)
	is.Contains(err.Error(), "ghost")

	is.NoError(r.Client().Signal(ctx, res.ID, "approved", approvalPayload{By: "ops"}))
	select {
	case by := <-gotBy:
		is.Equal("ops", by, "the downstream task reads the signal payload via ResultOf")
	case <-time.After(wfSettle):
		t.Fatal("the signalled branch never ran")
	}
	awaitWorkflow(t, m, res.ID, workflow.StateSucceeded)
}

// --- saga (Cancel policy) ----------------------------------------------------

type sagaStep struct {
	Step string `json:"step"`
}

func (sagaStep) Kind() string { return "wf.it.saga.do" }

type sagaComp struct {
	Step string `json:"step"`
}

func (sagaComp) Kind() string { return "wf.it.saga.undo" }

// TestWorkflowCancelPolicyRunsCompensationsInReverseOrder drives a saga to
// failure over the live backend: a -> b -> c where a and b declare
// compensations and c aborts. The Cancel policy must run comp:b then comp:a
// (reverse completion order) and settle the workflow failed, naming the dead
// task.
func TestWorkflowCancelPolicyRunsCompensationsInReverseOrder(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	r := newWorkflow(t, h)
	m := r.Manager()
	ctx := context.Background()

	var mu sync.Mutex
	var order []string
	is.NoError(workflow.Register(r.Worker(), func(_ context.Context, d sagaStep) (workflow.None, error) {
		if d.Step == "c" {
			return workflow.None{}, workflow.Abort(stringError("step c is doomed"))
		}
		return workflow.None{}, nil
	}))
	is.NoError(workflow.Register(r.Worker(), func(_ context.Context, u sagaComp) (workflow.None, error) {
		mu.Lock()
		order = append(order, u.Step)
		mu.Unlock()
		return workflow.None{}, nil
	}))

	def := workflow.Define("it-saga"). // default policy is Cancel
						Task("a", sagaStep{Step: "a"}, workflow.Compensate(sagaComp{Step: "a"})).
						Task("b", sagaStep{Step: "b"}, workflow.After("a"), workflow.Compensate(sagaComp{Step: "b"})).
						Task("c", sagaStep{Step: "c"}, workflow.After("b"))
	res, err := r.Client().Run(ctx, def)
	is.NoError(err)

	startWorker(t, r.Worker())
	awaitWorkflow(t, m, res.ID, workflow.StateFailed)

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	is.Equal([]string{"b", "a"}, got, "compensations run newest-completion-first: comp:b before comp:a")

	is.Equal(workflow.TaskSucceeded, taskView(t, m, res.ID, "a").State)
	is.Equal(workflow.TaskSucceeded, taskView(t, m, res.ID, "b").State)
	is.Equal(workflow.TaskDead, taskView(t, m, res.ID, "c").State)
	is.Equal(workflow.TaskSucceeded, taskView(t, m, res.ID, "comp:a").State)
	is.Equal(workflow.TaskSucceeded, taskView(t, m, res.ID, "comp:b").State)

	view, err := m.Get(ctx, res.ID)
	is.NoError(err)
	is.NotNil(view)
	is.Contains(view.FailureReason, "c", "the failure reason names the dead task")
}

// --- suspend + Manager.Retry -------------------------------------------------

type suspendFlaky struct{}

func (suspendFlaky) Kind() string { return "wf.it.flaky" }

// TestWorkflowSuspendPolicyThenManagerRetry proves the operator loop: a Suspend
// policy parks the workflow on a dead task (tasks untouched), and Manager.Retry
// resets the task with a fresh budget and drives the flow to success.
func TestWorkflowSuspendPolicyThenManagerRetry(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	r := newWorkflow(t, h)
	m := r.Manager()
	ctx := context.Background()

	var runs atomic.Int32
	is.NoError(workflow.Register(r.Worker(), func(context.Context, suspendFlaky) (workflow.None, error) {
		if runs.Add(1) == 1 {
			return workflow.None{}, workflow.Abort(stringError("first run aborts"))
		}
		return workflow.None{}, nil
	}))

	res, err := r.Client().Run(ctx,
		workflow.Define("it-suspend", workflow.OnFailure(workflow.Suspend)).Task("t", suspendFlaky{}))
	is.NoError(err)

	startWorker(t, r.Worker())
	awaitWorkflow(t, m, res.ID, workflow.StateSuspended)
	is.Equal(workflow.TaskDead, taskView(t, m, res.ID, "t").State, "Suspend leaves the dead task in place for the operator")

	is.NoError(m.Retry(ctx, res.ID))
	awaitWorkflow(t, m, res.ID, workflow.StateSucceeded)
	is.EqualValues(2, runs.Load(), "Retry reset the dead task and it ran once more, succeeding")
	is.Equal(workflow.TaskSucceeded, taskView(t, m, res.ID, "t").State)
}

// --- barrier (fan-in across workflows) --------------------------------------

type uboCheck struct {
	Idx int `json:"idx"`
}

func (uboCheck) Kind() string { return "wf.it.ubo.check" }

type businessStart struct{}

func (businessStart) Kind() string { return "wf.it.business.start" }

// TestWorkflowBarrierStartsDownstreamExactlyOnce replicates braid's fan-in: N
// concurrent "ubo" workflows each end by calling Run on the same downstream
// definition with a shared idempotency key. Exactly one insert must win and the
// downstream "business" workflow must run exactly once — the live-execution
// dedupe replacing braid's Redis SetNX lock and its task-id-conflict swallow.
func TestWorkflowBarrierStartsDownstreamExactlyOnce(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	r := newWorkflow(t, h)
	m := r.Manager()
	ctx := context.Background()

	const n = 3
	var inserted, deduplicated, businessRuns atomic.Int32
	upDone := make(chan struct{}, n)
	businessDef := workflow.Define("it-business").Task("start", businessStart{})

	is.NoError(workflow.Register(r.Worker(), func(ctx context.Context, _ uboCheck) (workflow.None, error) {
		out, err := r.Client().Run(ctx, businessDef, workflow.WithIdempotencyKey("kyb-42"))
		if err != nil {
			return workflow.None{}, err
		}
		if out.Deduplicated {
			deduplicated.Add(1)
		} else {
			inserted.Add(1)
		}
		upDone <- struct{}{}
		return workflow.None{}, nil
	}, workflow.WithConcurrency(n)))
	is.NoError(workflow.Register(r.Worker(), func(context.Context, businessStart) (workflow.None, error) {
		businessRuns.Add(1)
		return workflow.None{}, nil
	}))

	for i := range n {
		_, err := r.Client().Run(ctx, workflow.Define("it-ubo").Task("check", uboCheck{Idx: i}))
		is.NoError(err)
	}

	startWorker(t, r.Worker())
	for range n {
		select {
		case <-upDone:
		case <-time.After(wfSettle):
			t.Fatal("an upstream barrier task never ran")
		}
	}

	is.EqualValues(1, inserted.Load(), "exactly one Run wins the barrier")
	is.EqualValues(n-1, deduplicated.Load(), "the other runs observe Deduplicated")

	page, err := m.List(ctx, workflow.Filter{Name: "it-business"}, 0, 50)
	is.NoError(err)
	is.EqualValues(1, page.Total, "the downstream workflow exists exactly once")

	awaitWorkflow(t, m, page.Items[0].ID, workflow.StateSucceeded)
	is.EqualValues(1, businessRuns.Load(), "the downstream business task ran exactly once")
}

// --- transactional creation --------------------------------------------------

type txWorkflowArg struct {
	V string `json:"v"`
}

func (txWorkflowArg) Kind() string { return "wf.it.tx" }

// TestWorkflowTxRunnerRollbackAndCommit proves workflow creation is a real
// outbox over pgx.Tx: a rolled-back RunTx leaves no workflow, and a committed
// one both persists the workflow and lets the scheduler drive it to success.
func TestWorkflowTxRunnerRollbackAndCommit(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	r := newWorkflow(t, h)
	m := r.Manager()
	ctx := context.Background()
	pool := newPool(t, h.base, h.schema)

	tr, err := workflow.TxRunner[pgx.Tx](r)
	is.NoError(err)

	def := workflow.Define("it-tx").Task("t", txWorkflowArg{V: "x"})

	// Rollback: the workflow never lands.
	tx, err := pool.Begin(ctx)
	is.NoError(err)
	rolled, err := tr.RunTx(ctx, tx, def)
	is.NoError(err)
	is.NoError(tx.Rollback(ctx))
	got, err := m.Get(ctx, rolled.ID)
	is.NoError(err)
	is.Nil(got, "a rolled-back RunTx leaves no workflow")

	// Commit: the workflow lands and the scheduler runs it to completion.
	is.NoError(workflow.Register(r.Worker(), func(context.Context, txWorkflowArg) (workflow.None, error) {
		return workflow.None{}, nil
	}))
	startWorker(t, r.Worker())

	tx, err = pool.Begin(ctx)
	is.NoError(err)
	committed, err := tr.RunTx(ctx, tx, def)
	is.NoError(err)
	is.NoError(tx.Commit(ctx))

	got, err = m.Get(ctx, committed.ID)
	is.NoError(err)
	is.NotNil(got, "a committed RunTx persists the workflow")
	awaitWorkflow(t, m, committed.ID, workflow.StateSucceeded)
}

// --- coexistence with queue and event ---------------------------------------

type coexistTask struct{}

func (coexistTask) Kind() string { return "wf.it.coexist" }

// TestWorkflowCoexistsWithQueueAndEvent proves the three sources live in one
// schema without interference: a queue job, an event delivery and a workflow
// all run to completion over one shared Core, and each Manager's stats count
// only its own source — the workflow's task jobs never fold into the queue or
// event totals.
func TestWorkflowCoexistsWithQueueAndEvent(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	q := newQueue(t, h)
	e := newEvent(t, h)
	wf := newWorkflow(t, h)
	ctx := context.Background()

	jobDone := make(chan struct{}, 1)
	is.NoError(queue.Register(q.Worker(), func(context.Context, itJob) error {
		jobDone <- struct{}{}
		return nil
	}))
	is.NoError(e.Publisher().Register(ctx, event.Subscription{Name: "sink", EventType: orderEvent{}.EventType(), MaxAttempts: 3}))
	evDone := make(chan struct{}, 1)
	is.NoError(event.RegisterFunc(e.Worker(), "sink", func(context.Context, orderEvent) error {
		evDone <- struct{}{}
		return nil
	}))
	is.NoError(workflow.Register(wf.Worker(), func(context.Context, coexistTask) (workflow.None, error) {
		return workflow.None{}, nil
	}))

	startWorker(t, q.Worker())
	startWorker(t, e.Worker())
	startWorker(t, wf.Worker())

	res, err := wf.Client().Run(ctx, workflow.Define("it-coexist").Task("t", coexistTask{}))
	is.NoError(err)
	_, err = q.Producer().Enqueue(ctx, itJob{V: "shared"})
	is.NoError(err)
	_, err = e.Publisher().Publish(ctx, orderEvent{Amount: 1})
	is.NoError(err)

	for _, ch := range []chan struct{}{jobDone, evDone} {
		select {
		case <-ch:
		case <-time.After(wfSettle):
			t.Fatal("the queue job and event delivery did not both complete")
		}
	}
	awaitWorkflow(t, wf.Manager(), res.ID, workflow.StateSucceeded)

	// Stats stay partitioned by source: neither the queue nor the event totals
	// absorb the workflow's task job.
	require.Eventually(t, func() bool {
		qs, qerr := q.Manager().Stats(ctx, itJob{}.Kind())
		is.NoError(qerr)
		es, eerr := e.Manager().Stats(ctx)
		is.NoError(eerr)
		return qs.Succeeded == 1 && es.Succeeded == 1 && es.Events == 1
	}, wfSettle, 50*time.Millisecond)

	qs, err := q.Manager().Stats(ctx, itJob{}.Kind())
	is.NoError(err)
	is.EqualValues(1, qs.Succeeded, "the queue counts only its own job")
	es, err := e.Manager().Stats(ctx)
	is.NoError(err)
	is.EqualValues(1, es.Events, "the event ledger counts only its own event")

	page, err := wf.Manager().List(ctx, workflow.Filter{Name: "it-coexist"}, 0, 50)
	is.NoError(err)
	is.EqualValues(1, page.Total, "the workflow is visible only through the workflow Manager")
}

// stringError is a tiny error type so these tests avoid importing errors just
// to build a fixed message.
type stringError string

func (e stringError) Error() string { return string(e) }
