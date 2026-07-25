package workflow

import (
	"context"
	"fmt"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/workflow/kernel"

	"github.com/google/uuid"
)

// Context is the deterministic execution context a workflow function
// receives: the standard context.Context surface (cancellation, deadlines)
// plus workflow identity and the backend clock captured once for this
// replay pass. Workflow code must read time through Now, never time.Now —
// determinism requires every replay of the same history to see the same
// clock reading at the same decision point.
type Context interface {
	context.Context

	// WorkflowID returns the stable identity of this execution.
	WorkflowID() uuid.UUID
	// RunID returns the current processing epoch (MVP: equals WorkflowID;
	// see docs/workflow-v1-spec.md §1).
	RunID() uuid.UUID
	// Now returns the backend time captured once at the start of this replay
	// pass. Stable across every primitive call within the same pass.
	Now() time.Time
}

// wfContext is the concrete Context the worker builds for one replay pass.
// It is not exported: workflow code only ever sees it through the Context
// interface, and the package's primitives (ExecuteOperation, Sleep,
// WaitSignal, Select) recover the concrete type to reach the replay
// machinery below.
//
// ctx is retained so Operation handlers and store calls observe worker
// cancellation; forwarding methods satisfy context.Context without embedding.
//
//nolint:containedctx // must forward worker cancellation into Operations/store
type wfContext struct {
	ctx context.Context

	workflowID    uuid.UUID
	workflowName  string
	store         driver.WorkflowStore
	operations    map[string]operationFunc
	opMaxAttempts int

	// history holds only the decision events ExecuteOperation and Sleep
	// produce and consume through cursor (OperationScheduled /
	// OperationCompleted / OperationFailed, TimerStarted/TimerFired).
	// SignalReceived is deliberately excluded:
	// Client.Signal appends it directly to the store, out of band from any
	// replay pass, so it can land between a Sleep's TimerStarted and its
	// TimerFired — a cursor walk that included it would misread that
	// interleaving as a replay mismatch. See signals below and
	// docs/workflow-v1-spec.md §9-10.
	history *kernel.History
	cursor  *kernel.Cursor
	now     time.Time

	// signals holds every SignalReceived event, scanned out-of-band by
	// probeSignal rather than through the sequential cursor: a signal can
	// be delivered (and so recorded) at any point relative to the
	// workflow's code position — "early", before the matching WaitSignal
	// call ever runs (§9) — so its position carries no relationship to the
	// call's position in the decision sequence.
	signals []kernel.Event

	// consumedSignals marks, by index into signals, which SignalReceived
	// events this pass already matched to a WaitSignal / Select call — a
	// signal is consumed at most once per pass.
	consumedSignals map[int]bool
}

func (c *wfContext) Deadline() (time.Time, bool) { return c.ctx.Deadline() }
func (c *wfContext) Done() <-chan struct{}       { return c.ctx.Done() }
func (c *wfContext) Err() error                  { return c.ctx.Err() }
func (c *wfContext) Value(key any) any           { return c.ctx.Value(key) }
func (c *wfContext) WorkflowID() uuid.UUID       { return c.workflowID }

// RunID equals WorkflowID in the MVP (see docs/workflow-v1-spec.md §1).
func (c *wfContext) RunID() uuid.UUID { return c.workflowID }

func (c *wfContext) Now() time.Time { return c.now }

// asInternal recovers the concrete *wfContext behind a Context, panicking
// with a clear message when ctx was not produced by this runtime's worker —
// the same programmer-error signal a nil-map write would give, since a
// primitive called with a foreign Context could never make progress anyway.
func asInternal(ctx Context) *wfContext {
	wc, ok := ctx.(*wfContext)
	if !ok {
		panic("workflow: ctx was not produced by this runtime's Worker (do not wrap or reconstruct the workflow.Context)")
	}
	return wc
}

// parkInfo describes what would make a currently-unready future ready.
type parkInfo struct {
	// wakeAt is when a durable timer will fire, if this park is (at least
	// partly) waiting on one. Zero means "no timer": only an external Signal
	// can wake this workflow task.
	wakeAt time.Time
}

// parkSignal unwinds the workflow function's call stack via panic/recover
// once Select determines no future is ready: the deterministic, minimal
// substitute for a coroutine-based await in a fully synchronous replay
// model. The worker recovers it in runWorkflow and reschedules accordingly.
type parkSignal struct {
	info parkInfo
}

// park unwinds to the worker's recover point with info describing how this
// workflow task can make progress again.
func (c *wfContext) park(info parkInfo) {
	panic(parkSignal{info: info})
}

// record persists one history event and advances the cursor past it, so the
// cursor's "caught up" invariant (idx == len(history.Events)) holds after
// any fresh append within this pass — a later primitive call in the same
// pass must not re-read this event as if it belonged to its own decision
// point.
func (c *wfContext) record(typ kernel.EventType, payload []byte) kernel.Event {
	seq, err := c.store.AppendHistory(c.ctx, c.workflowID, string(typ), payload)
	if err != nil {
		panic(fmt.Errorf("workflow: append history %s: %w", typ, err))
	}
	ev := kernel.Event{Seq: seq, Type: typ, Payload: payload}
	c.history.Events = append(c.history.Events, ev)
	if _, ok := c.cursor.Next(); !ok {
		panic(fmt.Errorf("workflow: internal: cursor did not advance after recording %s", typ))
	}
	return ev
}
