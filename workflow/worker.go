package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
	"github.com/kausys/azync/workflow/kernel"

	"github.com/google/uuid"
)

// workflowTaskKind is the job kind a workflow's tasks route through:
// "$wf:" + the workflow's registered name (never its version — a job only
// ever carries the WorkflowID via its RunID column, and the execution
// header, not the job, is the source of truth for which registered version
// replays it).
func workflowTaskKind(name string) string { return "$wf:" + name }

// failurePayload is the durable encoding of a WorkflowFailed history event.
type failurePayload struct {
	Reason string `json:"reason"`
}

// enqueueWorkflowTask schedules one workflow-task job for workflowID under
// name's kind, at runAt (zero means "now"), through the WorkflowStore
// capability (Source SourceWorkflow, RunID = workflowID; see
// driver.WorkflowStore.ScheduleTask) rather than the plain Store's Enqueue,
// which always produces a Source SourceQueue job. Client.Start and
// Client.Signal use it to schedule the first pass and to wake a parked one;
// Worker uses it to schedule a Sleep's follow-up pass.
func enqueueWorkflowTask(ctx context.Context, store driver.WorkflowStore, name string, workflowID uuid.UUID, runAt time.Time) error {
	if err := store.ScheduleTask(ctx, workflowID, workflowTaskKind(name), runAt); err != nil {
		return fmt.Errorf("workflow: schedule workflow task: %w", err)
	}
	return nil
}

// Worker replays registered workflow functions against their durable
// history and executes registered Operations, driven by workflow-task jobs
// (Source SourceWorkflow). Register every workflow and Operation with
// RegisterWorkflow / RegisterOperation before Start.
type Worker struct {
	store  driver.WorkflowStore
	jobs   driver.Store
	logger *slog.Logger
	cfg    config

	mu         sync.Mutex
	workflows  map[string]workflowFunc
	operations map[string]operationFunc

	started atomic.Bool
	done    chan struct{}
	// doneOnce guards done against a double-close if Start is ever called
	// more than once (not itself guarded against, matching prior behavior).
	doneOnce sync.Once
}

// Wait blocks until a Start call has returned, or ctx ends first, whichever
// comes first. If Start was never called, Wait returns immediately (there is
// nothing to wait for). Close uses Wait to avoid closing a shared store out
// from under an in-flight drain; callers coordinating their own shutdown
// (stop ctx, then Wait, then release other resources) should do the same.
func (w *Worker) Wait(ctx context.Context) error {
	if !w.started.Load() {
		return nil
	}
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// registeredNames returns the distinct workflow names currently registered
// (one workflowTaskKind fetch partition per name), sorted is not required —
// order does not matter since ProcessNext just needs to try each once.
func (w *Worker) registeredNames() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	seen := make(map[string]struct{}, len(w.workflows))
	names := make([]string, 0, len(w.workflows))
	for key := range w.workflows {
		name := key[:strings.LastIndex(key, "@")]
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	return names
}

func (w *Worker) lookupWorkflow(name, version string) (workflowFunc, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	fn, ok := w.workflows[registrationKey(name, version)]
	return fn, ok
}

// snapshotOperations returns the current operation registry for one replay
// pass. Registration is expected to complete before Start, so the copy is
// mostly defensive.
func (w *Worker) snapshotOperations() map[string]operationFunc {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]operationFunc, len(w.operations))
	maps.Copy(out, w.operations)
	return out
}

// Start runs the worker until ctx is cancelled: WithConcurrency parallel
// polling passes (each leasing and processing at most one job at a time,
// exactly like ProcessNext), a vacuum loop that removes terminal executions
// past WithRetention, and a reconciler loop that re-enqueues a
// workflow-task for any running execution ListStalledWorkflows finds with
// no live task (defense-in-depth; see its doc comment —
// StartWorkflow/SignalWorkflow already schedule their task atomically with
// their own effect).
//
// On cancellation, in-flight passes drain for up to WithShutdownDrain
// before their context is cancelled, then a final bounded grace period
// before Start gives up on them and returns anyway — the same shape as
// internal/engine's drain, and for the same reason: a handler that ignores
// cancellation must not hang shutdown forever. Start returns nil after a
// graceful shutdown, drained or not.
func (w *Worker) Start(ctx context.Context) error {
	w.started.Store(true)
	defer w.doneOnce.Do(func() { close(w.done) })

	var loops sync.WaitGroup
	loops.Go(func() { w.vacuumLoop(ctx) })
	loops.Go(func() { w.reconcileLoop(ctx) })

	// jobsCtx outlives ctx by the drain budget so an in-flight pass's
	// settlement (Ack/Release/Dead/AppendHistory/ScheduleTask/...) finishes
	// even after shutdown begins; process*Job runs on it, never on ctx
	// directly (see dequeueOperations/dequeueWorkflows).
	jobsCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()

	n := max(w.cfg.concurrency, 1)
	var pollers sync.WaitGroup
	for range n {
		pollers.Go(func() { w.pollLoop(ctx, jobsCtx) })
	}

	<-ctx.Done()
	loops.Wait()

	drained := make(chan struct{})
	go func() { pollers.Wait(); close(drained) }()
	select {
	case <-drained:
		return nil
	case <-time.After(w.cfg.shutdownDrain):
	}

	w.logger.Warn("workflow worker drain budget exhausted, cancelling in-flight passes")
	cancelJobs()
	select {
	case <-drained:
	case <-time.After(finalDrainGrace):
		// The abandoned passes are leaked (drained's goroutine keeps waiting
		// in the background), not killed: their jobs recover via the lease
		// reaper once the lease expires.
		w.logger.Error("workflow worker hard drain timeout exceeded, abandoning in-flight passes")
	}
	return nil
}

func (w *Worker) pollLoop(ctx, jobsCtx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		processed, err := w.processNext(ctx, jobsCtx)
		if err != nil && ctx.Err() == nil {
			w.logger.Error("workflow task processing failed", "error", err)
		}
		// Only a clean, successful pass retries immediately; any error —
		// including one already logged and settled inside processNext, such
		// as a replay failure's Reschedule — paces through the poll interval
		// instead of spinning at full speed against the store.
		if processed && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.cfg.fetchPollInterval):
		}
	}
}

// vacuumLoop removes terminal executions older than retention on a fixed cadence.
func (w *Worker) vacuumLoop(ctx context.Context) {
	if w.cfg.vacuumInterval <= 0 {
		return
	}
	ticker := time.NewTicker(w.cfg.vacuumInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.cfg.retention <= 0 {
				continue
			}
			if _, err := w.store.VacuumWorkflows(ctx, w.cfg.retention); err != nil && ctx.Err() == nil {
				w.logger.Error("workflow vacuum failed", "error", err)
			}
		}
	}
}

// reconcileBatch bounds one ListStalledWorkflows call so a large backlog is
// swept in bounded steps.
const reconcileBatch = 100

// reconcileLoop periodically finds running executions with no live task
// (ListStalledWorkflows) and re-enqueues a workflow-task to revive them.
// Defense-in-depth: StartWorkflow and SignalWorkflow already schedule their
// task atomically with their own effect (see their doc comments), so a
// stall found here means something outside that path — a row predating this
// guarantee, an operator deleting a job by hand, a driver bug — left an
// execution stranded.
func (w *Worker) reconcileLoop(ctx context.Context) {
	if w.cfg.reconcileInterval <= 0 {
		return
	}
	ticker := time.NewTicker(w.cfg.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.reconcileOnce(ctx)
		}
	}
}

func (w *Worker) reconcileOnce(ctx context.Context) {
	olderThan := w.cfg.leaseTTL * stalledAfterLeaseMultiple
	stalled, err := w.store.ListStalledWorkflows(ctx, olderThan, reconcileBatch)
	if err != nil {
		if ctx.Err() == nil {
			w.logger.Error("workflow reconcile: list stalled workflows failed", "error", err)
		}
		return
	}
	for _, sw := range stalled {
		w.logger.Warn("workflow reconciler reviving stalled execution: no live task found",
			"workflow_id", sw.ID.String(), "name", sw.Name)
		if err := enqueueWorkflowTask(ctx, w.store, sw.Name, sw.ID, time.Time{}); err != nil {
			w.logger.Error("workflow reconcile: re-enqueue failed",
				"workflow_id", sw.ID.String(), "name", sw.Name, "error", err)
		}
	}
}

// ProcessNext dequeues and processes at most one due workflow-task or
// Operation-task job, returning processed=false when none was ready. It is
// the synchronous building block Start loops on, and is directly useful in
// tests that want deterministic, one-step control over replay.
//
// It first promotes any due scheduled job (a durable Sleep or Operation
// retry_wait) to pending: unlike dag.Worker and queue.Worker, this worker
// has no separate background maintenance loop over
// [driver.Store.PromoteDue].
func (w *Worker) ProcessNext(ctx context.Context) (bool, error) {
	return w.processNext(ctx, ctx)
}

// processNext is ProcessNext's implementation. jobsCtx is what a leased
// job's actual processing (process*Job) runs on; ctx governs everything
// else (promote-due, dequeue, and the poll loop's own pacing). A direct
// ProcessNext call passes the same context for both, preserving its
// documented single-ctx behavior; Start's poll loop passes its long-lived
// jobsCtx (see Start's doc comment) so an in-flight settlement survives
// ctx's cancellation during the shutdown drain window.
//
// It first promotes any due scheduled job (a durable Sleep or Operation
// retry_wait) to pending: unlike dag.Worker and queue.Worker, this worker
// has no separate background maintenance loop over
// [driver.Store.PromoteDue].
func (w *Worker) processNext(ctx, jobsCtx context.Context) (bool, error) {
	names := w.registeredNames()
	opKinds := w.registeredOperationKinds()
	kinds := make([]string, 0, len(names)+len(opKinds))
	for _, name := range names {
		kinds = append(kinds, workflowTaskKind(name))
	}
	kinds = append(kinds, opKinds...)
	if len(kinds) > 0 {
		if _, err := w.jobs.PromoteDue(ctx, driver.SourceWorkflow, kinds); err != nil {
			return false, fmt.Errorf("workflow: promote due workflow tasks: %w", err)
		}
	}

	switch w.cfg.workerMode {
	case WorkerModeOperationOnly:
		return w.dequeueOperations(ctx, jobsCtx, opKinds)
	case WorkerModeWorkflowOnly:
		return w.dequeueWorkflows(ctx, jobsCtx, names)
	default: // combined: prefer Operations so parked workflows unblock promptly
		if ok, err := w.dequeueOperations(ctx, jobsCtx, opKinds); ok || err != nil {
			return ok, err
		}
		return w.dequeueWorkflows(ctx, jobsCtx, names)
	}
}

func (w *Worker) dequeueOperations(ctx, jobsCtx context.Context, kinds []string) (bool, error) {
	for _, kind := range kinds {
		jobs, err := w.jobs.DequeueBatch(ctx, driver.SourceWorkflow, driver.DequeueParams{
			Kind:               kind,
			Limit:              1,
			Lease:              w.cfg.leaseTTL,
			DefaultMaxAttempts: w.cfg.defaultMaxAttempts,
			OverrideDefault:    true,
		})
		if err != nil {
			return false, fmt.Errorf("workflow: dequeue operation %q: %w", kind, err)
		}
		if len(jobs) == 0 {
			continue
		}
		return true, w.processOperationJob(jobsCtx, jobs[0])
	}
	return false, nil
}

func (w *Worker) dequeueWorkflows(ctx, jobsCtx context.Context, names []string) (bool, error) {
	for _, name := range names {
		jobs, err := w.jobs.DequeueBatch(ctx, driver.SourceWorkflow, driver.DequeueParams{
			Kind:               workflowTaskKind(name),
			Limit:              1,
			Lease:              w.cfg.leaseTTL,
			DefaultMaxAttempts: w.cfg.defaultMaxAttempts,
			OverrideDefault:    true,
		})
		if err != nil {
			return false, fmt.Errorf("workflow: dequeue %q: %w", name, err)
		}
		if len(jobs) == 0 {
			continue
		}
		return true, w.processJob(jobsCtx, jobs[0])
	}
	return false, nil
}

// processJob replays the workflow named by the job's execution (identified
// by the job's RunID, set by ScheduleTask) against its history and settles
// the job according to the outcome: acked on completion, failure or a clean
// park, dead-lettered on a lookup error that can never resolve, released
// (for a fresh lease elsewhere) on a transient store error or an internal
// replay error.
func (w *Worker) processJob(ctx context.Context, job driver.Job) error {
	workflowID := job.RunID

	view, err := w.store.GetWorkflowExecution(ctx, workflowID)
	if err != nil {
		if driver.IsNotFound(err) {
			return w.jobs.Dead(ctx, job.ID, job.LeaseToken, "workflow: execution not found")
		}
		return w.jobs.Release(ctx, job.ID, job.LeaseToken)
	}
	if isTerminalWorkflowState(view.State) {
		return w.jobs.Ack(ctx, job.ID, job.LeaseToken) // a stale wakeup arrived after the run already settled
	}
	if view.State == driver.WorkflowSuspended {
		// Parked for uncertain Operation / operator; do not replay until resumed.
		return w.jobs.Ack(ctx, job.ID, job.LeaseToken)
	}

	fn, ok := w.lookupWorkflow(view.Name, view.Version)
	if !ok {
		reason := unknownWorkflowError(view.Name, view.Version).Error()
		if err := w.store.FailWorkflow(ctx, workflowID, reason); err != nil {
			return w.jobs.Release(ctx, job.ID, job.LeaseToken)
		}
		w.recordTerminal(ctx, workflowID, kernel.EventWorkflowFailed, failurePayload{Reason: reason})
		return w.jobs.Dead(ctx, job.ID, job.LeaseToken, reason)
	}

	events, err := w.store.ListHistory(ctx, workflowID)
	if err != nil {
		return w.jobs.Release(ctx, job.ID, job.LeaseToken)
	}
	decisions, signals := splitHistory(events)
	hist := &kernel.History{Events: decisions}
	wc := &wfContext{
		ctx:             ctx,
		workflowID:      workflowID,
		workflowName:    view.Name,
		store:           w.store,
		operations:      w.snapshotOperations(),
		opMaxAttempts:   w.cfg.defaultMaxAttempts,
		history:         hist,
		signals:         signals,
		cursor:          kernel.NewCursor(hist),
		now:             time.Now(),
		consumedSignals: map[int]bool{},
	}

	outcome := w.runWorkflow(wc, fn, view.Input)
	switch outcome.kind {
	case outcomeCompleted:
		if err := w.store.CompleteWorkflow(ctx, workflowID, outcome.result); err != nil {
			return w.jobs.Release(ctx, job.ID, job.LeaseToken)
		}
		w.recordTerminal(ctx, workflowID, kernel.EventWorkflowCompleted, json.RawMessage(orNullJSON(outcome.result)))
		return w.jobs.Ack(ctx, job.ID, job.LeaseToken)

	case outcomeFailed:
		reason := outcome.err.Error()
		if err := w.store.FailWorkflow(ctx, workflowID, reason); err != nil {
			return w.jobs.Release(ctx, job.ID, job.LeaseToken)
		}
		w.recordTerminal(ctx, workflowID, kernel.EventWorkflowFailed, failurePayload{Reason: reason})
		return w.jobs.Ack(ctx, job.ID, job.LeaseToken)

	case outcomeParked:
		// Timer parks schedule a delayed wake. Operation parks are woken by
		// the Operation executor (or ResolveUncertain) via ScheduleTask.
		if !outcome.park.wakeAt.IsZero() {
			if err := enqueueWorkflowTask(ctx, w.store, view.Name, workflowID, outcome.park.wakeAt); err != nil {
				return w.jobs.Release(ctx, job.ID, job.LeaseToken)
			}
		}
		return w.jobs.Ack(ctx, job.ID, job.LeaseToken)

	default: // outcomeError: an internal replay failure, not a workflow failure.
		return w.settleReplayError(ctx, job, workflowID, outcome.err)
	}
}

// settleReplayError handles an internal replay failure (a determinism
// violation, a panic in the workflow function, a corrupt history record) —
// never a failure the workflow function itself returned. Release (the
// original handling here) never consumes the retry budget and reschedules
// with run_at = now(), so a persistent replay failure spun this job at full
// speed against the store forever (pollLoop's immediate-continue made it
// worse, see the fix there). This settles like any other handler failure
// instead: Reschedule with exponential backoff consumes an attempt, and
// exhausting the budget suspends the execution — alertable, preserving
// history — rather than retrying forever.
func (w *Worker) settleReplayError(ctx context.Context, job driver.Job, workflowID uuid.UUID, replayErr error) error {
	logger := w.logger.With("workflow_id", workflowID.String(), "job", job.ID.String(), "attempt", job.Attempt)

	if job.Attempt < job.MaxAttempts {
		logger.Error("workflow replay failed, rescheduling with backoff", "error", replayErr)
		err := w.jobs.Reschedule(ctx, job.ID, job.LeaseToken, engine.Backoff(job.Attempt), replayErr.Error())
		if driver.IsNotFound(err) {
			return nil
		}
		return err
	}

	reason := fmt.Sprintf("workflow: replay failed after %d attempts: %v", job.Attempt, replayErr)
	logger.Error("workflow replay exhausted retries, suspending execution", "error", replayErr)
	if err := w.jobs.Dead(ctx, job.ID, job.LeaseToken, reason); err != nil {
		if driver.IsNotFound(err) {
			return nil
		}
		return err
	}
	if err := w.store.SuspendWorkflow(ctx, workflowID, reason); err != nil {
		logger.Error("workflow: suspend after replay exhaustion failed", "error", err)
	}
	return nil
}

// recordTerminal appends a terminal history marker best-effort: the
// execution row (settled by CompleteWorkflow/FailWorkflow just before this
// call) is the authoritative record either way, so a failure to append the
// marker itself is logged, not propagated.
func (w *Worker) recordTerminal(ctx context.Context, workflowID uuid.UUID, typ kernel.EventType, payload any) {
	raw, err := json.Marshal(payload)
	if err != nil {
		w.logger.Error("workflow: marshal terminal event", "type", string(typ), "error", err)
		return
	}
	if _, err := w.store.AppendHistory(ctx, workflowID, string(typ), raw); err != nil {
		w.logger.Error("workflow: append terminal event", "type", string(typ), "error", err)
	}
}

// orNullJSON maps a nil/empty result to the JSON null literal so it always
// marshals as a value (never re-marshaled through json.Marshal, which would
// double-encode an already-encoded json.RawMessage).
func orNullJSON(raw json.RawMessage) []byte {
	if len(raw) == 0 {
		return []byte("null")
	}
	return raw
}

// splitHistory adapts durable history to the two views a replay pass needs:
// decisions, the sequential trail ExecuteOperation and Sleep replay through
// their cursor, and signals, every SignalReceived event probed out of band
// by WaitSignal/Select (see wfContext.signals for why the two must not
// share one cursor-walked slice). Every other marker event (WorkflowStarted,
// and any terminal marker a stale history might carry) belongs to neither:
// it is not a decision point any primitive's cursor ever expects to see —
// WorkflowStarted always precedes the first real decision event, and a
// cursor walk that included it would panic on ExecuteOperation/Sleep's very
// first call by finding the wrong type at index 0.
func splitHistory(events []driver.HistoryEvent) ([]kernel.Event, []kernel.Event) {
	decisions := make([]kernel.Event, 0, len(events))
	var signals []kernel.Event
	for _, e := range events {
		typ := kernel.EventType(e.Type)
		ev := kernel.Event{Seq: e.Seq, Type: typ, Payload: e.Payload}
		switch {
		case typ == kernel.EventSignalReceived:
			signals = append(signals, ev)
		case isMarkerEvent(typ):
			// neither a decision nor a signal; dropped.
		default:
			decisions = append(decisions, ev)
		}
	}
	return decisions, signals
}

// isMarkerEvent reports whether typ is a non-decision marker recorded
// directly on the execution's lifecycle transitions rather than by a
// workflow primitive replaying against the cursor.
func isMarkerEvent(typ kernel.EventType) bool {
	switch typ {
	case kernel.EventWorkflowStarted,
		kernel.EventWorkflowCompleted,
		kernel.EventWorkflowFailed,
		kernel.EventWorkflowCancelled,
		kernel.EventWorkflowSuspended:
		return true
	default:
		return false
	}
}

func isTerminalWorkflowState(state driver.WorkflowState) bool {
	switch state {
	case driver.WorkflowSucceeded, driver.WorkflowFailed, driver.WorkflowCancelled:
		return true
	default:
		return false
	}
}

// outcomeKind classifies how one replay pass of a workflow function ended.
type outcomeKind int

const (
	outcomeCompleted outcomeKind = iota
	outcomeFailed
	outcomeParked
	outcomeError
)

// runOutcome is runWorkflow's classification of one replay pass.
type runOutcome struct {
	kind   outcomeKind
	result json.RawMessage
	err    error
	park   parkInfo
}

// runWorkflow calls fn to (re)play the workflow function over wc's history,
// recovering the panic Select uses to park (see context.go) and classifying
// any other panic as an internal replay error rather than letting it crash
// the process — a workflow function is untrusted user code running inside
// the worker's own goroutine.
//
// Named return is required: recover in a defer can only mutate the value the
// surrounding function returns via a named result (Go recovers by returning
// normally with whatever the named results currently hold).
//
//nolint:nonamedreturns // recover-to-return requires a named result
func (w *Worker) runWorkflow(wc *wfContext, fn workflowFunc, input json.RawMessage) (out runOutcome) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if ps, ok := r.(parkSignal); ok {
			out = runOutcome{kind: outcomeParked, park: ps.info}
			return
		}
		if err, ok := r.(error); ok {
			out = runOutcome{kind: outcomeError, err: err}
			return
		}
		out = runOutcome{kind: outcomeError, err: fmt.Errorf("workflow: panic: %v", r)}
	}()

	result, err := fn(wc, input)
	if err != nil {
		return runOutcome{kind: outcomeFailed, err: err}
	}
	return runOutcome{kind: outcomeCompleted, result: result}
}
