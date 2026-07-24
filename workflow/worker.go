package workflow

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
)

// Worker is the workflow runtime: per-kind fetch loops feeding an executor
// pool on the shared engine, the engine's maintenance loops (promotion,
// reaper, vacuums — scoped to the workflow source), and the workflow scheduler
// loop that drives the DAG machinery. Handlers register via Register /
// RegisterKind before Start; the internal Sleep and WaitSignal tasks are never
// registered — the scheduler resolves them without running any handler.
type Worker struct {
	engine *engine.Engine
	cfg    config
	store  driver.WorkflowStore
	logger *slog.Logger
}

// Ready closes after wakeup setup succeeds and the polling loops are running.
// Polling-only workers become ready immediately after Start.
func (w *Worker) Ready() <-chan struct{} { return w.engine.Ready() }

// Start runs the worker until ctx is cancelled: the shared engine (fetch,
// execute, settle, maintenance) plus the workflow scheduler loop. The
// scheduler is set-based and idempotent, so every worker instance runs it on
// its own tick without leader election. On cancellation in-flight tasks drain
// for up to the shutdown drain budget.
func (w *Worker) Start(ctx context.Context) error {
	schedCtx, cancelSched := context.WithCancel(ctx)
	defer cancelSched()

	var loops sync.WaitGroup
	loops.Go(func() { w.schedulerLoop(schedCtx) })

	err := w.engine.Start(ctx)
	cancelSched()
	loops.Wait()
	return err
}

// schedulerLoop drives the DAG machinery on a fixed tick and vacuums terminal
// workflows on a slower cadence.
func (w *Worker) schedulerLoop(ctx context.Context) {
	tick := time.NewTicker(w.cfg.schedulerTick)
	defer tick.Stop()
	vacuum := time.NewTicker(w.cfg.workflowVacuumInterval)
	defer vacuum.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-tick.C:
			w.schedulerPass(ctx)

		case <-vacuum.C:
			if w.cfg.workflowRetention <= 0 {
				continue // retain forever
			}
			if _, err := w.store.VacuumWorkflows(ctx, w.cfg.workflowRetention); err != nil && ctx.Err() == nil {
				w.logger.Error("workflow vacuum failed", "error", err)
			}
		}
	}
}

// schedulerPass runs one set-based scheduler pass, in the contract's order:
// PromoteUnblocked releases tasks whose dependencies settled, CompleteDueSleeps
// finishes due timers, ApplyFailurePolicy reacts to dead tasks, and only then
// CompleteWorkflows settles finished workflows. The order is load-bearing —
// completion decides succeeded/failed from what the policy left behind, so the
// policy MUST run first; never reorder these calls.
func (w *Worker) schedulerPass(ctx context.Context) {
	if _, err := w.store.PromoteUnblocked(ctx); err != nil && ctx.Err() == nil {
		w.logger.Error("workflow promote unblocked failed", "error", err)
	}
	if _, err := w.store.CompleteDueSleeps(ctx); err != nil && ctx.Err() == nil {
		w.logger.Error("workflow complete due sleeps failed", "error", err)
	}
	failures, err := w.store.ApplyFailurePolicy(ctx)
	if err != nil && ctx.Err() == nil {
		w.logger.Error("workflow apply failure policy failed", "error", err)
	}
	for _, failure := range failures {
		w.logger.Warn("workflow failure policy applied",
			"workflow_id", failure.WorkflowID.String(),
			"policy", string(failure.Policy),
			"dead_tasks", failure.DeadTasks)
	}
	if _, err := w.store.CompleteWorkflows(ctx); err != nil && ctx.Err() == nil {
		w.logger.Error("workflow completion failed", "error", err)
	}
}
