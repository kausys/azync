package dag

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
)

// Worker is the DAG task runtime: per-kind fetch loops feeding an executor
// pool on the shared engine, the engine's maintenance loops (promotion,
// reaper, vacuums — scoped to the dag source), and the DAG scheduler
// loop that drives the DAG machinery. Handlers register via Register /
// RegisterKind before Start; the internal Sleep and WaitSignal tasks are never
// registered — the scheduler resolves them without running any handler.
type Worker struct {
	engine *engine.Engine
	cfg    config
	store  driver.DAGStore
	logger *slog.Logger
}

// Ready closes after wakeup setup succeeds and the polling loops are running.
// Polling-only workers become ready immediately after Start.
func (w *Worker) Ready() <-chan struct{} { return w.engine.Ready() }

// Wait blocks until a Start call has returned, or ctx ends first, whichever
// comes first. If Start was never called, Wait returns immediately (there is
// nothing to wait for). Close uses Wait to avoid closing a shared store out
// from under an in-flight drain; callers coordinating their own shutdown
// (stop ctx, then Wait, then release other resources) should do the same.
func (w *Worker) Wait(ctx context.Context) error {
	if !w.engine.Started() {
		return nil
	}
	select {
	case <-w.engine.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Start runs the worker until ctx is cancelled: the shared engine (fetch,
// execute, settle, maintenance) plus the DAG scheduler loop. The
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
// dags on a slower cadence.
func (w *Worker) schedulerLoop(ctx context.Context) {
	tick := time.NewTicker(w.cfg.schedulerTick)
	defer tick.Stop()
	vacuum := time.NewTicker(w.cfg.dagVacuumInterval)
	defer vacuum.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-tick.C:
			w.schedulerPass(ctx)

		case <-vacuum.C:
			if w.cfg.dagRetention <= 0 {
				continue // retain forever
			}
			if _, err := w.store.VacuumDAGs(ctx, w.cfg.dagRetention); err != nil && ctx.Err() == nil {
				w.logger.Error("dag vacuum failed", "error", err)
			}
		}
	}
}

// schedulerPass runs one set-based scheduler pass, in the contract's order:
// PromoteUnblocked releases tasks whose dependencies settled,
// DeliverBufferedSignals hands buffered signals to tasks that just became
// deliverable (a signal buffered while its task was blocked lands here, and a
// woken $sleep is due for the very next step), CompleteDueSleeps finishes due
// timers, ApplyFailurePolicy reacts to dead tasks, and only then CompleteDAGs
// settles finished dags. The order is load-bearing — completion decides
// succeeded/failed from what the policy left behind, so the policy MUST run
// first; never reorder these calls.
func (w *Worker) schedulerPass(ctx context.Context) {
	if _, err := w.store.PromoteUnblocked(ctx); err != nil && ctx.Err() == nil {
		w.logger.Error("dag promote unblocked failed", "error", err)
	}
	if _, err := w.store.DeliverBufferedSignals(ctx); err != nil && ctx.Err() == nil {
		w.logger.Error("dag deliver buffered signals failed", "error", err)
	}
	if _, err := w.store.CompleteDueSleeps(ctx); err != nil && ctx.Err() == nil {
		w.logger.Error("dag complete due sleeps failed", "error", err)
	}
	failures, err := w.store.ApplyFailurePolicy(ctx)
	if err != nil && ctx.Err() == nil {
		w.logger.Error("dag apply failure policy failed", "error", err)
	}
	for _, failure := range failures {
		w.logger.Warn("dag failure policy applied",
			"dag_id", failure.DAGID.String(),
			"policy", string(failure.Policy),
			"dead_tasks", failure.DeadTasks)
	}
	if _, err := w.store.CompleteDAGs(ctx); err != nil && ctx.Err() == nil {
		w.logger.Error("dag completion failed", "error", err)
	}
}
