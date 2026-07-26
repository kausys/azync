package queue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
)

// Worker is the job runtime: per-kind fetch loops feeding an executor pool,
// the maintenance loops (promotion, reaper, vacuums), and the leader-elected
// cron scheduler. Handlers register via Register before Start.
type Worker struct {
	engine   *engine.Engine
	cfg      config
	store    driver.Store
	logger   *slog.Logger
	producer *Producer
	cron     *cronRegistry

	// now is the cron scheduling clock; injectable in tests.
	now func() time.Time
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
// execute, settle, maintenance) plus the cron leader loop when cron schedules
// are registered and the driver supports leader election. On cancellation
// in-flight jobs drain for up to the shutdown drain budget.
//
// Start fails immediately, without running anything, if cron schedules are
// registered but the driver has no leader-election capability: running cron
// schedules unelected would enqueue every occurrence once per process, not
// once per cluster. Disable cron explicitly with WithCron(false) to run
// without it on such a driver.
func (w *Worker) Start(ctx context.Context) error {
	var elector driver.LeaderElector
	if w.cfg.cronEnabled && len(w.cron.entries) > 0 {
		var ok bool
		elector, ok = w.store.(driver.LeaderElector)
		if !ok {
			return fmt.Errorf(
				"queue: %d cron schedule(s) registered but driver %T has no leader-election capability; use WithCron(false) to run without cron",
				len(w.cron.entries), w.store)
		}
	}

	cronCtx, cancelCron := context.WithCancel(ctx)
	defer cancelCron()

	var cronLoops sync.WaitGroup
	if elector != nil {
		cronLoops.Go(func() { w.cronLoop(cronCtx, elector) })
	}

	err := w.engine.Start(ctx)
	cancelCron()
	cronLoops.Wait()
	return err
}
