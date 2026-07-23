package queue

import (
	"context"
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

	cronWarnOnce sync.Once
	// now is the cron scheduling clock; injectable in tests.
	now func() time.Time
}

// Ready closes after wakeup setup succeeds and the polling loops are running.
// Polling-only workers become ready immediately after Start.
func (w *Worker) Ready() <-chan struct{} { return w.engine.Ready() }

// Start runs the worker until ctx is cancelled: the shared engine (fetch,
// execute, settle, maintenance) plus the cron leader loop when cron schedules
// are registered and the driver supports leader election. On cancellation
// in-flight jobs drain for up to the shutdown drain budget.
func (w *Worker) Start(ctx context.Context) error {
	cronCtx, cancelCron := context.WithCancel(ctx)
	defer cancelCron()

	var cronLoops sync.WaitGroup
	if w.cfg.cronEnabled && len(w.cron.entries) > 0 {
		if elector, ok := w.store.(driver.LeaderElector); ok {
			cronLoops.Go(func() { w.cronLoop(cronCtx, elector) })
		} else {
			w.cronWarnOnce.Do(func() {
				w.logger.Warn("cron disabled: driver has no leader-election capability",
					"schedules", len(w.cron.entries))
			})
		}
	}

	err := w.engine.Start(ctx)
	cancelCron()
	cronLoops.Wait()
	return err
}
