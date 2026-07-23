package engine

import (
	"context"
	"time"

	"github.com/kausys/azync/driver"
)

// fetchLoop drives one job kind: it reserves executor capacity BEFORE leasing
// (the invariant: a leased job always has an executor slot), dequeues a batch
// up to the reserved slots, and hands each job to the executor. Idle periods
// back off and wake on push notifications.
func (e *Engine) fetchLoop(ctx, jobsCtx context.Context, k Kind, wake <-chan struct{}) {
	semKind := make(sem, k.Concurrency)
	idle := e.settings.FetchCooldown

	for ctx.Err() == nil {
		// Block for the first slot pair (kind + global).
		if !semKind.acquire(ctx) {
			return
		}
		if !e.semGlobal.acquire(ctx) {
			semKind.release()
			return
		}
		slots := 1
		// Opportunistically grab more pairs up to the batch size.
		maxBatch := min(e.settings.FetchBatchSize, k.Concurrency)
		for slots < maxBatch && semKind.tryAcquire() {
			if !e.semGlobal.tryAcquire() {
				semKind.release()
				break
			}
			slots++
		}

		jobs, err := e.store.DequeueBatch(ctx, e.source, driver.DequeueParams{
			Kind:               k.Name,
			Limit:              slots,
			Lease:              e.settings.LeaseTTL,
			DefaultMaxAttempts: k.MaxAttempts,
			OverrideDefault:    k.MaxAttemptsSet,
		})
		if err != nil && ctx.Err() == nil {
			e.logger.Error("dequeue failed", "kind", k.Name, "error", err)
		}

		// Release unused capacity, dispatch the rest.
		for range slots - len(jobs) {
			semKind.release()
			e.semGlobal.release()
		}
		for _, job := range jobs {
			e.inflight.Go(func() {
				e.execute(jobsCtx, k, job, func() {
					semKind.release()
					e.semGlobal.release()
				})
			})
		}

		if len(jobs) > 0 {
			idle = e.settings.FetchCooldown
			continue
		}

		// Idle: wait for a wake, the poll fallback, or shutdown.
		idle = min(idle*2, e.settings.IdleBackoffMax)
		wait := max(idle, e.settings.FetchPollInterval)
		select {
		case <-ctx.Done():
			return
		case <-wake:
			idle = e.settings.FetchCooldown
		case <-time.After(wait):
		}
	}
}
