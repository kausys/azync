package engine

import (
	"context"
	"time"
)

const (
	defaultPromoteInterval = time.Second
	defaultVacuumInterval  = time.Hour
)

// maintenanceLoop runs the background upkeep for every registered kind of this
// engine's source in a single statement per operation: scheduled->pending
// promotion, the lease reaper with its poison-job budget, and the
// stats/idempotency/completed-history vacuums.
func (e *Engine) maintenanceLoop(ctx context.Context, kinds []string) {
	promoteInterval := e.settings.PromoteInterval
	if promoteInterval <= 0 {
		promoteInterval = defaultPromoteInterval
	}
	vacuumInterval := e.settings.VacuumInterval
	if vacuumInterval <= 0 {
		vacuumInterval = defaultVacuumInterval
	}

	promote := time.NewTicker(promoteInterval)
	defer promote.Stop()
	reap := time.NewTicker(e.settings.LeaseTTL)
	defer reap.Stop()
	vacuum := time.NewTicker(vacuumInterval)
	defer vacuum.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-promote.C:
			if _, err := e.store.PromoteDue(ctx, e.source, kinds); err != nil && ctx.Err() == nil {
				e.logger.Error("promote due failed", "error", err)
			}

		case <-reap.C:
			reaped, killed, err := e.store.ReapExpired(ctx, e.source, kinds, e.settings.MaxReaps)
			if err != nil && ctx.Err() == nil {
				e.logger.Error("reap failed", "error", err)
			}
			if reaped > 0 {
				e.logger.Warn("reaped expired leases", "reaped", reaped, "dead", killed)
			}

		case <-vacuum.C:
			if _, err := e.store.VacuumStats(ctx, e.source, e.settings.StatsRetention); err != nil && ctx.Err() == nil {
				e.logger.Error("stats vacuum failed", "error", err)
			}
			if _, err := e.store.VacuumIdempotency(ctx, e.source); err != nil && ctx.Err() == nil {
				e.logger.Error("idempotency vacuum failed", "error", err)
			}
			if _, err := e.store.VacuumCompleted(ctx, e.source, e.settings.CompletedRetention); err != nil && ctx.Err() == nil {
				e.logger.Error("completed vacuum failed", "error", err)
			}
		}
	}
}
