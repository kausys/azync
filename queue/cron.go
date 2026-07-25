package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/robfig/cron/v3"
)

// Cron: periodic jobs enqueued by a single leader (driver.LeaderElector) with
// an idempotency window per occurrence — belt and braces: even two transient
// leaders produce exactly one job per occurrence.

// cronLeadershipName is the leadership handle cron loops compete for.
const cronLeadershipName = "cron"

// cronOccurrenceWindow is the dedupe window per occurrence key — it only needs
// to outlive a leader failover overlap.
const cronOccurrenceWindow = time.Hour

var cronSpecParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

type cronEntry struct {
	name     string
	schedule cron.Schedule
	args     JobArgs
	opts     []EnqueueOption
	next     time.Time
}

type cronRegistry struct {
	entries map[string]*cronEntry
}

func newCronRegistry() *cronRegistry {
	return &cronRegistry{entries: map[string]*cronEntry{}}
}

// RegisterCron schedules args to be enqueued on the cron spec (standard
// 5-field or @descriptors). Missed occurrences are not backfilled: the leader
// starts counting from "now".
func (w *Worker) RegisterCron(name, spec string, args JobArgs, opts ...EnqueueOption) error {
	if w.engine.Started() {
		return errors.New("queue: cannot register cron after Start")
	}
	if _, exists := w.cron.entries[name]; exists {
		return fmt.Errorf("queue: cron %q already registered", name)
	}
	schedule, err := cronSpecParser.Parse(spec)
	if err != nil {
		return fmt.Errorf("queue: cron %q spec: %w", name, err)
	}
	w.cron.entries[name] = &cronEntry{name: name, schedule: schedule, args: args, opts: opts}
	return nil
}

func (w *Worker) cronLoop(ctx context.Context, elector driver.LeaderElector) {
	logger := w.logger
	ticker := time.NewTicker(w.cfg.cronTick)
	defer ticker.Stop()

	var release func()
	leader := false
	defer func() {
		if release != nil {
			release()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if !leader {
			rel, ok, err := elector.AcquireLeadership(ctx, cronLeadershipName)
			if err != nil {
				if ctx.Err() == nil {
					logger.Error("cron lock attempt failed", "error", err)
				}
				continue
			}
			if !ok {
				continue // another worker leads
			}
			release, leader = rel, true
			now := w.now()
			for _, e := range w.cron.entries {
				e.next = e.schedule.Next(now)
			}
			logger.Info("cron leadership acquired", "schedules", len(w.cron.entries))
		}

		now := w.now()
		for _, e := range w.cron.entries {
			for !e.next.After(now) {
				occurrence := e.next
				key := cronOccurrenceKey(e.name, e.args.Kind(), occurrence)
				opts := append([]EnqueueOption{IdempotencyKeyTTL(key, cronOccurrenceWindow)}, e.opts...)
				if _, err := w.producer.Enqueue(ctx, e.args, opts...); err != nil && ctx.Err() == nil {
					logger.Error("cron enqueue failed", "schedule", e.name, "error", err)
					break // retry this occurrence next tick (the key dedupes)
				}
				e.next = e.schedule.Next(occurrence)
			}
		}
	}
}

func cronOccurrenceKey(entryName, kind string, occurrence time.Time) string {
	return "cron:" + entryName + ":" + kind + ":" + strconv.FormatInt(occurrence.Unix(), 10)
}
