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
// 5-field or @descriptors). Missed occurrences are not backfilled on first
// acquisition: the leader starts counting from "now"; a leader that later
// re-acquires after briefly losing leadership resumes from where it left off
// instead (see cronLoop), so a failover window does not silently skip a due
// occurrence.
//
// opts must not set an idempotency key (IdempotencyKey or IdempotencyKeyTTL):
// cron already dedupes each occurrence with its own key, and a caller
// override would replace that key, disabling cron's only defense against a
// duplicate fire during a leadership handover.
func (w *Worker) RegisterCron(name, spec string, args JobArgs, opts ...EnqueueOption) error {
	if w.engine.Started() {
		return errors.New("queue: cannot register cron after Start")
	}
	if _, exists := w.cron.entries[name]; exists {
		return fmt.Errorf("queue: cron %q already registered", name)
	}
	if cronOptsSetIdempotencyKey(opts) {
		return fmt.Errorf(
			"queue: cron %q: IdempotencyKey/IdempotencyKeyTTL are reserved for the occurrence key", name)
	}
	schedule, err := cronSpecParser.Parse(spec)
	if err != nil {
		return fmt.Errorf("queue: cron %q spec: %w", name, err)
	}
	w.cron.entries[name] = &cronEntry{name: name, schedule: schedule, args: args, opts: opts}
	return nil
}

// cronOptsSetIdempotencyKey reports whether opts, applied in isolation, would
// set an idempotency key.
func cronOptsSetIdempotencyKey(opts []EnqueueOption) bool {
	var probe enqueueOptions
	for _, opt := range opts {
		opt(&probe)
	}
	return probe.idemKey != ""
}

// cronLease is the minimal leadership surface cronLoop drives, satisfied
// either by a real driver.LeadershipLease or by leaderElectorLease adapting
// the plain driver.LeaderElector fallback to the same shape.
type cronLease interface {
	Valid(ctx context.Context) bool
	Release()
}

// leaderElectorLease adapts a plain driver.LeaderElector's fire-and-forget
// release into the cronLease shape: Valid always reports true, since a bare
// LeaderElector has no way to detect a lost leadership — this is exactly the
// latch-forever behavior LeaseElector exists to fix.
type leaderElectorLease struct{ release func() }

func (leaderElectorLease) Valid(context.Context) bool { return true }
func (l leaderElectorLease) Release()                 { l.release() }

func (w *Worker) cronLoop(ctx context.Context, elector driver.LeaderElector) {
	logger := w.logger
	ticker := time.NewTicker(w.cfg.cronTick)
	defer ticker.Stop()

	leaseElector, hasLease := elector.(driver.LeaseElector)

	var lease cronLease
	defer func() {
		if lease != nil {
			lease.Release()
		}
	}()

	acquire := func() (cronLease, bool, error) {
		if hasLease {
			return leaseElector.AcquireLeadershipLease(ctx, cronLeadershipName)
		}
		release, ok, err := elector.AcquireLeadership(ctx, cronLeadershipName)
		if err != nil || !ok {
			return nil, ok, err
		}
		return leaderElectorLease{release: release}, true, nil
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if lease != nil && !lease.Valid(ctx) {
			logger.Warn("cron leadership lost, re-acquiring")
			lease.Release()
			lease = nil
		}

		if lease == nil {
			l, ok, err := acquire()
			if err != nil {
				if ctx.Err() == nil {
					logger.Error("cron lock attempt failed", "error", err)
				}
				continue
			}
			if !ok {
				continue // another worker leads
			}
			lease = l
			now := w.now()
			for _, e := range w.cron.entries {
				if e.next.IsZero() {
					// First acquisition ever: missed occurrences are not
					// backfilled, start counting from now.
					e.next = e.schedule.Next(now)
				}
				// A re-acquisition after briefly losing leadership keeps the
				// stored e.next as-is (set by the previous leader, or by this
				// same instance before it lost the lease) so a failover
				// window does not skip an occurrence that fell inside it —
				// as long as it is still within the dedupe window, cron's
				// idempotency key still protects against a double fire.
			}
			logger.Info("cron leadership acquired", "schedules", len(w.cron.entries))
		}

		now := w.now()
		for _, e := range w.cron.entries {
			for !e.next.After(now) {
				occurrence := e.next
				key := cronOccurrenceKey(e.name, e.args.Kind(), occurrence)
				opts := append(append([]EnqueueOption{}, e.opts...), IdempotencyKeyTTL(key, cronOccurrenceWindow))
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
