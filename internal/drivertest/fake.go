// Package drivertest provides an in-memory [driver.Store] used by the queue and
// event runtimes' unit tests. Fake honors the real contract semantics — durable
// budget resolution, lease-token fencing, reap-to-dead, idempotency dedupe and
// atomic publish fan-out — so runtime logic can be tested without a database.
//
// It also implements the optional [driver.Notifier] and [driver.LeaderElector]
// capabilities so wake-driven fetch loops and leader-elected cron can be
// exercised in memory. It deliberately does not implement [driver.TxStore]: the
// fake has no transactions.
package drivertest

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/clock"

	"github.com/google/uuid"
)

// Fake is a thread-safe, in-memory driver.Store for tests.
type Fake struct {
	// Clock is the time source; tests may replace it with a controllable clock
	// before use. It defaults to clock.SystemClock.
	Clock clock.Clock

	mu          sync.Mutex
	jobs        map[uuid.UUID]*fakeJob
	events      map[uuid.UUID]driver.EventRecord
	subscribers map[subKey]*subRecord
	idempotency map[idemKey]time.Time // time-window keys -> expiry
	attempts    map[uuid.UUID][]driver.AttemptError
	stats       map[statKey]*statCounters
	leaders     map[string]bool

	wakeMu sync.Mutex
	wakers []chan driver.Wake
}

// NewFake returns an empty Fake backed by the system clock.
func NewFake() *Fake {
	return &Fake{
		Clock:       clock.SystemClock{},
		jobs:        map[uuid.UUID]*fakeJob{},
		events:      map[uuid.UUID]driver.EventRecord{},
		subscribers: map[subKey]*subRecord{},
		idempotency: map[idemKey]time.Time{},
		attempts:    map[uuid.UUID][]driver.AttemptError{},
		stats:       map[statKey]*statCounters{},
		leaders:     map[string]bool{},
	}
}

// fakeJob is the internal job record: the wire Job plus the two columns the
// contract does not expose (the resolved-budget flag and the idempotency key).
type fakeJob struct {
	driver.Job
	maxAttemptsExplicit bool
	idempotencyKey      string
}

type subKey struct {
	name      string
	eventType string
}

type subRecord struct {
	driver.Subscriber
	createdAt time.Time
	updatedAt time.Time
}

type idemKey struct {
	source driver.Source
	kind   string
	key    string
}

type statKey struct {
	source driver.Source
	kind   string
	day    time.Time
}

type statField int

const (
	statEnqueued statField = iota
	statProcessed
	statFailed
	statReaped
)

type statCounters struct {
	enqueued  int64
	processed int64
	failed    int64
	reaped    int64
}

// --- helpers -------------------------------------------------------------

func (f *Fake) now() time.Time { return f.Clock.Now() }

func clonePayload(p json.RawMessage) json.RawMessage {
	if p == nil {
		return nil
	}
	return append(json.RawMessage(nil), p...)
}

// cloneMeta returns a defensive copy of m. A nil input yields a non-nil empty
// map so meta read back from the store is never nil, matching the contract that
// Job.Meta and EventRecord.Meta are always a (possibly empty) map on reads.
func cloneMeta(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	maps.Copy(out, m)
	return out
}

func dayOf(t time.Time) time.Time {
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

// toJob returns a defensive copy so callers cannot mutate the stored record.
func (j *fakeJob) toJob() driver.Job {
	out := j.Job
	out.Payload = clonePayload(j.Payload)
	out.Meta = cloneMeta(j.Meta)
	out.Event = nil
	return out
}

func (f *Fake) recordAttempt(id uuid.UUID, attempt int, lastError, trace string, at time.Time) {
	f.attempts[id] = append(f.attempts[id], driver.AttemptError{
		Attempt: attempt,
		Error:   lastError,
		At:      at,
		Trace:   trace,
	})
}

func (f *Fake) bumpStat(source driver.Source, kind string, field statField, n int64, at time.Time) {
	k := statKey{source: source, kind: kind, day: dayOf(at)}
	c := f.stats[k]
	if c == nil {
		c = &statCounters{}
		f.stats[k] = c
	}
	switch field {
	case statEnqueued:
		c.enqueued += n
	case statProcessed:
		c.processed += n
	case statFailed:
		c.failed += n
	case statReaped:
		c.reaped += n
	}
}

// settle applies a lease-token-fenced transition, returning the job or a
// not-found error when the token no longer owns an active row. Callers hold f.mu.
func (f *Fake) settle(op string, id, leaseToken uuid.UUID) (*fakeJob, error) {
	j := f.jobs[id]
	if j == nil || j.State != driver.StateActive || j.LeaseToken != leaseToken {
		return nil, driver.NewNotFound(op)
	}
	return j, nil
}

// --- producers -----------------------------------------------------------

// Enqueue inserts one queue job, honoring both idempotency modes.
func (f *Fake) Enqueue(_ context.Context, p driver.EnqueueParams) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.enqueueLocked(p)
}

func (f *Fake) enqueueLocked(p driver.EnqueueParams) (bool, error) {
	now := f.now()
	if p.IdempotencyKey != "" {
		// Live-job dedupe: a live prior job with the key blocks a duplicate.
		for _, j := range f.jobs {
			if j.Source == driver.SourceQueue && j.Kind == p.Kind &&
				j.idempotencyKey == p.IdempotencyKey &&
				j.State != driver.StateDead && j.State != driver.StateSucceeded {
				return false, nil
			}
		}
		// Time-window dedupe: an unexpired reservation blocks a duplicate.
		if p.IdempotencyTTL > 0 {
			k := idemKey{source: driver.SourceQueue, kind: p.Kind, key: p.IdempotencyKey}
			if exp, ok := f.idempotency[k]; ok && exp.After(now) {
				return false, nil
			}
			f.idempotency[k] = now.Add(p.IdempotencyTTL)
		}
	}

	runAt := p.RunAt
	if runAt.IsZero() {
		runAt = now.Add(p.Delay)
	}
	state := driver.StatePending
	if runAt.After(now) {
		state = driver.StateScheduled
	}
	f.jobs[p.ID] = &fakeJob{
		Job: driver.Job{
			ID:          p.ID,
			Source:      driver.SourceQueue,
			Kind:        p.Kind,
			State:       state,
			MaxAttempts: p.MaxAttempts,
			Payload:     clonePayload(p.Payload),
			Meta:        cloneMeta(p.Meta),
			TraceID:     p.TraceID,
			SpanID:      p.SpanID,
			TraceFlags:  p.TraceFlags,
			RunAt:       runAt,
			EnqueuedAt:  now,
		},
		maxAttemptsExplicit: p.MaxAttemptsExplicit,
		idempotencyKey:      p.IdempotencyKey,
	}
	f.bumpStat(driver.SourceQueue, p.Kind, statEnqueued, 1, now)
	if state == driver.StatePending {
		f.wake(driver.SourceQueue, p.Kind)
	}
	return true, nil
}

// Publish appends the event and fans out one pending delivery per subscriber.
func (f *Fake) Publish(_ context.Context, p driver.PublishParams) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.publishLocked(p)
}

func (f *Fake) publishLocked(p driver.PublishParams) (int, error) {
	now := f.now()
	f.events[p.ID] = driver.EventRecord{
		ID:            p.ID,
		Type:          p.Type,
		TenantID:      p.TenantID,
		AggregateType: p.AggregateType,
		AggregateID:   p.AggregateID,
		Version:       p.Version,
		OccurredAt:    p.OccurredAt,
		Payload:       clonePayload(p.Payload),
		Meta:          cloneMeta(p.Meta),
		TraceID:       p.TraceID,
		SpanID:        p.SpanID,
		TraceFlags:    p.TraceFlags,
	}
	delivered := 0
	for _, sub := range f.subscribersFor(p.Type) {
		f.createDelivery(p.ID, sub.Name, sub.MaxAttempts, false, now)
		delivered++
	}
	return delivered, nil
}

// createDelivery inserts one pending event job for a subscriber. Callers hold
// f.mu. The delivery carries no payload; the body is rehydrated from the ledger
// on dequeue.
func (f *Fake) createDelivery(eventID uuid.UUID, subscriber string, maxAttempts int, replay bool, now time.Time) {
	id := uuid.New()
	f.jobs[id] = &fakeJob{
		Job: driver.Job{
			ID:          id,
			Source:      driver.SourceEvent,
			Kind:        subscriber,
			State:       driver.StatePending,
			MaxAttempts: maxAttempts,
			RunAt:       now,
			EventID:     eventID,
			Replay:      replay,
			EnqueuedAt:  now,
		},
		// Event deliveries carry an explicit per-subscriber budget, so the
		// queue-style first-lease default override never touches them.
		maxAttemptsExplicit: true,
	}
	f.bumpStat(driver.SourceEvent, subscriber, statEnqueued, 1, now)
	f.wake(driver.SourceEvent, subscriber)
}

// SeedOrphanDelivery inserts a pending event delivery job whose ledger event is
// absent, so a dequeue rehydrates it with a nil Event. It exists purely to let
// the event runtime's tests exercise the "delivery without a ledger record"
// dead-letter path, which Publish (always atomic with the ledger) cannot
// produce. deliveryID is the job's id; the missing event id is generated.
func (f *Fake) SeedOrphanDelivery(deliveryID uuid.UUID, subscriber string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	f.jobs[deliveryID] = &fakeJob{
		Job: driver.Job{
			ID:          deliveryID,
			Source:      driver.SourceEvent,
			Kind:        subscriber,
			State:       driver.StatePending,
			MaxAttempts: 1,
			RunAt:       now,
			EventID:     uuid.New(), // no matching ledger event
			EnqueuedAt:  now,
		},
		maxAttemptsExplicit: true,
	}
	f.bumpStat(driver.SourceEvent, subscriber, statEnqueued, 1, now)
	f.wake(driver.SourceEvent, subscriber)
}

// subscribersFor returns the registrations for an event type sorted by name.
// Callers hold f.mu.
func (f *Fake) subscribersFor(eventType string) []driver.Subscriber {
	var out []driver.Subscriber
	for _, s := range f.subscribers {
		if s.EventType == eventType {
			out = append(out, s.Subscriber)
		}
	}
	slices.SortFunc(out, func(a, b driver.Subscriber) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return out
}

// RegisterSubscriber upserts a subscriber keyed by (name, event type).
func (f *Fake) RegisterSubscriber(_ context.Context, sub driver.Subscriber) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	k := subKey{name: sub.Name, eventType: sub.EventType}
	if existing, ok := f.subscribers[k]; ok {
		existing.MaxAttempts = sub.MaxAttempts
		existing.updatedAt = now
		return nil
	}
	f.subscribers[k] = &subRecord{Subscriber: sub, createdAt: now, updatedAt: now}
	return nil
}

// Subscribers returns the registrations for an event type, ordered by name.
func (f *Fake) Subscribers(_ context.Context, eventType string) ([]driver.Subscriber, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.subscribersFor(eventType), nil
}

// --- fetch ---------------------------------------------------------------

// DequeueBatch leases up to Limit due pending jobs of (source, Kind).
func (f *Fake) DequeueBatch(_ context.Context, source driver.Source, p driver.DequeueParams) ([]driver.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p.Limit <= 0 {
		return nil, nil
	}
	now := f.now()

	var due []*fakeJob
	for _, j := range f.jobs {
		if j.Source == source && j.Kind == p.Kind && j.State == driver.StatePending && !j.RunAt.After(now) {
			due = append(due, j)
		}
	}
	slices.SortFunc(due, func(a, b *fakeJob) int {
		if c := a.RunAt.Compare(b.RunAt); c != 0 {
			return c
		}
		return bytes.Compare(a.ID[:], b.ID[:])
	})
	if len(due) > p.Limit {
		due = due[:p.Limit]
	}

	out := make([]driver.Job, 0, len(due))
	for _, j := range due {
		j.Attempt++
		if !j.maxAttemptsExplicit && p.OverrideDefault {
			j.MaxAttempts = p.DefaultMaxAttempts
		}
		// The first lease resolves the retry budget durably.
		j.maxAttemptsExplicit = true
		j.State = driver.StateActive
		j.LeaseToken = uuid.New()
		j.LeaseUntil = now.Add(p.Lease)

		leased := j.toJob()
		if source == driver.SourceEvent {
			if rec, ok := f.events[j.EventID]; ok {
				event := rec
				event.Payload = clonePayload(rec.Payload)
				event.Meta = cloneMeta(rec.Meta)
				leased.Event = &event
			}
		}
		out = append(out, leased)
	}
	return out, nil
}

// --- settlement ----------------------------------------------------------

// Ack completes an active job, retaining it as succeeded.
func (f *Fake) Ack(_ context.Context, id, leaseToken uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, err := f.settle("ack", id, leaseToken)
	if err != nil {
		return err
	}
	now := f.now()
	j.State = driver.StateSucceeded
	j.LeaseToken = uuid.Nil
	j.LeaseUntil = time.Time{}
	j.CompletedAt = now
	f.bumpStat(j.Source, j.Kind, statProcessed, 1, now)
	return nil
}

// Reschedule parks a failed active job for a later retry and records the attempt.
func (f *Fake) Reschedule(_ context.Context, id, leaseToken uuid.UUID, delay time.Duration, lastError string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, err := f.settle("reschedule", id, leaseToken)
	if err != nil {
		return err
	}
	now := f.now()
	j.State = driver.StateScheduled
	j.RunAt = now.Add(delay)
	j.LeaseToken = uuid.Nil
	j.LeaseUntil = time.Time{}
	j.LastError = lastError
	j.FailedAt = now
	f.recordAttempt(j.ID, j.Attempt, lastError, j.TraceID, now)
	f.bumpStat(j.Source, j.Kind, statFailed, 1, now)
	return nil
}

// Dead moves a failed active job to the dead-letter state and records the attempt.
func (f *Fake) Dead(_ context.Context, id, leaseToken uuid.UUID, lastError string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, err := f.settle("dead", id, leaseToken)
	if err != nil {
		return err
	}
	now := f.now()
	j.State = driver.StateDead
	j.LeaseToken = uuid.Nil
	j.LeaseUntil = time.Time{}
	j.LastError = lastError
	j.FailedAt = now
	f.recordAttempt(j.ID, j.Attempt, lastError, j.TraceID, now)
	f.bumpStat(j.Source, j.Kind, statFailed, 1, now)
	return nil
}

// Release returns a leased job to pending without recording an attempt,
// decrementing the attempt it did not really spend.
func (f *Fake) Release(_ context.Context, id, leaseToken uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, err := f.settle("release", id, leaseToken)
	if err != nil {
		return err
	}
	now := f.now()
	j.State = driver.StatePending
	j.Attempt = max(j.Attempt-1, 0)
	j.LeaseToken = uuid.Nil
	j.LeaseUntil = time.Time{}
	j.RunAt = now
	f.wake(j.Source, j.Kind)
	return nil
}

// ExtendLease renews an active job's lease.
func (f *Fake) ExtendLease(_ context.Context, id, leaseToken uuid.UUID, lease time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, err := f.settle("extend lease", id, leaseToken)
	if err != nil {
		return err
	}
	j.LeaseUntil = f.now().Add(lease)
	return nil
}

// --- maintenance ---------------------------------------------------------

// PromoteDue moves due scheduled jobs of the kinds to pending.
func (f *Fake) PromoteDue(_ context.Context, source driver.Source, kinds []string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(kinds) == 0 {
		return 0, nil
	}
	want := sliceSet(kinds)
	now := f.now()
	var promoted int64
	for _, j := range f.jobs {
		if j.Source == source && want[j.Kind] && j.State == driver.StateScheduled && !j.RunAt.After(now) {
			j.State = driver.StatePending
			promoted++
			f.wake(source, j.Kind)
		}
	}
	return promoted, nil
}

// ReapExpired reclaims active jobs whose lease expired, killing poison jobs.
func (f *Fake) ReapExpired(_ context.Context, source driver.Source, kinds []string, maxReaps int) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(kinds) == 0 {
		return 0, 0, nil
	}
	want := sliceSet(kinds)
	now := f.now()
	var reaped, killed int64
	perKind := map[string]int64{}
	for _, j := range f.jobs {
		if j.Source != source || !want[j.Kind] || j.State != driver.StateActive || !j.LeaseUntil.Before(now) {
			continue
		}
		j.ReapCount++
		reaped++
		perKind[j.Kind]++
		j.LeaseToken = uuid.Nil
		j.LeaseUntil = time.Time{}
		if j.ReapCount >= maxReaps {
			j.State = driver.StateDead
			j.LastError = "lease expired " + strconv.Itoa(j.ReapCount) + " times"
			j.FailedAt = now
			f.recordAttempt(j.ID, j.Attempt, j.LastError, j.TraceID, now)
			killed++
		} else {
			j.State = driver.StatePending
			f.wake(source, j.Kind)
		}
	}
	for kind, n := range perKind {
		f.bumpStat(source, kind, statReaped, n, now)
	}
	return reaped, killed, nil
}

// VacuumStats trims stat counters of the source older than retention.
func (f *Fake) VacuumStats(_ context.Context, source driver.Source, retention time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if retention <= 0 {
		return 0, nil
	}
	// A day is trimmed only when its whole span is older than the retention:
	// day < floor_day(now-retention) implies day+24h <= now-retention, so no
	// counter within the retention window is ever removed.
	cutoff := dayOf(f.now().Add(-retention))
	var removed int64
	for k := range f.stats {
		if k.source == source && k.day.Before(cutoff) {
			delete(f.stats, k)
			removed++
		}
	}
	return removed, nil
}

// VacuumIdempotency trims expired time-window dedupe keys of the source.
func (f *Fake) VacuumIdempotency(_ context.Context, source driver.Source) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	var removed int64
	for k, exp := range f.idempotency {
		if k.source == source && !exp.After(now) {
			delete(f.idempotency, k)
			removed++
		}
	}
	return removed, nil
}

// VacuumCompleted trims succeeded jobs of the source completed before retention ago.
func (f *Fake) VacuumCompleted(_ context.Context, source driver.Source, retention time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if retention <= 0 {
		return 0, nil
	}
	cutoff := f.now().Add(-retention)
	var removed int64
	for id, j := range f.jobs {
		if j.Source == source && j.State == driver.StateSucceeded && j.CompletedAt.Before(cutoff) {
			delete(f.jobs, id)
			delete(f.attempts, id)
			removed++
		}
	}
	return removed, nil
}

// --- job admin -----------------------------------------------------------

// ListKinds returns the distinct kinds of the source, sorted.
func (f *Fake) ListKinds(_ context.Context, source driver.Source) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[string]struct{}{}
	for _, j := range f.jobs {
		if j.Source == source {
			seen[j.Kind] = struct{}{}
		}
	}
	for k := range f.stats {
		if k.source == source {
			seen[k.kind] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	slices.Sort(out)
	return out, nil
}

// KindDepths returns per-kind state counters of the source.
func (f *Fake) KindDepths(_ context.Context, source driver.Source) (map[string]driver.Depths, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]driver.Depths{}
	for _, j := range f.jobs {
		if j.Source != source {
			continue
		}
		d := out[j.Kind]
		addDepth(&d, j.State)
		out[j.Kind] = d
	}
	return out, nil
}

// Stats returns one kind's depths and daily throughput window.
func (f *Fake) Stats(_ context.Context, source driver.Source, kind string) (driver.Depths, []driver.DailyCount, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var depths driver.Depths
	for _, j := range f.jobs {
		if j.Source == source && j.Kind == kind {
			addDepth(&depths, j.State)
		}
	}
	daily := f.dailyLocked(source, &kind)
	return depths, daily, nil
}

// AllDaily returns the daily throughput window summed across every kind.
func (f *Fake) AllDaily(_ context.Context, source driver.Source) ([]driver.DailyCount, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dailyLocked(source, nil), nil
}

// dailyLocked aggregates stat counters by day for the source, optionally scoped
// to one kind. Callers hold f.mu.
func (f *Fake) dailyLocked(source driver.Source, kind *string) []driver.DailyCount {
	byDay := map[time.Time]*driver.DailyCount{}
	for k, c := range f.stats {
		if k.source != source {
			continue
		}
		if kind != nil && k.kind != *kind {
			continue
		}
		d := byDay[k.day]
		if d == nil {
			d = &driver.DailyCount{Date: k.day}
			byDay[k.day] = d
		}
		d.Enqueued += c.enqueued
		d.Processed += c.processed
		d.Failed += c.failed
		d.Reaped += c.reaped
	}
	out := make([]driver.DailyCount, 0, len(byDay))
	for _, d := range byDay {
		out = append(out, *d)
	}
	slices.SortFunc(out, func(a, b driver.DailyCount) int { return a.Date.Compare(b.Date) })
	return out
}

// ListJobs lists jobs of the source matching the filter, paginated.
func (f *Fake) ListJobs(_ context.Context, source driver.Source, filter driver.JobFilter, offset, limit int) ([]driver.Job, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var matched []*fakeJob
	for _, j := range f.jobs {
		if j.Source != source {
			continue
		}
		if filter.Kind != "" && j.Kind != filter.Kind {
			continue
		}
		if filter.State != "" && j.State != filter.State {
			continue
		}
		matched = append(matched, j)
	}
	orderJobs(matched, filter.State)
	total := int64(len(matched))

	if offset < 0 {
		offset = 0
	}
	if offset > len(matched) {
		offset = len(matched)
	}
	matched = matched[offset:]
	if limit > 0 && limit < len(matched) {
		matched = matched[:limit]
	}
	out := make([]driver.Job, 0, len(matched))
	for _, j := range matched {
		out = append(out, j.toJob())
	}
	return out, total, nil
}

// GetJob returns one job of the source by id, or a not-found error.
func (f *Fake) GetJob(_ context.Context, source driver.Source, id uuid.UUID) (*driver.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.jobs[id]
	if j == nil || j.Source != source {
		return nil, driver.NewNotFound("get job")
	}
	out := j.toJob()
	return &out, nil
}

// JobAttempts returns a job's failure history, oldest first.
func (f *Fake) JobAttempts(_ context.Context, source driver.Source, id uuid.UUID) ([]driver.AttemptError, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if j := f.jobs[id]; j == nil || j.Source != source {
		return nil, nil
	}
	return slices.Clone(f.attempts[id]), nil
}

// RetryJob resets a dead job of the source to pending.
func (f *Fake) RetryJob(_ context.Context, source driver.Source, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.jobs[id]
	if j == nil || j.Source != source || j.State != driver.StateDead {
		return driver.NewNotFound("retry")
	}
	f.resetToPending(j)
	return nil
}

// RetryAllDead resets every dead job of (source, kind) to pending.
func (f *Fake) RetryAllDead(_ context.Context, source driver.Source, kind string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var retried int64
	for _, j := range f.jobs {
		if j.Source == source && (kind == "" || j.Kind == kind) && j.State == driver.StateDead {
			f.resetToPending(j)
			retried++
		}
	}
	return retried, nil
}

// resetToPending returns a dead job to a fresh pending state. Callers hold f.mu.
func (f *Fake) resetToPending(j *fakeJob) {
	now := f.now()
	j.State = driver.StatePending
	j.RunAt = now
	j.Attempt = 0
	j.ReapCount = 0
	j.LastError = ""
	j.FailedAt = time.Time{}
	f.wake(j.Source, j.Kind)
}

// ArchiveJob force-fails a pending or scheduled job to dead.
func (f *Fake) ArchiveJob(_ context.Context, source driver.Source, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.jobs[id]
	if j == nil || j.Source != source || (j.State != driver.StatePending && j.State != driver.StateScheduled) {
		return driver.NewNotFound("archive")
	}
	j.State = driver.StateDead
	j.LeaseUntil = time.Time{}
	return nil
}

// PauseJob holds a pending or scheduled job out of the ready set.
func (f *Fake) PauseJob(_ context.Context, source driver.Source, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.jobs[id]
	if j == nil || j.Source != source || (j.State != driver.StatePending && j.State != driver.StateScheduled) {
		return driver.NewNotFound("pause")
	}
	j.State = driver.StatePaused
	return nil
}

// ResumeJob returns a paused job to pending or scheduled per its run_at.
func (f *Fake) ResumeJob(_ context.Context, source driver.Source, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.jobs[id]
	if j == nil || j.Source != source || j.State != driver.StatePaused {
		return driver.NewNotFound("resume")
	}
	if j.RunAt.After(f.now()) {
		j.State = driver.StateScheduled
	} else {
		j.State = driver.StatePending
		f.wake(j.Source, j.Kind)
	}
	return nil
}

// DeleteJob deletes a job of the source in the given state.
func (f *Fake) DeleteJob(_ context.Context, source driver.Source, id uuid.UUID, state driver.JobState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.jobs[id]
	if j == nil || j.Source != source || j.State != state {
		return driver.NewNotFound("delete")
	}
	delete(f.jobs, id)
	delete(f.attempts, id)
	return nil
}

// DeleteAll deletes every job of (source, kind) in the given state.
func (f *Fake) DeleteAll(_ context.Context, source driver.Source, kind string, state driver.JobState) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var removed int64
	for id, j := range f.jobs {
		if j.Source == source && (kind == "" || j.Kind == kind) && j.State == state {
			delete(f.jobs, id)
			delete(f.attempts, id)
			removed++
		}
	}
	return removed, nil
}

// VacuumDead deletes dead jobs of (source, kind) enqueued before olderThan ago.
func (f *Fake) VacuumDead(_ context.Context, source driver.Source, kind string, olderThan time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cutoff := f.now().Add(-olderThan)
	var removed int64
	for id, j := range f.jobs {
		if j.Source == source && (kind == "" || j.Kind == kind) && j.State == driver.StateDead && j.EnqueuedAt.Before(cutoff) {
			delete(f.jobs, id)
			delete(f.attempts, id)
			removed++
		}
	}
	return removed, nil
}

// NukeAll deletes all jobs, stats and idempotency keys of the source.
func (f *Fake) NukeAll(_ context.Context, source driver.Source) (driver.NukeReport, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var report driver.NukeReport
	for id, j := range f.jobs {
		if j.Source == source {
			delete(f.jobs, id)
			delete(f.attempts, id)
			report.Jobs++
		}
	}
	for k := range f.stats {
		if k.source == source {
			delete(f.stats, k)
			report.Stats++
		}
	}
	for k := range f.idempotency {
		if k.source == source {
			delete(f.idempotency, k)
			report.Keys++
		}
	}
	return report, nil
}

// --- event ledger admin --------------------------------------------------

// ListEvents lists ledger events matching the filter, newest first, paginated.
func (f *Fake) ListEvents(_ context.Context, filter driver.EventFilter, offset, limit int) ([]driver.EventAdminRow, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var matched []driver.EventRecord
	for _, e := range f.events {
		if !f.eventMatches(e, filter) {
			continue
		}
		matched = append(matched, e)
	}
	slices.SortFunc(matched, func(a, b driver.EventRecord) int {
		if c := b.OccurredAt.Compare(a.OccurredAt); c != 0 { // newest first
			return c
		}
		return bytes.Compare(b.ID[:], a.ID[:])
	})
	total := int64(len(matched))

	if offset < 0 {
		offset = 0
	}
	if offset > len(matched) {
		offset = len(matched)
	}
	matched = matched[offset:]
	if limit > 0 && limit < len(matched) {
		matched = matched[:limit]
	}
	out := make([]driver.EventAdminRow, 0, len(matched))
	for _, e := range matched {
		out = append(out, f.eventAdminRow(e))
	}
	return out, total, nil
}

func (f *Fake) eventMatches(e driver.EventRecord, filter driver.EventFilter) bool {
	if filter.Type != "" && e.Type != filter.Type {
		return false
	}
	if filter.TenantID != uuid.Nil && e.TenantID != filter.TenantID {
		return false
	}
	if !filter.Since.IsZero() && e.OccurredAt.Before(filter.Since) {
		return false
	}
	if !filter.Until.IsZero() && e.OccurredAt.After(filter.Until) {
		return false
	}
	if filter.Undispatched != nil && *filter.Undispatched != (f.deliveryCount(e.ID) == 0) {
		return false
	}
	return true
}

// deliveryCount counts delivery jobs for an event. Callers hold f.mu.
func (f *Fake) deliveryCount(eventID uuid.UUID) int64 {
	var n int64
	for _, j := range f.jobs {
		if j.Source == driver.SourceEvent && j.EventID == eventID {
			n++
		}
	}
	return n
}

// eventAdminRow projects a ledger record for the admin surface. Callers hold f.mu.
func (f *Fake) eventAdminRow(e driver.EventRecord) driver.EventAdminRow {
	deliveries := f.deliveryCount(e.ID)
	row := driver.EventAdminRow{
		ID:            e.ID,
		Type:          e.Type,
		TenantID:      e.TenantID,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID,
		Version:       e.Version,
		OccurredAt:    e.OccurredAt,
		TraceID:       e.TraceID,
		SpanID:        e.SpanID,
		TraceFlags:    e.TraceFlags,
		Meta:          cloneMeta(e.Meta),
		Payload:       clonePayload(e.Payload),
		Deliveries:    deliveries,
	}
	if deliveries > 0 {
		row.DispatchedAt = e.OccurredAt
	}
	return row
}

// GetEvent returns one ledger event by id, or a not-found error.
func (f *Fake) GetEvent(_ context.Context, id uuid.UUID) (*driver.EventAdminRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.events[id]
	if !ok {
		return nil, driver.NewNotFound("get event")
	}
	row := f.eventAdminRow(e)
	return &row, nil
}

// ListSubscriberViews returns subscriber registrations, ordered by type then name.
func (f *Fake) ListSubscriberViews(_ context.Context, eventType string) ([]driver.SubscriberView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]driver.SubscriberView, 0, len(f.subscribers))
	for _, s := range f.subscribers {
		if eventType != "" && s.EventType != eventType {
			continue
		}
		out = append(out, driver.SubscriberView{
			EventType:   s.EventType,
			Subscriber:  s.Name,
			MaxAttempts: s.MaxAttempts,
			CreatedAt:   s.createdAt,
			UpdatedAt:   s.updatedAt,
		})
	}
	slices.SortFunc(out, func(a, b driver.SubscriberView) int {
		if c := cmp.Compare(a.EventType, b.EventType); c != 0 {
			return c
		}
		return cmp.Compare(a.Subscriber, b.Subscriber)
	})
	return out, nil
}

// OpsStats returns the ledger admin summary.
func (f *Fake) OpsStats(_ context.Context) (driver.OpsStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cutoff := f.now().Add(-24 * time.Hour)
	types := map[string]struct{}{}
	var out driver.OpsStats
	for _, e := range f.events {
		if f.deliveryCount(e.ID) == 0 {
			out.Undispatched++
		}
		if !e.OccurredAt.Before(cutoff) {
			out.Total24h++
			types[e.Type] = struct{}{}
		}
	}
	out.Types24h = int64(len(types))
	out.Subscribers = int64(len(f.subscribers))
	return out, nil
}

// Replay re-fans-out ledger events matching the filter into fresh deliveries.
func (f *Fake) Replay(_ context.Context, filter driver.ReplayFilter) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()

	var events []driver.EventRecord
	for _, e := range f.events {
		if filter.EventType != "" && e.Type != filter.EventType {
			continue
		}
		if filter.EventID != uuid.Nil && e.ID != filter.EventID {
			continue
		}
		if !filter.Since.IsZero() && e.OccurredAt.Before(filter.Since) {
			continue
		}
		if !filter.Until.IsZero() && e.OccurredAt.After(filter.Until) {
			continue
		}
		events = append(events, e)
	}
	slices.SortFunc(events, func(a, b driver.EventRecord) int { return bytes.Compare(a.ID[:], b.ID[:]) })
	if filter.Limit > 0 && filter.Limit < len(events) {
		events = events[:filter.Limit]
	}

	var created int64
	for _, e := range events {
		for _, sub := range f.subscribersFor(e.Type) {
			if filter.Subscriber != "" && sub.Name != filter.Subscriber {
				continue
			}
			f.createDelivery(e.ID, sub.Name, sub.MaxAttempts, true, now)
			created++
		}
	}
	return created, nil
}

// Retain deletes old events whose deliveries have all reached a terminal state,
// cascading to those terminal deliveries.
func (f *Fake) Retain(_ context.Context, before time.Time, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 {
		return 0, nil
	}
	var candidates []driver.EventRecord
	for _, e := range f.events {
		if !e.OccurredAt.Before(before) {
			continue
		}
		if f.hasInFlightDelivery(e.ID) {
			continue
		}
		candidates = append(candidates, e)
	}
	slices.SortFunc(candidates, func(a, b driver.EventRecord) int {
		if c := a.OccurredAt.Compare(b.OccurredAt); c != 0 {
			return c
		}
		return bytes.Compare(a.ID[:], b.ID[:])
	})
	if limit < len(candidates) {
		candidates = candidates[:limit]
	}
	var removed int64
	for _, e := range candidates {
		for id, j := range f.jobs {
			if j.Source == driver.SourceEvent && j.EventID == e.ID {
				delete(f.jobs, id)
				delete(f.attempts, id)
			}
		}
		delete(f.events, e.ID)
		removed++
	}
	return removed, nil
}

// hasInFlightDelivery reports whether an event has any delivery job that has
// not yet reached a terminal state. Only succeeded and dead are terminal:
// pending, scheduled (mid-retry), active, and paused (operator-parked) are all
// in-flight. Callers hold f.mu.
func (f *Fake) hasInFlightDelivery(eventID uuid.UUID) bool {
	for _, j := range f.jobs {
		if j.Source == driver.SourceEvent && j.EventID == eventID &&
			j.State != driver.StateSucceeded && j.State != driver.StateDead {
			return true
		}
	}
	return false
}

// --- lifecycle -----------------------------------------------------------

// Close is a no-op for the in-memory fake.
func (f *Fake) Close(context.Context) error { return nil }

// --- Notifier capability -------------------------------------------------

// Wake returns a channel of wakeups; it is closed when ctx ends.
func (f *Fake) Wake(ctx context.Context) (<-chan driver.Wake, error) {
	ch := make(chan driver.Wake, 256)
	f.wakeMu.Lock()
	f.wakers = append(f.wakers, ch)
	f.wakeMu.Unlock()
	go func() {
		<-ctx.Done()
		f.wakeMu.Lock()
		defer f.wakeMu.Unlock()
		for i, w := range f.wakers {
			if w == ch {
				f.wakers = slices.Delete(f.wakers, i, i+1)
				break
			}
		}
		close(ch)
	}()
	return ch, nil
}

// wake broadcasts a non-blocking wakeup to every live listener.
func (f *Fake) wake(source driver.Source, kind string) {
	f.wakeMu.Lock()
	defer f.wakeMu.Unlock()
	for _, ch := range f.wakers {
		select {
		case ch <- driver.Wake{Source: source, Kind: kind}:
		default:
		}
	}
}

// --- LeaderElector capability --------------------------------------------

// AcquireLeadership takes the named in-memory lock if it is free.
func (f *Fake) AcquireLeadership(_ context.Context, name string) (func(), bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.leaders[name] {
		return func() {}, false, nil
	}
	f.leaders[name] = true
	release := func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.leaders, name)
	}
	return release, true, nil
}

// --- small helpers -------------------------------------------------------

func sliceSet(items []string) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, s := range items {
		out[s] = true
	}
	return out
}

func addDepth(d *driver.Depths, state driver.JobState) {
	switch state {
	case driver.StatePending:
		d.Pending++
	case driver.StateScheduled:
		d.Scheduled++
	case driver.StateActive:
		d.Active++
	case driver.StateDead:
		d.Dead++
	case driver.StatePaused:
		d.Paused++
	case driver.StateSucceeded:
		d.Succeeded++
	}
}

// orderJobs sorts jobs per the ListJobs contract: the ordering key depends on
// the state filter (or lack of one), always broken by ID for stability.
//   - StatePending or StateDead: EnqueuedAt ascending
//   - StateScheduled or StatePaused: RunAt ascending
//   - StateActive: LeaseUntil ascending
//   - StateSucceeded: CompletedAt descending
//   - no state filter (""): EnqueuedAt descending (newest first, admin browsing)
func orderJobs(jobs []*fakeJob, state driver.JobState) {
	slices.SortFunc(jobs, func(a, b *fakeJob) int {
		var ta, tb time.Time
		desc := false
		switch state {
		case driver.StatePending, driver.StateDead:
			ta, tb = a.EnqueuedAt, b.EnqueuedAt
		case driver.StateScheduled, driver.StatePaused:
			ta, tb = a.RunAt, b.RunAt
		case driver.StateActive:
			ta, tb = a.LeaseUntil, b.LeaseUntil
		case driver.StateSucceeded:
			ta, tb, desc = a.CompletedAt, b.CompletedAt, true
		default: // no state filter
			ta, tb, desc = a.EnqueuedAt, b.EnqueuedAt, true
		}
		c := ta.Compare(tb)
		if desc {
			c = -c
		}
		if c != 0 {
			return c
		}
		return bytes.Compare(a.ID[:], b.ID[:])
	})
}
