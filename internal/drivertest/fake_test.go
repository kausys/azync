package drivertest_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// manualClock is a controllable clock for deterministic lease/reap tests.
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newFake(t *testing.T) (*drivertest.Fake, *manualClock) {
	t.Helper()
	clk := &manualClock{t: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	f := drivertest.NewFake()
	f.Clock = clk
	return f, clk
}

func payload() json.RawMessage { return json.RawMessage(`{"n":1}`) }

func TestEnqueueDequeueAck(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()
	f, _ := newFake(t)

	id := uuid.New()
	inserted, err := f.Enqueue(ctx, driver.EnqueueParams{ID: id, Kind: "send", Payload: payload(), MaxAttempts: 5})
	is.NoError(err)
	is.True(inserted)

	jobs, err := f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "send", Limit: 10, Lease: 30 * time.Second})
	is.NoError(err)
	is.Len(jobs, 1)
	is.Equal(id, jobs[0].ID)
	is.Equal(driver.StateActive, jobs[0].State)
	is.Equal(1, jobs[0].Attempt)
	is.NotEqual(uuid.Nil, jobs[0].LeaseToken)

	is.NoError(f.Ack(ctx, jobs[0].ID, jobs[0].LeaseToken))

	// Nothing left to dequeue, and the job is retained as succeeded.
	again, err := f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "send", Limit: 10, Lease: 30 * time.Second})
	is.NoError(err)
	is.Empty(again)

	got, err := f.GetJob(ctx, driver.SourceQueue, id)
	is.NoError(err)
	is.Equal(driver.StateSucceeded, got.State)
}

func TestSettlementFencingStaleToken(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()
	f, _ := newFake(t)

	id := uuid.New()
	_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: id, Kind: "send", Payload: payload()})
	is.NoError(err)
	jobs, err := f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "send", Limit: 1, Lease: time.Minute})
	is.NoError(err)
	is.Len(jobs, 1)

	// A stale token cannot settle the job.
	err = f.Ack(ctx, id, uuid.New())
	is.Error(err)
	is.True(driver.IsNotFound(err))

	// The real token still owns the lease.
	is.NoError(f.Ack(ctx, id, jobs[0].LeaseToken))
}

func TestIdempotencyLiveDedupe(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()
	f, _ := newFake(t)

	first, err := f.Enqueue(ctx, driver.EnqueueParams{ID: uuid.New(), Kind: "k", Payload: payload(), IdempotencyKey: "dup"})
	is.NoError(err)
	is.True(first)

	second, err := f.Enqueue(ctx, driver.EnqueueParams{ID: uuid.New(), Kind: "k", Payload: payload(), IdempotencyKey: "dup"})
	is.NoError(err)
	is.False(second, "duplicate live idempotency key must be rejected")

	// A different kind with the same key is independent.
	other, err := f.Enqueue(ctx, driver.EnqueueParams{ID: uuid.New(), Kind: "other", Payload: payload(), IdempotencyKey: "dup"})
	is.NoError(err)
	is.True(other)
}

func TestReapBackToPendingThenDead(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()
	f, clk := newFake(t)

	id := uuid.New()
	_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: id, Kind: "send", Payload: payload()})
	is.NoError(err)

	// First lease, let it expire, reap -> back to pending (reap_count 1 < 2).
	_, err = f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "send", Limit: 1, Lease: time.Second})
	is.NoError(err)
	clk.advance(2 * time.Second)
	reaped, killed, err := f.ReapExpired(ctx, driver.SourceQueue, []string{"send"}, 2)
	is.NoError(err)
	is.Equal(int64(1), reaped)
	is.Equal(int64(0), killed)
	got, err := f.GetJob(ctx, driver.SourceQueue, id)
	is.NoError(err)
	is.Equal(driver.StatePending, got.State)

	// Second lease, expire, reap -> dead (reap_count 2 >= 2).
	_, err = f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "send", Limit: 1, Lease: time.Second})
	is.NoError(err)
	clk.advance(2 * time.Second)
	reaped, killed, err = f.ReapExpired(ctx, driver.SourceQueue, []string{"send"}, 2)
	is.NoError(err)
	is.Equal(int64(1), reaped)
	is.Equal(int64(1), killed)
	got, err = f.GetJob(ctx, driver.SourceQueue, id)
	is.NoError(err)
	is.Equal(driver.StateDead, got.State)

	// The death recorded an attempt in the history.
	attempts, err := f.JobAttempts(ctx, driver.SourceQueue, id)
	is.NoError(err)
	is.Len(attempts, 1)
}

func TestReleaseDecrementsAttempt(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()
	f, _ := newFake(t)

	id := uuid.New()
	_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: id, Kind: "send", Payload: payload()})
	is.NoError(err)
	jobs, err := f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "send", Limit: 1, Lease: time.Minute})
	is.NoError(err)
	is.Len(jobs, 1)
	is.Equal(1, jobs[0].Attempt)

	is.NoError(f.Release(ctx, id, jobs[0].LeaseToken))
	got, err := f.GetJob(ctx, driver.SourceQueue, id)
	is.NoError(err)
	is.Equal(driver.StatePending, got.State)
	is.Equal(0, got.Attempt, "Release decrements the attempt it did not really spend")

	// No attempt history was recorded by Release.
	attempts, err := f.JobAttempts(ctx, driver.SourceQueue, id)
	is.NoError(err)
	is.Empty(attempts)
}

func TestPublishFanOutAndEventRehydration(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()
	f, _ := newFake(t)

	is.NoError(f.RegisterSubscriber(ctx, driver.Subscriber{Name: "a", EventType: "user.created", MaxAttempts: 3}))
	is.NoError(f.RegisterSubscriber(ctx, driver.Subscriber{Name: "b", EventType: "user.created", MaxAttempts: 3}))

	eventID := uuid.New()
	body := json.RawMessage(`{"user":42}`)
	delivered, err := f.Publish(ctx, driver.PublishParams{ID: eventID, Type: "user.created", Payload: body, OccurredAt: time.Now()})
	is.NoError(err)
	is.Equal(2, delivered, "one delivery per subscriber")

	jobsA, err := f.DequeueBatch(ctx, driver.SourceEvent, driver.DequeueParams{Kind: "a", Limit: 10, Lease: time.Minute})
	is.NoError(err)
	is.Len(jobsA, 1)
	is.Equal(driver.SourceEvent, jobsA[0].Source)
	is.Equal(eventID, jobsA[0].EventID)
	is.Nil(jobsA[0].Payload, "delivery job payload is nil; body lives in the ledger")
	is.NotNil(jobsA[0].Event, "event is rehydrated on dequeue")
	is.Equal(eventID, jobsA[0].Event.ID)
	is.Equal("user.created", jobsA[0].Event.Type)
	is.JSONEq(string(body), string(jobsA[0].Event.Payload))

	jobsB, err := f.DequeueBatch(ctx, driver.SourceEvent, driver.DequeueParams{Kind: "b", Limit: 10, Lease: time.Minute})
	is.NoError(err)
	is.Len(jobsB, 1)
}

func TestReplayCreatesFreshDeliveries(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()
	f, _ := newFake(t)

	is.NoError(f.RegisterSubscriber(ctx, driver.Subscriber{Name: "a", EventType: "T", MaxAttempts: 3}))
	is.NoError(f.RegisterSubscriber(ctx, driver.Subscriber{Name: "b", EventType: "T", MaxAttempts: 3}))

	eventID := uuid.New()
	_, err := f.Publish(ctx, driver.PublishParams{ID: eventID, Type: "T", Payload: payload(), OccurredAt: time.Now()})
	is.NoError(err)

	// Drain the original deliveries.
	for _, name := range []string{"a", "b"} {
		jobs, err := f.DequeueBatch(ctx, driver.SourceEvent, driver.DequeueParams{Kind: name, Limit: 10, Lease: time.Minute})
		is.NoError(err)
		is.Len(jobs, 1)
		is.NoError(f.Ack(ctx, jobs[0].ID, jobs[0].LeaseToken))
	}

	created, err := f.Replay(ctx, driver.ReplayFilter{EventType: "T"})
	is.NoError(err)
	is.Equal(int64(2), created, "one fresh delivery per subscriber")

	replayed, err := f.DequeueBatch(ctx, driver.SourceEvent, driver.DequeueParams{Kind: "a", Limit: 10, Lease: time.Minute})
	is.NoError(err)
	is.Len(replayed, 1)
	is.True(replayed[0].Replay, "replayed deliveries are flagged")
	is.NotNil(replayed[0].Event)
	is.Equal(eventID, replayed[0].Event.ID)
}

func TestPromoteDueMovesScheduledToPending(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()
	f, clk := newFake(t)

	id := uuid.New()
	_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: id, Kind: "send", Payload: payload(), Delay: time.Hour})
	is.NoError(err)

	// Scheduled in the future: not dequeueable yet.
	jobs, err := f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "send", Limit: 10, Lease: time.Minute})
	is.NoError(err)
	is.Empty(jobs)

	clk.advance(2 * time.Hour)
	promoted, err := f.PromoteDue(ctx, driver.SourceQueue, []string{"send"})
	is.NoError(err)
	is.Equal(int64(1), promoted)

	jobs, err = f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "send", Limit: 10, Lease: time.Minute})
	is.NoError(err)
	is.Len(jobs, 1)
}

func TestVacuumStatsNeverTrimsDataWithinRetention(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()

	// A counter bumped at 23:50 on July 21: by July 23 00:10 its newest data is
	// only 24h20m old, so a 36h retention must keep the whole day.
	clk := drivertest.NewManualClock(time.Date(2026, 7, 21, 23, 50, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: uuid.New(), Kind: "send", Payload: payload()})
	is.NoError(err)

	clk.Set(time.Date(2026, 7, 23, 0, 10, 0, 0, time.UTC))
	removed, err := f.VacuumStats(ctx, driver.SourceQueue, 36*time.Hour)
	is.NoError(err)
	is.Zero(removed, "day-granular vacuuming must never trim counters younger than the retention")

	// With a 12h retention every datum of that day is expired, so it goes.
	removed, err = f.VacuumStats(ctx, driver.SourceQueue, 12*time.Hour)
	is.NoError(err)
	is.Equal(int64(1), removed)
}

func TestNotifierWakesOnEnqueue(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := t.Context()
	f, _ := newFake(t)

	wakes, err := f.Wake(ctx)
	is.NoError(err)
	is.NotNil(wakes)

	_, err = f.Enqueue(ctx, driver.EnqueueParams{ID: uuid.New(), Kind: "send", Payload: payload()})
	is.NoError(err)

	select {
	case w := <-wakes:
		is.Equal(driver.SourceQueue, w.Source)
		is.Equal("send", w.Kind)
	case <-time.After(time.Second):
		t.Fatal("expected a wake after enqueue")
	}
}

func TestLeaderElectionMutualExclusion(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()
	f, _ := newFake(t)

	release, ok, err := f.AcquireLeadership(ctx, "cron")
	is.NoError(err)
	is.True(ok)

	_, ok2, err := f.AcquireLeadership(ctx, "cron")
	is.NoError(err)
	is.False(ok2, "leadership is exclusive while held")

	release()
	_, ok3, err := f.AcquireLeadership(ctx, "cron")
	is.NoError(err)
	is.True(ok3, "leadership is available again after release")
}

// jobIDs extracts IDs in order for ordering assertions.
func jobIDs(jobs []driver.Job) []uuid.UUID {
	ids := make([]uuid.UUID, len(jobs))
	for i, j := range jobs {
		ids[i] = j.ID
	}
	return ids
}

func TestRetainSkipsNonTerminalDeliveries(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()
	f, clk := newFake(t)

	is.NoError(f.RegisterSubscriber(ctx, driver.Subscriber{Name: "a", EventType: "T", MaxAttempts: 5}))

	eventID := uuid.New()
	occurredAt := clk.Now()
	delivered, err := f.Publish(ctx, driver.PublishParams{ID: eventID, Type: "T", Payload: payload(), OccurredAt: occurredAt})
	is.NoError(err)
	is.Equal(1, delivered)

	cutoff := occurredAt.Add(time.Hour)

	// Pending delivery: not retained.
	removed, err := f.Retain(ctx, cutoff, 10)
	is.NoError(err)
	is.Equal(int64(0), removed, "pending delivery is in-flight")

	jobs, err := f.DequeueBatch(ctx, driver.SourceEvent, driver.DequeueParams{Kind: "a", Limit: 10, Lease: time.Minute})
	is.NoError(err)
	is.Len(jobs, 1)
	jobID, leaseToken := jobs[0].ID, jobs[0].LeaseToken

	// Reschedule mid-retry: delivery is now scheduled, still in-flight.
	is.NoError(f.Reschedule(ctx, jobID, leaseToken, time.Minute, "transient failure"))
	removed, err = f.Retain(ctx, cutoff, 10)
	is.NoError(err)
	is.Equal(int64(0), removed, "scheduled (mid-retry) delivery is in-flight, not terminal")

	// An operator parks the retry: paused is also in-flight.
	is.NoError(f.PauseJob(ctx, driver.SourceEvent, jobID))
	removed, err = f.Retain(ctx, cutoff, 10)
	is.NoError(err)
	is.Equal(int64(0), removed, "paused delivery is in-flight, not terminal")

	// Resume, let it become due, lease it again, and let it die: only now is
	// every delivery job of the event terminal.
	is.NoError(f.ResumeJob(ctx, driver.SourceEvent, jobID))
	clk.advance(2 * time.Minute)
	_, err = f.PromoteDue(ctx, driver.SourceEvent, []string{"a"})
	is.NoError(err)
	redone, err := f.DequeueBatch(ctx, driver.SourceEvent, driver.DequeueParams{Kind: "a", Limit: 10, Lease: time.Minute})
	is.NoError(err)
	is.Len(redone, 1)
	is.NoError(f.Dead(ctx, redone[0].ID, redone[0].LeaseToken, "exhausted"))

	removed, err = f.Retain(ctx, cutoff, 10)
	is.NoError(err)
	is.Equal(int64(1), removed, "dead delivery is terminal; the event is retainable")

	_, err = f.GetJob(ctx, driver.SourceEvent, jobID)
	is.Error(err)
	is.True(driver.IsNotFound(err), "cascade removes the delivery job with its event")
}

func TestListJobsOrdering(t *testing.T) {
	t.Parallel()

	t.Run("no state filter orders by EnqueuedAt descending", func(t *testing.T) {
		t.Parallel()
		is := require.New(t)
		ctx := context.Background()
		f, clk := newFake(t)

		ids := make([]uuid.UUID, 3)
		for i := range ids {
			ids[i] = uuid.New()
			_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: ids[i], Kind: "x", Payload: payload()})
			is.NoError(err)
			clk.advance(time.Minute)
		}

		jobs, total, err := f.ListJobs(ctx, driver.SourceQueue, driver.JobFilter{}, 0, 10)
		is.NoError(err)
		is.EqualValues(3, total)
		is.Equal([]uuid.UUID{ids[2], ids[1], ids[0]}, jobIDs(jobs), "newest EnqueuedAt first")
	})

	t.Run("pending orders by EnqueuedAt ascending", func(t *testing.T) {
		t.Parallel()
		is := require.New(t)
		ctx := context.Background()
		f, clk := newFake(t)

		ids := make([]uuid.UUID, 3)
		for i := range ids {
			ids[i] = uuid.New()
			_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: ids[i], Kind: "x", Payload: payload()})
			is.NoError(err)
			clk.advance(time.Minute)
		}

		jobs, _, err := f.ListJobs(ctx, driver.SourceQueue, driver.JobFilter{State: driver.StatePending}, 0, 10)
		is.NoError(err)
		is.Equal(ids, jobIDs(jobs), "oldest EnqueuedAt first")
	})

	t.Run("dead orders by EnqueuedAt ascending, independent of death order", func(t *testing.T) {
		t.Parallel()
		is := require.New(t)
		ctx := context.Background()
		f, clk := newFake(t)

		ids := make([]uuid.UUID, 3)
		for i := range ids {
			ids[i] = uuid.New()
			_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: ids[i], Kind: "x", Payload: payload()})
			is.NoError(err)
			clk.advance(time.Minute)
		}

		leased, err := f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "x", Limit: 10, Lease: time.Minute})
		is.NoError(err)
		is.Len(leased, 3)
		tokens := make(map[uuid.UUID]uuid.UUID, 3)
		for _, j := range leased {
			tokens[j.ID] = j.LeaseToken
		}

		// Kill them out of enqueue order to prove the sort key is EnqueuedAt,
		// not death order.
		for _, i := range []int{2, 0, 1} {
			is.NoError(f.Dead(ctx, ids[i], tokens[ids[i]], "boom"))
		}

		jobs, _, err := f.ListJobs(ctx, driver.SourceQueue, driver.JobFilter{State: driver.StateDead}, 0, 10)
		is.NoError(err)
		is.Equal(ids, jobIDs(jobs), "oldest EnqueuedAt first, regardless of death order")
	})

	t.Run("scheduled orders by RunAt ascending", func(t *testing.T) {
		t.Parallel()
		is := require.New(t)
		ctx := context.Background()
		f, _ := newFake(t)

		id0, id1, id2 := uuid.New(), uuid.New(), uuid.New()
		// Enqueued in this order, but with RunAt out of that order, to prove
		// the sort key is RunAt, not EnqueuedAt or insertion order.
		_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: id0, Kind: "x", Payload: payload(), Delay: 3 * time.Hour})
		is.NoError(err)
		_, err = f.Enqueue(ctx, driver.EnqueueParams{ID: id1, Kind: "x", Payload: payload(), Delay: time.Hour})
		is.NoError(err)
		_, err = f.Enqueue(ctx, driver.EnqueueParams{ID: id2, Kind: "x", Payload: payload(), Delay: 2 * time.Hour})
		is.NoError(err)

		jobs, _, err := f.ListJobs(ctx, driver.SourceQueue, driver.JobFilter{State: driver.StateScheduled}, 0, 10)
		is.NoError(err)
		is.Equal([]uuid.UUID{id1, id2, id0}, jobIDs(jobs), "soonest RunAt first")
	})

	t.Run("paused orders by RunAt ascending", func(t *testing.T) {
		t.Parallel()
		is := require.New(t)
		ctx := context.Background()
		f, _ := newFake(t)

		id0, id1, id2 := uuid.New(), uuid.New(), uuid.New()
		_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: id0, Kind: "x", Payload: payload(), Delay: 3 * time.Hour})
		is.NoError(err)
		_, err = f.Enqueue(ctx, driver.EnqueueParams{ID: id1, Kind: "x", Payload: payload(), Delay: time.Hour})
		is.NoError(err)
		_, err = f.Enqueue(ctx, driver.EnqueueParams{ID: id2, Kind: "x", Payload: payload(), Delay: 2 * time.Hour})
		is.NoError(err)
		for _, id := range []uuid.UUID{id0, id1, id2} {
			is.NoError(f.PauseJob(ctx, driver.SourceQueue, id))
		}

		jobs, _, err := f.ListJobs(ctx, driver.SourceQueue, driver.JobFilter{State: driver.StatePaused}, 0, 10)
		is.NoError(err)
		is.Equal([]uuid.UUID{id1, id2, id0}, jobIDs(jobs), "soonest RunAt first")
	})

	t.Run("active orders by LeaseUntil ascending", func(t *testing.T) {
		t.Parallel()
		is := require.New(t)
		ctx := context.Background()
		f, _ := newFake(t)

		for range 3 {
			_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: uuid.New(), Kind: "x", Payload: payload()})
			is.NoError(err)
		}

		// Lease one job at a time with leases in an order that does not match
		// dequeue order, to prove the sort key is LeaseUntil.
		var short, mid, long uuid.UUID
		for _, step := range []struct {
			lease time.Duration
			dst   *uuid.UUID
		}{
			{30 * time.Second, &long},
			{10 * time.Second, &short},
			{20 * time.Second, &mid},
		} {
			leased, err := f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "x", Limit: 1, Lease: step.lease})
			is.NoError(err)
			is.Len(leased, 1)
			*step.dst = leased[0].ID
		}

		jobs, _, err := f.ListJobs(ctx, driver.SourceQueue, driver.JobFilter{State: driver.StateActive}, 0, 10)
		is.NoError(err)
		is.Equal([]uuid.UUID{short, mid, long}, jobIDs(jobs), "soonest LeaseUntil first")
	})

	t.Run("succeeded orders by CompletedAt descending", func(t *testing.T) {
		t.Parallel()
		is := require.New(t)
		ctx := context.Background()
		f, clk := newFake(t)

		for range 3 {
			_, err := f.Enqueue(ctx, driver.EnqueueParams{ID: uuid.New(), Kind: "x", Payload: payload()})
			is.NoError(err)
		}
		leased, err := f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "x", Limit: 10, Lease: time.Minute})
		is.NoError(err)
		is.Len(leased, 3)

		// Ack in order [0,1,2]; CompletedAt is strictly increasing, so
		// descending-CompletedAt order is the reverse of the ack order.
		for _, j := range leased {
			is.NoError(f.Ack(ctx, j.ID, j.LeaseToken))
			clk.advance(time.Minute)
		}

		jobs, _, err := f.ListJobs(ctx, driver.SourceQueue, driver.JobFilter{State: driver.StateSucceeded}, 0, 10)
		is.NoError(err)
		is.Equal([]uuid.UUID{leased[2].ID, leased[1].ID, leased[0].ID}, jobIDs(jobs), "most recently completed first")
	})
}

// The fake must satisfy the driver contract and both optional capabilities.
var (
	_ driver.Store         = (*drivertest.Fake)(nil)
	_ driver.Notifier      = (*drivertest.Fake)(nil)
	_ driver.LeaderElector = (*drivertest.Fake)(nil)
)
