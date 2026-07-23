package integration

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/queue"

	"github.com/stretchr/testify/require"
)

// itJob is the canonical queue payload for the integration suite. Each test
// registers its own handler for this kind on a private, ephemeral schema.
type itJob struct {
	V string `json:"v"`
}

func (itJob) Kind() string { return "it.job" }

// newQueue composes a queue runtime over the harness Core with cron disabled by
// default (the cron test re-enables it).
func newQueue(t *testing.T, h *harness, opts ...queue.Option) *queue.Runtime {
	t.Helper()
	q, err := queue.New(h.core, append([]queue.Option{queue.WithCron(false)}, opts...)...)
	require.NoError(t, err)
	return q
}

func TestQueueEnqueueWorkerSucceeds(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	q := newQueue(t, h)
	ctx := context.Background()

	done := make(chan struct{}, 1)
	is.NoError(queue.Register(q.Worker(), func(ctx context.Context, job itJob) error {
		is.Equal("hello", job.V)
		is.Equal(1, queue.Attempt(ctx))
		is.NotNil(queue.Metadata(ctx))
		done <- struct{}{}
		return nil
	}))
	startWorker(t, q.Worker())

	res, err := q.Producer().Enqueue(ctx, itJob{V: "hello"})
	is.NoError(err)
	is.False(res.Deduplicated)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("job did not execute")
	}
	require.Eventually(t, func() bool {
		s, serr := q.Manager().Stats(ctx, itJob{}.Kind())
		is.NoError(serr)
		return s.Processed == 1 && s.Succeeded == 1 && s.Pending+s.Active+s.Scheduled == 0
	}, 3*time.Second, 20*time.Millisecond)
}

func TestQueueRetryBackoffToDead(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	q := newQueue(t, h)
	ctx := context.Background()

	var attempts atomic.Int32
	is.NoError(queue.Register(q.Worker(), func(context.Context, itJob) error {
		attempts.Add(1)
		return errors.New("always fails") // plain error → retry with backoff
	}, queue.WithMaxRetries(2)))
	startWorker(t, q.Worker())

	_, err := q.Producer().Enqueue(ctx, itJob{V: "boom"})
	is.NoError(err)

	require.Eventually(t, func() bool {
		page, lerr := q.Manager().List(ctx, itJob{}.Kind(), queue.StateDead, 0, 10)
		is.NoError(lerr)
		return page.Total == 1
	}, 8*time.Second, 50*time.Millisecond)

	page, err := q.Manager().List(ctx, itJob{}.Kind(), queue.StateDead, 0, 10)
	is.NoError(err)
	is.Len(page.Items, 1)
	is.Equal(2, page.Items[0].MaxAttempts)
	is.Equal(int32(2), attempts.Load(), "the job exhausted its two-attempt budget across a backoff")
}

func TestQueueAbortGoesToDead(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	q := newQueue(t, h)
	ctx := context.Background()

	var attempts atomic.Int32
	is.NoError(queue.Register(q.Worker(), func(context.Context, itJob) error {
		attempts.Add(1)
		return queue.Abort(errors.New("permanent"))
	}, queue.WithMaxRetries(5)))
	startWorker(t, q.Worker())

	_, err := q.Producer().Enqueue(ctx, itJob{V: "abort"})
	is.NoError(err)

	require.Eventually(t, func() bool {
		s, serr := q.Manager().Stats(ctx, itJob{}.Kind())
		is.NoError(serr)
		return s.Dead == 1
	}, 3*time.Second, 20*time.Millisecond)
	is.Equal(int32(1), attempts.Load(), "Abort dead-letters on the first attempt without consuming the retry budget")
}

func TestQueueRetryAfterDelay(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	q := newQueue(t, h)
	ctx := context.Background()

	var attempts atomic.Int32
	done := make(chan struct{}, 1)
	is.NoError(queue.Register(q.Worker(), func(context.Context, itJob) error {
		if attempts.Add(1) == 1 {
			return queue.RetryAfter(errors.New("warm up"), 50*time.Millisecond)
		}
		done <- struct{}{}
		return nil
	}))
	startWorker(t, q.Worker())

	_, err := q.Producer().Enqueue(ctx, itJob{V: "retry-after"})
	is.NoError(err)

	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("job did not complete after its fixed retry delay")
	}
	is.Equal(int32(2), attempts.Load())
	require.Eventually(t, func() bool {
		s, serr := q.Manager().Stats(ctx, itJob{}.Kind())
		is.NoError(serr)
		return s.Processed == 1 && s.Failed == 1
	}, 3*time.Second, 20*time.Millisecond)
}

func TestQueueIdempotencyLiveAndTTL(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	q := newQueue(t, h)
	ctx := context.Background()

	// Live-job dedupe: a second enqueue while the first is still live is dropped.
	first, err := q.Producer().Enqueue(ctx, itJob{V: "a"}, queue.IdempotencyKey("live"))
	is.NoError(err)
	is.False(first.Deduplicated)
	dup, err := q.Producer().Enqueue(ctx, itJob{V: "b"}, queue.IdempotencyKey("live"))
	is.NoError(err)
	is.True(dup.Deduplicated)
	is.NotEqual(first.ID, dup.ID, "the rejected enqueue keeps its own generated id")

	// Time-window dedupe: a TTL reservation rejects duplicates within the window.
	firstTTL, err := q.Producer().Enqueue(ctx, itJob{V: "c"}, queue.IdempotencyKeyTTL("window", time.Hour))
	is.NoError(err)
	is.False(firstTTL.Deduplicated)
	dupTTL, err := q.Producer().Enqueue(ctx, itJob{V: "d"}, queue.IdempotencyKeyTTL("window", time.Hour))
	is.NoError(err)
	is.True(dupTTL.Deduplicated)
}

func TestQueueScheduledPromotion(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	q := newQueue(t, h)
	ctx := context.Background()

	done := make(chan struct{}, 1)
	is.NoError(queue.Register(q.Worker(), func(context.Context, itJob) error {
		done <- struct{}{}
		return nil
	}))
	startWorker(t, q.Worker())

	// Delay is resolved on the backend clock, so the job is durably scheduled
	// (not pending) and only becomes runnable after promotion.
	res, err := q.Producer().Enqueue(ctx, itJob{V: "later"}, queue.Delay(300*time.Millisecond))
	is.NoError(err)
	got, err := q.Manager().Get(ctx, res.ID)
	is.NoError(err)
	is.Equal(queue.StateScheduled, got.State, "a delayed job starts scheduled")

	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("scheduled job was not promoted and executed")
	}
}

// TestQueueReaperRevivesDeadWorkerAndFencesStaleToken simulates a worker that
// leases a job and dies without acking: its lease expires, a live worker's
// reaper reclaims the job and completes it, and the dead worker's stale lease
// token can no longer settle the job.
func TestQueueReaperRevivesDeadWorkerAndFencesStaleToken(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	q := newQueue(t, h)
	ctx := context.Background()
	store := h.core.Store()

	_, err := q.Producer().Enqueue(ctx, itJob{V: "orphaned"})
	is.NoError(err)

	// A "dead" worker leases the job with a short lease and never renews or acks.
	orphan, err := store.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{
		Kind: itJob{}.Kind(), Limit: 1, Lease: 200 * time.Millisecond,
	})
	is.NoError(err)
	is.Len(orphan, 1)
	staleToken := orphan[0].LeaseToken
	time.Sleep(300 * time.Millisecond) // let the lease expire

	done := make(chan struct{}, 1)
	is.NoError(queue.Register(q.Worker(), func(context.Context, itJob) error {
		done <- struct{}{}
		return nil
	}))
	startWorker(t, q.Worker())

	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("reaper did not revive the orphaned job for a live worker")
	}

	is.True(driver.IsNotFound(store.Ack(ctx, orphan[0].ID, staleToken)),
		"the dead worker's stale lease token is fenced")
	require.Eventually(t, func() bool {
		s, serr := q.Manager().Stats(ctx, itJob{}.Kind())
		is.NoError(serr)
		return s.Succeeded == 1
	}, 3*time.Second, 20*time.Millisecond)
}

func TestQueueManagerOperations(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	q := newQueue(t, h)
	ctx := context.Background()
	m := q.Manager()

	// Two jobs; dead-letter one directly through the store. DequeueBatch picks by
	// (run_at, id), so track which job actually got leased rather than assuming.
	res1, err := q.Producer().Enqueue(ctx, itJob{V: "j1"})
	is.NoError(err)
	res2, err := q.Producer().Enqueue(ctx, itJob{V: "j2"})
	is.NoError(err)
	store := h.core.Store()
	leased, err := store.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: itJob{}.Kind(), Limit: 1, Lease: time.Minute})
	is.NoError(err)
	is.Len(leased, 1)
	deadID := leased[0].ID
	pendingID := res1.ID
	if deadID == res1.ID {
		pendingID = res2.ID
	}
	is.NoError(store.Dead(ctx, deadID, leased[0].LeaseToken, "manual"))

	// Stats: depths plus a zero-filled 30-day series.
	stats, err := m.Stats(ctx, itJob{}.Kind())
	is.NoError(err)
	is.EqualValues(2, stats.Enqueued)
	is.Len(stats.Daily, 30)
	is.Equal(30, stats.WindowDays)

	// Get + List.
	got, err := m.Get(ctx, pendingID)
	is.NoError(err)
	is.NotNil(got)
	is.Equal(pendingID, got.ID)
	deadPage, err := m.List(ctx, itJob{}.Kind(), queue.StateDead, 0, 10)
	is.NoError(err)
	is.EqualValues(1, deadPage.Total)
	is.Equal(deadID, deadPage.Items[0].ID)

	// Retry the dead job → pending.
	is.NoError(m.Retry(ctx, deadID))
	got, err = m.Get(ctx, deadID)
	is.NoError(err)
	is.Equal(queue.StatePending, got.State)

	// Pause / Resume the remaining pending job.
	is.NoError(m.Pause(ctx, pendingID))
	got, err = m.Get(ctx, pendingID)
	is.NoError(err)
	is.Equal(queue.StatePaused, got.State)
	is.NoError(m.Resume(ctx, pendingID))
	got, err = m.Get(ctx, pendingID)
	is.NoError(err)
	is.Equal(queue.StatePending, got.State)

	// Purge removes pending/scheduled/dead; paused and active survive.
	report, err := m.Purge(ctx, itJob{}.Kind())
	is.NoError(err)
	is.GreaterOrEqual(report.Pending, int64(2))
	afterPending, err := m.List(ctx, itJob{}.Kind(), queue.StatePending, 0, 10)
	is.NoError(err)
	is.EqualValues(0, afterPending.Total, "purge emptied the pending jobs")
}

func TestQueueCronLeadershipAndOccurrenceDedup(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	ctx := context.Background()

	// Two workers over the same schema, both with cron enabled and the same
	// schedule. Leadership elects a single enqueuer; the per-occurrence
	// idempotency window is belt-and-braces against a failover overlap.
	second, err := azync.Open(h.base, fastCoreOptions(h.schema)...)
	is.NoError(err)
	t.Cleanup(func() { _ = second.Close(context.Background()) })

	runtimes := []*queue.Runtime{
		newQueue(t, h, queue.WithCron(true), queue.WithCronTick(50*time.Millisecond)),
		mustQueue(t, second, queue.WithCron(true), queue.WithCronTick(50*time.Millisecond)),
	}
	for _, r := range runtimes {
		is.NoError(r.Worker().RegisterCron("beat", "@every 1s", itJob{V: "cron"}))
		is.NoError(queue.Register(r.Worker(), func(context.Context, itJob) error { return nil }))
		startWorker(t, r.Worker())
	}

	// Let a few occurrences fire under a single leader.
	time.Sleep(2500 * time.Millisecond)

	page, err := runtimes[0].Manager().List(ctx, itJob{}.Kind(), "", 0, 100)
	is.NoError(err)
	is.GreaterOrEqual(page.Total, int64(1), "cron fired at least one occurrence")
	is.LessOrEqual(page.Total, int64(3), "each occurrence yields exactly one job despite two workers")
}

// mustQueue composes a queue runtime over an arbitrary Core (used for the second
// cron worker built on its own Core over the shared schema).
func mustQueue(t *testing.T, core *azync.Core, opts ...queue.Option) *queue.Runtime {
	t.Helper()
	q, err := queue.New(core, opts...)
	require.NoError(t, err)
	return q
}
