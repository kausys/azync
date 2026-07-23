package integration

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/event"
	"github.com/kausys/azync/queue"

	"github.com/stretchr/testify/require"
)

// TestUnifiedQueueAndEventCoexist runs a queue and an event runtime over one
// shared Core (one schema, one pool, one listener) and proves their stats stay
// partitioned by source.
func TestUnifiedQueueAndEventCoexist(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	q := newQueue(t, h)
	e := newEvent(t, h)
	ctx := context.Background()

	jobDone := make(chan struct{}, 1)
	is.NoError(queue.Register(q.Worker(), func(_ context.Context, _ queue.Job[itJob]) error {
		jobDone <- struct{}{}
		return nil
	}))
	is.NoError(e.Publisher().Register(ctx, event.Subscriber{Name: "sink", EventType: orderEvent{}.EventType(), MaxAttempts: 3}))
	evDone := make(chan struct{}, 1)
	is.NoError(e.Worker().Subscribe("sink", func(_ context.Context, _ event.Envelope) error {
		evDone <- struct{}{}
		return nil
	}))
	startWorker(t, q.Worker())
	startWorker(t, e.Worker())

	_, err := q.Producer().Enqueue(ctx, itJob{V: "shared"})
	is.NoError(err)
	_, err = e.Publisher().Publish(ctx, orderEvent{Amount: 1})
	is.NoError(err)

	for _, ch := range []chan struct{}{jobDone, evDone} {
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatal("shared-core queue and event work did not both complete")
		}
	}

	require.Eventually(t, func() bool {
		qs, qerr := q.Manager().Stats(ctx, itJob{}.Kind())
		is.NoError(qerr)
		es, eerr := e.Manager().Stats(ctx)
		is.NoError(eerr)
		return qs.Succeeded == 1 && es.Succeeded == 1 && es.Events == 1
	}, 3*time.Second, 20*time.Millisecond)

	// Source partitioning: the queue's kind is not an event subscriber, and the
	// event stats never fold in the queue job.
	es, err := e.Manager().Stats(ctx)
	is.NoError(err)
	is.EqualValues(1, es.Events, "event stats count only the event source")
}

// TestUnifiedNukeAllIsSourceScoped proves a queue NukeAll leaves the event
// ledger and its deliveries untouched.
func TestUnifiedNukeAllIsSourceScoped(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	q := newQueue(t, h)
	e := newEvent(t, h)
	ctx := context.Background()

	_, err := q.Producer().Enqueue(ctx, itJob{V: "q1"})
	is.NoError(err)
	is.NoError(e.Publisher().Register(ctx, event.Subscriber{Name: "sink", EventType: orderEvent{}.EventType(), MaxAttempts: 3}))
	_, err = e.Publisher().Publish(ctx, orderEvent{Amount: 1})
	is.NoError(err)

	report, err := q.Manager().NukeAll(ctx)
	is.NoError(err)
	is.EqualValues(1, report.Jobs)

	qStats, err := q.Manager().Stats(ctx, itJob{}.Kind())
	is.NoError(err)
	is.EqualValues(0, qStats.Pending, "the queue source was wiped")

	eStats, err := e.Manager().Stats(ctx)
	is.NoError(err)
	is.EqualValues(1, eStats.Events, "the event ledger survives a queue NukeAll")
	is.EqualValues(1, eStats.Pending, "event deliveries survive a queue NukeAll")
}

// TestUnifiedListenWakeDeliversPromptly sets a 10s poll interval so only a
// LISTEN/NOTIFY wakeup can deliver within a second — proving push wakeups work.
func TestUnifiedListenWakeDeliversPromptly(t *testing.T) {
	is := require.New(t)
	h := newHarness(t, azync.WithFetchPollInterval(10*time.Second), azync.WithIdleBackoffMax(10*time.Second))
	q := newQueue(t, h)
	ctx := context.Background()

	done := make(chan struct{}, 1)
	is.NoError(queue.Register(q.Worker(), func(_ context.Context, _ queue.Job[itJob]) error {
		done <- struct{}{}
		return nil
	}))
	startWorker(t, q.Worker())

	// Wait for the fetch loops to be running, then give the dedicated LISTEN
	// connection a moment to establish before the NOTIFY fires.
	select {
	case <-q.Worker().Ready():
	case <-time.After(3 * time.Second):
		t.Fatal("worker never became ready")
	}
	time.Sleep(300 * time.Millisecond)

	start := time.Now()
	_, err := q.Producer().Enqueue(ctx, itJob{V: "wakeup"})
	is.NoError(err)
	select {
	case <-done:
		is.Less(time.Since(start), time.Second, "NOTIFY woke the worker well before the 10s poll fallback")
	case <-time.After(time.Second):
		t.Fatal("LISTEN wakeup did not deliver within a second")
	}
}

// TestUnifiedPollOnlyDelivers proves the always-correct polling path works with
// push wakeups disabled (no listener at all).
func TestUnifiedPollOnlyDelivers(t *testing.T) {
	is := require.New(t)
	h := newHarness(t, azync.PollOnly())
	q := newQueue(t, h)
	ctx := context.Background()

	done := make(chan struct{}, 1)
	is.NoError(queue.Register(q.Worker(), func(_ context.Context, _ queue.Job[itJob]) error {
		done <- struct{}{}
		return nil
	}))
	startWorker(t, q.Worker())

	_, err := q.Producer().Enqueue(ctx, itJob{V: "polled"})
	is.NoError(err)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("poll-only worker did not deliver")
	}
}
