package integration

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/event"
	"github.com/kausys/azync/queue"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestTxProducerRollbackAndCommit proves the queue outbox is transactional:
// EnqueueTx in a rolled-back transaction leaves no job, and a committed one both
// persists the job and fires the after-commit NOTIFY that wakes a worker.
func TestTxProducerRollbackAndCommit(t *testing.T) {
	is := require.New(t)
	h := newHarness(t, azync.WithFetchPollInterval(10*time.Second), azync.WithIdleBackoffMax(10*time.Second))
	q := newQueue(t, h)
	ctx := context.Background()
	pool := newPool(t, h.base, h.schema)

	txp, err := queue.TxProducer[pgx.Tx](q)
	is.NoError(err)

	// Rollback: the enqueue never lands.
	tx, err := pool.Begin(ctx)
	is.NoError(err)
	rolled, err := txp.EnqueueTx(ctx, tx, itJob{V: "rollback"})
	is.NoError(err)
	is.NoError(tx.Rollback(ctx))
	got, err := q.Manager().Get(ctx, rolled.ID)
	is.NoError(err)
	is.Nil(got, "a rolled-back EnqueueTx leaves no job")
	stats, err := q.Manager().Stats(ctx, itJob{}.Kind())
	is.NoError(err)
	is.EqualValues(0, stats.Pending)

	// Commit: the job lands and its after-commit NOTIFY wakes the worker under a
	// 10s poll interval, so prompt delivery can only be the NOTIFY.
	done := make(chan struct{}, 1)
	is.NoError(queue.Register(q.Worker(), func(context.Context, itJob) error {
		done <- struct{}{}
		return nil
	}))
	startWorker(t, q.Worker())
	select {
	case <-q.Worker().Ready():
	case <-time.After(3 * time.Second):
		t.Fatal("worker never became ready")
	}
	time.Sleep(300 * time.Millisecond)

	tx, err = pool.Begin(ctx)
	is.NoError(err)
	committed, err := txp.EnqueueTx(ctx, tx, itJob{V: "commit"})
	is.NoError(err)
	is.NoError(tx.Commit(ctx))

	got, err = q.Manager().Get(ctx, committed.ID)
	is.NoError(err)
	is.NotNil(got, "a committed EnqueueTx persists the job")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("committed EnqueueTx did not fire a NOTIFY that woke the worker")
	}
}

// TestTxPublisherRollbackAndCommit proves the event outbox is transactional: a
// rolled-back PublishTx leaves neither the ledger event nor its deliveries, and
// a committed one delivers to a subscriber via the after-commit NOTIFY.
func TestTxPublisherRollbackAndCommit(t *testing.T) {
	is := require.New(t)
	h := newHarness(t, azync.WithFetchPollInterval(10*time.Second), azync.WithIdleBackoffMax(10*time.Second))
	e := newEvent(t, h)
	ctx := context.Background()
	pool := newPool(t, h.base, h.schema)

	is.NoError(e.Publisher().Register(ctx, event.Subscription{Name: "sink", EventType: orderEvent{}.EventType(), MaxAttempts: 3}))

	txp, err := event.TxPublisher[pgx.Tx](e)
	is.NoError(err)

	// Rollback: neither the event nor any delivery survives.
	tx, err := pool.Begin(ctx)
	is.NoError(err)
	rolledID, err := txp.PublishTx(ctx, tx, orderEvent{Amount: 1})
	is.NoError(err)
	is.NoError(tx.Rollback(ctx))
	gone, err := e.Manager().Get(ctx, rolledID)
	is.NoError(err)
	is.Nil(gone, "a rolled-back PublishTx leaves no ledger event")
	stats, err := e.Manager().Stats(ctx)
	is.NoError(err)
	is.EqualValues(0, stats.Events)
	is.EqualValues(0, stats.Pending, "no deliveries were created")

	// Commit: the event and its delivery land, and the NOTIFY wakes the worker.
	done := make(chan struct{}, 1)
	is.NoError(event.RegisterFunc(e.Worker(), "sink", func(context.Context, orderEvent) error {
		done <- struct{}{}
		return nil
	}))
	startWorker(t, e.Worker())
	select {
	case <-e.Worker().Ready():
	case <-time.After(3 * time.Second):
		t.Fatal("worker never became ready")
	}
	time.Sleep(300 * time.Millisecond)

	tx, err = pool.Begin(ctx)
	is.NoError(err)
	committedID, err := txp.PublishTx(ctx, tx, orderEvent{Amount: 2})
	is.NoError(err)
	is.NoError(tx.Commit(ctx))

	got, err := e.Manager().Get(ctx, committedID)
	is.NoError(err)
	is.NotNil(got, "a committed PublishTx persists the ledger event")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("committed PublishTx did not fire a NOTIFY that woke the worker")
	}
}
