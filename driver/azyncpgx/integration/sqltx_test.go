package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/dag"
	"github.com/kausys/azync/driver/azyncpgx"
	"github.com/kausys/azync/event"
	"github.com/kausys/azync/queue"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

// txBackend is one way an application holds the transaction it enlists azync
// in. Every outbox scenario below runs against each backend — the pgx.Tx the
// driver speaks natively and a database/sql *sql.Tx — so both paths are held
// to one contract instead of two suites that could drift apart.
type txBackend[TTx any] struct {
	core     *azync.Core
	begin    func(t *testing.T) TTx
	commit   func(t *testing.T, tx TTx)
	rollback func(t *testing.T, tx TTx)
}

// pgxBackend enlists in a pgx.Tx over a caller-owned pgx pool, against a Core
// opened through the registry.
func pgxBackend(t *testing.T, coreOpts ...azync.Option) txBackend[pgx.Tx] {
	t.Helper()
	h := newHarness(t, coreOpts...)
	pool := newPool(t, h.base, h.schema)
	return txBackend[pgx.Tx]{
		core: h.core,
		begin: func(t *testing.T) pgx.Tx {
			t.Helper()
			tx, err := pool.Begin(context.Background())
			require.NoError(t, err)
			// A scenario that fails mid-transaction must not leave it open:
			// its locks would block the schema drop on cleanup forever.
			t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
			return tx
		},
		commit: func(t *testing.T, tx pgx.Tx) {
			t.Helper()
			require.NoError(t, tx.Commit(context.Background()))
		},
		rollback: func(t *testing.T, tx pgx.Tx) {
			t.Helper()
			require.NoError(t, tx.Rollback(context.Background()))
		},
	}
}

// sqlBackend enlists in a *sql.Tx over a database/sql pool, against a Core
// built with azync.New over the store's SQLTx view.
func sqlBackend(t *testing.T, coreOpts ...azync.Option) txBackend[*sql.Tx] {
	t.Helper()
	is := require.New(t)
	base := requireDB(t)
	schema := newSchema(t, base)

	poolCfg, err := pgxpool.ParseConfig(base)
	is.NoError(err)
	poolCfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	is.NoError(err)
	t.Cleanup(pool.Close)

	store := azyncpgx.New(pool, azyncpgx.WithSchema(schema), azyncpgx.WithLogger(discardLogger()))
	core, err := azync.New(store.SQLTx(), append(fastRuntimeOptions(), coreOpts...)...)
	is.NoError(err)
	t.Cleanup(func() { _ = core.Close(context.Background()) })
	is.NoError(core.Migrate(context.Background()))

	connCfg, err := pgx.ParseConfig(base)
	is.NoError(err)
	connCfg.RuntimeParams["search_path"] = schema
	db := stdlib.OpenDB(*connCfg)
	t.Cleanup(func() { _ = db.Close() })

	return txBackend[*sql.Tx]{
		core: core,
		begin: func(t *testing.T) *sql.Tx {
			t.Helper()
			tx, err := db.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() }) // see pgxBackend
			return tx
		},
		commit: func(t *testing.T, tx *sql.Tx) {
			t.Helper()
			require.NoError(t, tx.Commit())
		},
		rollback: func(t *testing.T, tx *sql.Tx) {
			t.Helper()
			require.NoError(t, tx.Rollback())
		},
	}
}

func TestOutboxOverPgxTx(t *testing.T) { runOutboxSuite(t, pgxBackend) }

func TestOutboxOverSQLTx(t *testing.T) { runOutboxSuite(t, sqlBackend) }

// runOutboxSuite is the transactional contract every backend meets: what a
// rolled-back transaction enlisted leaves nothing behind, what a committed one
// enlisted is there, and the deduplication paths — which only report through
// a query returning no row — answer the same way.
func runOutboxSuite[TTx any](t *testing.T, newBackend func(t *testing.T, coreOpts ...azync.Option) txBackend[TTx]) {
	t.Helper()
	t.Run("publish", func(t *testing.T) {
		is := require.New(t)
		b := newBackend(t)
		ctx := context.Background()
		e, err := event.New(b.core)
		is.NoError(err)
		is.NoError(e.Publisher().Register(ctx, event.Subscription{Name: "sink", EventType: orderEvent{}.EventType(), MaxAttempts: 3}))
		txp, err := e.TxPublisher[TTx]()
		is.NoError(err)

		tx := b.begin(t)
		rolledID, err := txp.PublishTx(ctx, tx, orderEvent{Amount: 1})
		is.NoError(err)
		b.rollback(t, tx)
		gone, err := e.Manager().Get(ctx, rolledID)
		is.NoError(err)
		is.Nil(gone, "a rolled-back PublishTx leaves no ledger event")

		tx = b.begin(t)
		committedID, err := txp.PublishTx(ctx, tx, orderEvent{Amount: 2})
		is.NoError(err)
		b.commit(t, tx)
		got, err := e.Manager().Get(ctx, committedID)
		is.NoError(err)
		is.NotNil(got, "a committed PublishTx persists the ledger event")
		stats, err := e.Manager().Stats(ctx)
		is.NoError(err)
		is.EqualValues(1, stats.Events)
		is.EqualValues(1, stats.Pending, "the committed event fanned out one delivery")
	})

	t.Run("enqueue", func(t *testing.T) {
		is := require.New(t)
		b := newBackend(t)
		ctx := context.Background()
		q, err := queue.New(b.core, queue.WithCron(false))
		is.NoError(err)
		txp, err := q.TxProducer[TTx]()
		is.NoError(err)

		tx := b.begin(t)
		rolled, err := txp.EnqueueTx(ctx, tx, itJob{V: "rollback"})
		is.NoError(err)
		b.rollback(t, tx)
		gone, err := q.Manager().Get(ctx, rolled.ID)
		is.NoError(err)
		is.Nil(gone, "a rolled-back EnqueueTx leaves no job")

		tx = b.begin(t)
		committed, err := txp.EnqueueTx(ctx, tx, itJob{V: "commit"})
		is.NoError(err)
		// A live job holding the key: the insert returns no row.
		first, err := txp.EnqueueTx(ctx, tx, itJob{V: "live"}, queue.IdempotencyKey("live"))
		is.NoError(err)
		again, err := txp.EnqueueTx(ctx, tx, itJob{V: "live"}, queue.IdempotencyKey("live"))
		is.NoError(err)
		// A claimed time window: the claim returns no row.
		windowed, err := txp.EnqueueTx(ctx, tx, itJob{V: "window"}, queue.IdempotencyKeyTTL("window", time.Minute))
		is.NoError(err)
		inWindow, err := txp.EnqueueTx(ctx, tx, itJob{V: "window"}, queue.IdempotencyKeyTTL("window", time.Minute))
		is.NoError(err)
		b.commit(t, tx)

		got, err := q.Manager().Get(ctx, committed.ID)
		is.NoError(err)
		is.NotNil(got, "a committed EnqueueTx persists the job")
		is.False(first.Deduplicated)
		is.True(again.Deduplicated, "a live job holding the key deduplicates")
		is.False(windowed.Deduplicated)
		is.True(inWindow.Deduplicated, "a claimed window deduplicates")
		stats, err := q.Manager().Stats(ctx, itJob{}.Kind())
		is.NoError(err)
		is.EqualValues(3, stats.Pending, "commit, live and window — the duplicates never landed")
	})

	t.Run("create dag", func(t *testing.T) {
		is := require.New(t)
		b := newBackend(t)
		ctx := context.Background()
		d, err := dag.New(b.core)
		is.NoError(err)
		txr, err := d.TxRunner[TTx]()
		is.NoError(err)
		def := func() *dag.Definition { return dag.Define("it-tx").Task("poll", provisionPoll{Ref: "x"}) }

		tx := b.begin(t)
		rolled, err := txr.RunTx(ctx, tx, def())
		is.NoError(err)
		b.rollback(t, tx)
		gone, err := d.Manager().Get(ctx, rolled.ID)
		is.NoError(err)
		is.Nil(gone, "a rolled-back RunTx leaves no workflow")

		tx = b.begin(t)
		created, err := txr.RunTx(ctx, tx, def(), dag.WithIdempotencyKey("once"))
		is.NoError(err)
		b.commit(t, tx)
		got, err := d.Manager().Get(ctx, created.ID)
		is.NoError(err)
		is.NotNil(got, "a committed RunTx persists the workflow")

		// The duplicate resolves the live execution's id through a second
		// query, scanned back through the backend's own row type.
		tx = b.begin(t)
		dup, err := txr.RunTx(ctx, tx, def(), dag.WithIdempotencyKey("once"))
		is.NoError(err)
		b.commit(t, tx)
		is.True(dup.Deduplicated)
		is.Equal(created.ID, dup.ID, "the duplicate names the live execution")
	})

	t.Run("wakeup fires only on commit", func(t *testing.T) {
		is := require.New(t)
		// A ten-second poll: a delivery within a second can only be the NOTIFY.
		b := newBackend(t, azync.WithFetchPollInterval(10*time.Second), azync.WithIdleBackoffMax(10*time.Second))
		ctx := context.Background()
		e, err := event.New(b.core)
		is.NoError(err)
		delivered := make(chan int, 2)
		is.NoError(e.Worker().RegisterFunc("sink", func(_ context.Context, evt orderEvent) error {
			delivered <- evt.Amount
			return nil
		}))
		startWorker(t, e.Worker())
		awaitEventReady(t, e)
		time.Sleep(300 * time.Millisecond) // let the listener settle before the first NOTIFY
		txp, err := e.TxPublisher[TTx]()
		is.NoError(err)

		tx := b.begin(t)
		_, err = txp.PublishTx(ctx, tx, orderEvent{Amount: 1})
		is.NoError(err)
		b.rollback(t, tx)

		tx = b.begin(t)
		_, err = txp.PublishTx(ctx, tx, orderEvent{Amount: 2})
		is.NoError(err)
		b.commit(t, tx)

		select {
		case amount := <-delivered:
			is.Equal(2, amount, "only the committed event is delivered")
		case <-time.After(time.Second):
			t.Fatal("the commit did not send the NOTIFY that wakes the worker")
		}
		select {
		case amount := <-delivered:
			t.Fatalf("a second delivery arrived: %d", amount)
		case <-time.After(300 * time.Millisecond):
		}
	})
}

// TestSQLTxCoreServesOneTransactionType proves the limit SQLTxStore documents:
// a Core built over the view resolves the transactional clients for *sql.Tx
// and refuses them for pgx.Tx, at construction rather than at first use.
func TestSQLTxCoreServesOneTransactionType(t *testing.T) {
	is := require.New(t)
	b := sqlBackend(t)
	e, err := event.New(b.core)
	is.NoError(err)

	_, err = e.TxPublisher[*sql.Tx]()
	is.NoError(err)
	_, err = e.TxPublisher[pgx.Tx]()
	is.ErrorContains(err, "does not support transactional publishes with transaction type pgx.Tx")
}
