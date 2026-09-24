package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kausys/azync/dag"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/driver/azyncpgx"
	"github.com/kausys/azync/event"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

// outboxSide is an application database holding an outbox, apart from the
// harness Core it forwards to: its own schema, reached through database/sql
// and pgx the way an application reaches its tables, with no search_path —
// the outbox qualifies every statement itself.
type outboxSide struct {
	outbox *azyncpgx.Outbox
	schema string
	db     *sql.DB
	pool   *pgxpool.Pool
}

func newOutboxSide(t *testing.T, base string) outboxSide {
	t.Helper()
	is := require.New(t)
	schema := newSchema(t, base)
	outbox, err := azyncpgx.NewOutbox(azyncpgx.WithOutboxSchema(schema))
	is.NoError(err)

	cfg, err := pgx.ParseConfig(base)
	is.NoError(err)
	db := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = db.Close() })
	pool, err := pgxpool.New(context.Background(), base)
	is.NoError(err)
	t.Cleanup(pool.Close)

	is.NoError(outbox.Migrate(context.Background(), db))
	return outboxSide{outbox: outbox, schema: schema, db: db, pool: pool}
}

func (s outboxSide) table() string {
	return pgx.Identifier{s.schema, "azync_outbox"}.Sanitize()
}

func (s outboxSide) count(t *testing.T) int {
	t.Helper()
	var n int
	require.NoError(t, s.pool.QueryRow(context.Background(), `SELECT count(*) FROM `+s.table()).Scan(&n))
	return n
}

func (s outboxSide) begin(t *testing.T) *sql.Tx {
	t.Helper()
	tx, err := s.db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

// drainAll drains until a batch comes back short, summing what each did.
func (s outboxSide) drainAll(t *testing.T, target driver.Store) azyncpgx.DrainResult {
	t.Helper()
	var total azyncpgx.DrainResult
	for {
		res, err := s.outbox.Drain(context.Background(), s.db, target, 10)
		require.NoError(t, err)
		total.Forwarded += res.Forwarded
		total.Quarantined += res.Quarantined
		if res.Forwarded+res.Quarantined < 10 {
			return total
		}
	}
}

func TestOutboxMigrateIsIdempotentWithItsOwnHistory(t *testing.T) {
	is := require.New(t)
	base := requireDB(t)
	side := newOutboxSide(t, base)
	ctx := context.Background()

	is.NoError(side.outbox.Migrate(ctx, side.db), "a second run is a no-op")
	is.True(relExists(t, side.pool, side.schema+".azync_outbox"))
	is.True(relExists(t, side.pool, side.schema+".azync_outbox_migrations"), "its own history table")
	is.False(relExists(t, side.pool, side.schema+".azync_jobs"), "none of the store's tables")
	var applied int
	is.NoError(side.pool.QueryRow(ctx, `SELECT count(*) FROM `+
		pgx.Identifier{side.schema, "azync_outbox_migrations"}.Sanitize()+` WHERE version_id = 1 AND is_applied`).Scan(&applied))
	is.Equal(1, applied, "version 1 recorded once")

	// Concurrent first runs against a fresh schema serialize on the lock.
	fresh, err := azyncpgx.NewOutbox(azyncpgx.WithOutboxSchema(newSchema(t, base)))
	is.NoError(err)
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Go(func() { errs[i] = fresh.Migrate(ctx, side.db) })
	}
	wg.Wait()
	is.NoError(errors.Join(errs...))
}

func TestOutboxRefusesASchemaThatIsNotLowercase(t *testing.T) {
	is := require.New(t)
	for _, schema := range []string{"Tenant", "tenant-1", "1tenant"} {
		_, err := azyncpgx.NewOutbox(azyncpgx.WithOutboxSchema(schema))
		is.Error(err, schema)
	}
	_, err := azyncpgx.NewOutbox()
	is.NoError(err, "no schema: the search_path decides")
}

func TestOutboxCapturesOnlyWhatCommits(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	side := newOutboxSide(t, h.base)
	ctx := context.Background()
	e := newEvent(t, h)

	overSQL, err := e.TxPublisherVia(side.outbox.SQLTx())
	is.NoError(err)
	tx := side.begin(t)
	_, err = overSQL.PublishTx(ctx, tx, orderEvent{Amount: 1})
	is.NoError(err)
	is.NoError(tx.Rollback())
	is.Zero(side.count(t), "a rolled-back capture leaves nothing")

	tx = side.begin(t)
	id, err := overSQL.PublishTx(ctx, tx, orderEvent{Amount: 2})
	is.NoError(err)
	is.NoError(tx.Commit())

	overPgx, err := e.TxPublisherVia(side.outbox.TxStore())
	is.NoError(err)
	ptx, err := side.pool.Begin(ctx)
	is.NoError(err)
	_, err = overPgx.PublishTx(ctx, ptx, orderEvent{Amount: 3})
	is.NoError(err)
	is.NoError(ptx.Commit(ctx))

	is.Equal(2, side.count(t), "both transaction types capture")
	var kind, topic string
	is.NoError(side.pool.QueryRow(ctx, `SELECT kind, topic FROM `+side.table()+` WHERE id = $1`, id).Scan(&kind, &topic))
	is.Equal("event", kind)
	is.Equal(orderEvent{}.EventType(), topic)
	view, err := e.Manager().Get(ctx, id)
	is.NoError(err)
	is.Nil(view, "nothing reaches the store before a drain")
}

type outboxDAGTask struct {
	N int `json:"n"`
}

func (outboxDAGTask) Kind() string { return "wf.it.outbox" }

func TestOutboxDrainForwardsEachKindAsItWasBuilt(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	side := newOutboxSide(t, h.base)
	ctx := context.Background()
	e, q, d := newEvent(t, h), newQueue(t, h), newWorkflow(t, h)
	is.NoError(e.Publisher().Register(ctx, event.Subscription{Name: "sink", EventType: orderEvent{}.EventType(), MaxAttempts: 3}))

	publisher, err := e.TxPublisherVia(side.outbox.SQLTx())
	is.NoError(err)
	producer, err := q.TxProducerVia(side.outbox.SQLTx())
	is.NoError(err)
	runner, err := d.TxRunnerVia(side.outbox.SQLTx())
	is.NoError(err)

	tx := side.begin(t)
	eventID, err := publisher.PublishTx(ctx, tx, orderEvent{Amount: 7}, event.WithMeta("origin", "outbox"))
	is.NoError(err)
	job, err := producer.EnqueueTx(ctx, tx, itJob{V: "outbox"})
	is.NoError(err)
	// No compensation and a timer with no payload: both must arrive as SQL
	// NULL, exactly as a DAG created directly would.
	run, err := runner.RunTx(ctx, tx, dag.Define("it-outbox").
		Task("a", outboxDAGTask{N: 1}).
		Sleep("wait", time.Hour, dag.After("a")))
	is.NoError(err)
	is.NoError(tx.Commit())

	var captured driver.PublishParams
	var raw []byte
	is.NoError(side.pool.QueryRow(ctx, `SELECT params FROM `+side.table()+` WHERE id = $1`, eventID).Scan(&raw))
	is.NoError(json.Unmarshal(raw, &captured))

	res, err := side.outbox.Drain(ctx, side.db, h.core.Store(), 0)
	is.NoError(err)
	is.Equal(azyncpgx.DrainResult{Forwarded: 3}, res)
	is.Zero(side.count(t), "forwarded rows are removed")

	forwarded, err := e.Manager().Get(ctx, eventID)
	is.NoError(err)
	is.NotNil(forwarded)
	is.JSONEq(`{"amount":7}`, string(forwarded.Payload))
	is.Equal("outbox", forwarded.Meta["origin"])
	is.True(forwarded.OccurredAt.Equal(captured.OccurredAt), "the time it was built, not the time it was drained")
	stats, err := e.Manager().Stats(ctx)
	is.NoError(err)
	is.EqualValues(1, stats.Pending, "fanned out on forwarding")

	queued, err := q.Manager().Get(ctx, job.ID)
	is.NoError(err)
	is.NotNil(queued)

	created, err := d.Manager().Get(ctx, run.ID)
	is.NoError(err)
	is.NotNil(created)
	global := newPool(t, h.base, h.schema)
	var nonNull int
	is.NoError(global.QueryRow(ctx, `SELECT count(*) FROM azync_jobs WHERE dag_id = $1
		AND (compensation_payload IS NOT NULL OR (kind = '$sleep' AND payload IS NULL))`, run.ID).Scan(&nonNull))
	is.Zero(nonNull, "no JSON null stored where a direct creation stores SQL NULL")
}

func TestOutboxDrainTreatsAnAlreadyForwardedRowAsDone(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	side := newOutboxSide(t, h.base)
	ctx := context.Background()
	e := newEvent(t, h)
	is.NoError(e.Publisher().Register(ctx, event.Subscription{Name: "sink", EventType: orderEvent{}.EventType(), MaxAttempts: 3}))

	publisher, err := e.TxPublisherVia(side.outbox.SQLTx())
	is.NoError(err)
	tx := side.begin(t)
	id, err := publisher.PublishTx(ctx, tx, orderEvent{Amount: 1})
	is.NoError(err)
	is.NoError(tx.Commit())

	// A drain that forwarded the row and crashed before removing it.
	var raw []byte
	is.NoError(side.pool.QueryRow(ctx, `SELECT params FROM `+side.table()+` WHERE id = $1`, id).Scan(&raw))
	var p driver.PublishParams
	is.NoError(json.Unmarshal(raw, &p))
	_, err = h.core.Store().Publish(ctx, p)
	is.NoError(err)

	res, err := side.outbox.Drain(ctx, side.db, h.core.Store(), 0)
	is.NoError(err)
	is.Equal(azyncpgx.DrainResult{Forwarded: 1}, res, "the target already had it: done")
	is.Zero(side.count(t))
	stats, err := e.Manager().Stats(ctx)
	is.NoError(err)
	is.EqualValues(1, stats.Events)
	is.EqualValues(1, stats.Pending, "one delivery, not two")
}

func TestOutboxDrainQuarantinesWhatCanNeverBeForwarded(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	side := newOutboxSide(t, h.base)
	ctx := context.Background()
	e := newEvent(t, h)

	publisher, err := e.TxPublisherVia(side.outbox.SQLTx())
	is.NoError(err)
	tx := side.begin(t)
	good, err := publisher.PublishTx(ctx, tx, orderEvent{Amount: 1})
	is.NoError(err)
	is.NoError(tx.Commit())
	_, err = side.pool.Exec(ctx, `INSERT INTO `+side.table()+` (id, kind, topic, format, params) VALUES
		(gen_random_uuid(), 'event', 'x', 1, '{"id":"not-a-uuid"}'),
		(gen_random_uuid(), 'event', 'x', 99, '{}')`)
	is.NoError(err)

	res, err := side.outbox.Drain(ctx, side.db, h.core.Store(), 0)
	is.NoError(err)
	is.Equal(azyncpgx.DrainResult{Forwarded: 1, Quarantined: 2}, res, "the good row is not held back by the bad ones")
	forwarded, err := e.Manager().Get(ctx, good)
	is.NoError(err)
	is.NotNil(forwarded)

	rows, err := side.pool.Query(ctx, `SELECT last_error FROM `+side.table()+` WHERE quarantined_at IS NOT NULL ORDER BY seq`)
	is.NoError(err)
	reasons, err := pgx.CollectRows(rows, pgx.RowTo[string])
	is.NoError(err)
	is.Len(reasons, 2)
	is.Contains(reasons[0], "decode event params")
	is.Contains(reasons[1], "unknown outbox format 99")

	again, err := side.outbox.Drain(ctx, side.db, h.core.Store(), 0)
	is.NoError(err)
	is.Equal(azyncpgx.DrainResult{}, again, "quarantined rows stay out of later drains")
}

// flakyTarget fails the failOn-th Publish, as an unreachable store would.
type flakyTarget struct {
	driver.Store
	calls, failOn int
}

func (s *flakyTarget) Publish(ctx context.Context, p driver.PublishParams) (int, error) {
	s.calls++
	if s.calls == s.failOn {
		return 0, errors.New("target unreachable")
	}
	return s.Store.Publish(ctx, p)
}

func TestOutboxDrainStopsAtATransientFailureAndKeepsWhatItForwarded(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	side := newOutboxSide(t, h.base)
	ctx := context.Background()
	e := newEvent(t, h)

	publisher, err := e.TxPublisherVia(side.outbox.SQLTx())
	is.NoError(err)
	tx := side.begin(t)
	for i := range 3 {
		_, err := publisher.PublishTx(ctx, tx, orderEvent{Amount: i})
		is.NoError(err)
	}
	is.NoError(tx.Commit())

	res, err := side.outbox.Drain(ctx, side.db, &flakyTarget{Store: h.core.Store(), failOn: 2}, 0)
	is.ErrorContains(err, "target unreachable")
	is.Equal(azyncpgx.DrainResult{Forwarded: 1}, res)
	is.Equal(2, side.count(t), "the rest wait for the next drain")

	res, err = side.outbox.Drain(ctx, side.db, h.core.Store(), 0)
	is.NoError(err)
	is.Equal(azyncpgx.DrainResult{Forwarded: 2}, res)
	stats, err := e.Manager().Stats(ctx)
	is.NoError(err)
	is.EqualValues(3, stats.Events)
}

func TestOutboxConcurrentDrainsForwardEachRowOnce(t *testing.T) {
	is := require.New(t)
	h := newHarness(t)
	side := newOutboxSide(t, h.base)
	ctx := context.Background()
	e := newEvent(t, h)
	is.NoError(e.Publisher().Register(ctx, event.Subscription{Name: "sink", EventType: orderEvent{}.EventType(), MaxAttempts: 3}))

	const total = 60
	publisher, err := e.TxPublisherVia(side.outbox.SQLTx())
	is.NoError(err)
	tx := side.begin(t)
	for i := range total {
		_, err := publisher.PublishTx(ctx, tx, orderEvent{Amount: i})
		is.NoError(err)
	}
	is.NoError(tx.Commit())

	var wg sync.WaitGroup
	forwarded := make([]int, 6)
	for i := range forwarded {
		wg.Go(func() {
			for {
				res, err := side.outbox.Drain(ctx, side.db, h.core.Store(), 7)
				if err != nil || res.Forwarded == 0 {
					return
				}
				forwarded[i] += res.Forwarded
			}
		})
	}
	wg.Wait()
	rest := side.drainAll(t, h.core.Store()) // rows skipped while another drain held them

	sum := rest.Forwarded
	for _, n := range forwarded {
		sum += n
	}
	is.Equal(total, sum, "every row forwarded exactly once")
	stats, err := e.Manager().Stats(ctx)
	is.NoError(err)
	is.EqualValues(total, stats.Events)
	is.EqualValues(total, stats.Pending, "one delivery per event")
}

func TestOutboxCaptureRefusesAnIDItAlreadyHolds(t *testing.T) {
	is := require.New(t)
	base := requireDB(t)
	side := newOutboxSide(t, base)
	p := driver.PublishParams{ID: uuid.New(), Type: "t", OccurredAt: time.Now(), Payload: json.RawMessage(`{}`)}

	tx := side.begin(t)
	_, err := side.outbox.SQLTx().PublishTx(context.Background(), tx, p)
	is.NoError(err)
	_, err = side.outbox.SQLTx().PublishTx(context.Background(), tx, p)
	is.ErrorIs(err, driver.ErrAlreadyExists)
}
