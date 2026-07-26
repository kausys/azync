package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/driver/azyncpgx"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// newBatchedStore builds a Core directly over an azyncpgx.Store constructed
// with a small WithMaintenanceBatch, so PromoteDue/ReapExpired/VacuumDead
// must loop internally to process more than one batch's worth of rows —
// exactly the behavior the registry path (azync.Open) has no option to
// configure.
func newBatchedStore(t *testing.T, base, schema string, batch int) *azync.Core {
	t.Helper()
	is := require.New(t)

	admin, err := pgxpool.New(context.Background(), base)
	is.NoError(err)
	t.Cleanup(admin.Close)
	_, err = admin.Exec(context.Background(), `CREATE SCHEMA IF NOT EXISTS `+schema)
	is.NoError(err)

	cfg, err := pgxpool.ParseConfig(base)
	is.NoError(err)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	is.NoError(err)
	t.Cleanup(pool.Close)

	store := azyncpgx.New(pool, azyncpgx.WithSchema(schema), azyncpgx.WithMaintenanceBatch(batch))
	core, err := azync.New(store, azync.WithLogger(discardLogger()))
	is.NoError(err)
	t.Cleanup(func() { _ = core.Close(context.Background()) })
	is.NoError(core.Migrate(context.Background()))
	return core
}

// TestPromoteDueProcessesMoreThanOneBatch proves PromoteDue loops internally
// until every due job is promoted, not just the first WithMaintenanceBatch of
// them.
func TestPromoteDueProcessesMoreThanOneBatch(t *testing.T) {
	is := require.New(t)
	base := requireDB(t)
	schema := newSchema(t, base)
	core := newBatchedStore(t, base, schema, 2)
	store := core.Store()
	ctx := context.Background()

	const kind = "batch_promote"
	const total = 5
	for range total {
		_, err := store.Enqueue(ctx, driver.EnqueueParams{
			ID: uuid.New(), Kind: kind, Payload: json.RawMessage(`{}`), Delay: 10 * time.Millisecond,
		})
		is.NoError(err)
	}
	time.Sleep(30 * time.Millisecond) // let run_at fall into the past while still scheduled

	promoted, err := store.PromoteDue(ctx, driver.SourceQueue, []string{kind})
	is.NoError(err)
	is.EqualValues(total, promoted, "PromoteDue must loop past a single batch to promote every due job")

	depths, err := store.KindDepths(ctx, driver.SourceQueue)
	is.NoError(err)
	is.EqualValues(total, depths[kind].Pending)
}

// TestReapExpiredProcessesMoreThanOneBatch proves ReapExpired loops
// internally (one transaction per batch) until every expired lease is
// reclaimed, not just the first WithMaintenanceBatch of them.
func TestReapExpiredProcessesMoreThanOneBatch(t *testing.T) {
	is := require.New(t)
	base := requireDB(t)
	schema := newSchema(t, base)
	core := newBatchedStore(t, base, schema, 2)
	store := core.Store()
	ctx := context.Background()

	const kind = "batch_reap"
	const total = 5
	for range total {
		_, err := store.Enqueue(ctx, driver.EnqueueParams{
			ID: uuid.New(), Kind: kind, Payload: json.RawMessage(`{}`),
		})
		is.NoError(err)
	}
	leased, err := store.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{
		Kind: kind, Limit: total, Lease: 10 * time.Millisecond,
	})
	is.NoError(err)
	is.Len(leased, total)
	time.Sleep(30 * time.Millisecond) // let every lease expire

	reaped, killed, err := store.ReapExpired(ctx, driver.SourceQueue, []string{kind}, 5)
	is.NoError(err)
	is.EqualValues(total, reaped, "ReapExpired must loop past a single batch to reclaim every expired lease")
	is.Zero(killed)

	depths, err := store.KindDepths(ctx, driver.SourceQueue)
	is.NoError(err)
	is.EqualValues(total, depths[kind].Pending)
}
