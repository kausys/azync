package integration

import (
	"context"
	"testing"

	"github.com/kausys/azync"
	"github.com/kausys/azync/queue"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// openUnmigrated opens a Core against a fresh ephemeral schema WITHOUT migrating,
// so the migration tests drive Migrate explicitly.
func openUnmigrated(t *testing.T, base, schema string, opts ...azync.Option) *azync.Core {
	t.Helper()
	core, err := azync.Open(base, append([]azync.Option{
		azync.WithSchema(schema),
		azync.WithLogger(discardLogger()),
	}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = core.Close(context.Background()) })
	return core
}

// relExists reports whether a relation is visible on the pool's search_path.
func relExists(t *testing.T, pool *pgxpool.Pool, name string) bool {
	t.Helper()
	var reg *string
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT to_regclass($1)::text`, name).Scan(&reg))
	return reg != nil
}

// TestMigrateCreatesSchemaAndIsIdempotent proves Migrate creates the schema and
// its tables (including the default version table), that it is required before
// use, and that running it again is a no-op.
func TestMigrateCreatesSchemaAndIsIdempotent(t *testing.T) {
	is := require.New(t)
	base := requireDB(t)
	schema := newSchema(t, base)
	core := openUnmigrated(t, base, schema)
	ctx := context.Background()

	q, err := queue.New(core, queue.WithCron(false))
	is.NoError(err)

	// Before migration the tables do not exist, so an enqueue fails.
	_, err = q.Producer().Enqueue(ctx, itJob{V: "before"})
	is.Error(err, "Enqueue before Migrate fails: the schema has no tables yet")

	is.NoError(core.Migrate(ctx), "Migrate creates the schema and its tables")

	// After migration the enqueue succeeds.
	_, err = q.Producer().Enqueue(ctx, itJob{V: "after"})
	is.NoError(err)

	// Migrate is idempotent: a second run is a no-op.
	is.NoError(core.Migrate(ctx))

	pool := newPool(t, base, schema)
	is.True(relExists(t, pool, "azync_jobs"), "the unified job table exists")
	is.True(relExists(t, pool, "azync_events"), "the event ledger exists")
	is.True(relExists(t, pool, "azync_migrations"), "the default version table exists")
}

// TestMigrateCustomVersionTable proves WithMigrationsTable redirects goose's
// version bookkeeping to the named table, leaving the default table absent.
func TestMigrateCustomVersionTable(t *testing.T) {
	is := require.New(t)
	base := requireDB(t)
	schema := newSchema(t, base)
	core := openUnmigrated(t, base, schema, azync.WithMigrationsTable("azync_schema_history"))
	ctx := context.Background()

	is.NoError(core.Migrate(ctx))

	pool := newPool(t, base, schema)
	is.True(relExists(t, pool, "azync_schema_history"), "the custom version table is used")
	is.False(relExists(t, pool, "azync_migrations"), "the default version table is not created")
	is.True(relExists(t, pool, "azync_jobs"), "the schema still migrated normally")
}
