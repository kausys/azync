package integration

import (
	"context"
	"testing"

	"github.com/kausys/azync"
	"github.com/kausys/azync/queue"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
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

// columnExists reports whether the named column exists on the table visible
// on the pool's search_path.
func columnExists(t *testing.T, pool *pgxpool.Pool, table, column string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
		)`, table, column).Scan(&exists))
	return exists
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

// legacyTenantTraceColumns are the columns 00001/00002 originally shipped
// (v0.0.1/v0.0.2) that migration 00005 removes.
var legacyTenantTraceColumns = []struct{ table, column string }{
	{"azync_events", "tenant_id"},
	{"azync_events", "trace_id"},
	{"azync_events", "span_id"},
	{"azync_events", "trace_flags"},
	{"azync_jobs", "trace_id"},
	{"azync_jobs", "span_id"},
	{"azync_jobs", "trace_flags"},
	{"azync_job_attempts", "trace"},
	{"azync_dags", "trace_id"},
	{"azync_dags", "span_id"},
	{"azync_dags", "trace_flags"},
}

// TestMigrateFreshInstallHasNoLegacyTenantTraceColumns proves a fresh install
// converges on the schema with tenant/trace folded into meta, even though
// 00001/00002 are the restored v0.0.1/v0.0.2 files that (re)create those
// columns: 00005 drops them immediately after.
func TestMigrateFreshInstallHasNoLegacyTenantTraceColumns(t *testing.T) {
	is := require.New(t)
	base := requireDB(t)
	schema := newSchema(t, base)
	core := openUnmigrated(t, base, schema)
	is.NoError(core.Migrate(context.Background()))

	pool := newPool(t, base, schema)
	for _, c := range legacyTenantTraceColumns {
		is.False(columnExists(t, pool, c.table, c.column),
			"%s.%s should not exist after a fresh migrate", c.table, c.column)
	}
	is.False(relExists(t, pool, "azync_events_tenant_occurred_idx"))
}

// TestMigrateConvergesFromDriftedTenantTraceSchema simulates the exact drift
// window this migration exists to close: an environment that migrated with
// the original (pre-03bef2d) 00001/00002 has the legacy columns physically
// applied, and goose's version table has no way to know the files changed.
// It proves 00005 converges that environment to the same schema as a fresh
// install, and that a second run is idempotent.
func TestMigrateConvergesFromDriftedTenantTraceSchema(t *testing.T) {
	is := require.New(t)
	base := requireDB(t)
	schema := newSchema(t, base)
	core := openUnmigrated(t, base, schema)
	is.NoError(core.Migrate(context.Background()))

	pool := newPool(t, base, schema)
	ctx := context.Background()

	// Recreate exactly what the original 00001/00002 files applied, modeling
	// an environment that migrated before the tenant/trace columns were
	// folded into meta.
	_, err := pool.Exec(ctx, `
		ALTER TABLE azync_events
			ADD COLUMN tenant_id   uuid NULL,
			ADD COLUMN trace_id    text NULL,
			ADD COLUMN span_id     text NULL,
			ADD COLUMN trace_flags smallint NULL;
		CREATE INDEX azync_events_tenant_occurred_idx ON azync_events (tenant_id, occurred_at, id)
			WHERE tenant_id IS NOT NULL;
		ALTER TABLE azync_jobs
			ADD COLUMN trace_id    text NULL,
			ADD COLUMN span_id     text NULL,
			ADD COLUMN trace_flags smallint NULL;
		ALTER TABLE azync_job_attempts ADD COLUMN trace text NULL;
		ALTER TABLE azync_dags
			ADD COLUMN trace_id    text NULL,
			ADD COLUMN span_id     text NULL,
			ADD COLUMN trace_flags smallint NULL;
	`)
	is.NoError(err)

	// Un-record every migration from 00005 on, modeling an environment that
	// stopped upgrading before those files existed. Deleting only 00005 while
	// leaving 00006 recorded would itself be a state goose can never
	// legitimately produce (it enforces strict in-order application), and
	// goose's own gap detection correctly refuses to proceed from it.
	_, err = pool.Exec(ctx, `DELETE FROM azync_migrations WHERE version_id >= 5`)
	is.NoError(err)

	is.NoError(core.Migrate(ctx), "converging migration must succeed against the drifted schema")
	for _, c := range legacyTenantTraceColumns {
		is.False(columnExists(t, pool, c.table, c.column),
			"%s.%s should be dropped after convergence", c.table, c.column)
	}
	is.False(relExists(t, pool, "azync_events_tenant_occurred_idx"))
	is.True(relExists(t, pool, "azync_jobs_succeeded_vacuum_idx"), "00006 must also have run")

	// Idempotent: running again (the normal case, version already recorded)
	// is a no-op and does not error.
	is.NoError(core.Migrate(ctx))
}

// TestOldOnConflictPredicateInfersAgainstNewLiveIndex pins the rolling-deploy
// guarantee for 00009: a v0.0.5-era replica still runs CreateDAG's old ON
// CONFLICT clause — whose inference WHERE enumerates only the THREE pre-paused
// live states — after a new replica migrated and the old 3-state index is
// gone. Postgres must infer the new 4-state partial unique index as the
// arbiter (subset-IN predicate implication); if this ever regresses, every
// CreateDAG on a not-yet-redeployed replica fails with "no unique or
// exclusion constraint matching the ON CONFLICT specification" mid-deploy,
// which is exactly the outage this test exists to catch in CI instead.
func TestOldOnConflictPredicateInfersAgainstNewLiveIndex(t *testing.T) {
	is := require.New(t)
	base := requireDB(t)
	schema := newSchema(t, base)
	core := openUnmigrated(t, base, schema)
	ctx := context.Background()
	is.NoError(core.Migrate(ctx))
	pool := newPool(t, base, schema)

	// The old index must be gone and only the 4-state one present, or this
	// test would prove nothing.
	var oldExists bool
	is.NoError(pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT FROM pg_indexes WHERE schemaname = current_schema()
			AND indexname = 'azync_dags_idempotency_idx')`).Scan(&oldExists))
	is.False(oldExists, "00009 must have dropped the 3-state index")

	// The v0.0.5 insertDAGSQL, verbatim: 3-state inference predicate.
	const oldInsertDAGSQL = `
INSERT INTO azync_dags
	(id, name, state, on_failure, idempotency_key, meta, created_at, updated_at)
VALUES ($1, $2, 'running',
	CASE WHEN $3 = 'suspend' THEN 'suspend' ELSE 'cancel' END,
	NULLIF($4, ''), $5::jsonb, now(), now())
ON CONFLICT (name, idempotency_key)
	WHERE idempotency_key IS NOT NULL AND state IN ('running', 'suspended', 'compensating')
DO NOTHING
RETURNING id`

	var id pgtype.UUID
	err := pool.QueryRow(ctx, oldInsertDAGSQL,
		uuid.New(), "it-rolling", "cancel", "roll-key", `{}`).Scan(&id)
	is.NoError(err, "the old 3-state ON CONFLICT must infer the new 4-state index as arbiter")

	// And the barrier actually fires through the old clause: a duplicate live
	// key hits DO NOTHING (ErrNoRows from RETURNING), never a second row.
	err = pool.QueryRow(ctx, oldInsertDAGSQL,
		uuid.New(), "it-rolling", "cancel", "roll-key", `{}`).Scan(&id)
	is.ErrorIs(err, pgx.ErrNoRows, "the duplicate must be absorbed by DO NOTHING through the new index")

	var count int
	is.NoError(pool.QueryRow(ctx,
		`SELECT count(*) FROM azync_dags WHERE name = 'it-rolling'`).Scan(&count))
	is.Equal(1, count, "exactly one live run holds the key")
}
