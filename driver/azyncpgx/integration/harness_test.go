// Package integration runs azync's end-to-end suite against a live PostgreSQL
// through the public API (azync.Open with a blank import of the pgx driver, then
// queue.New / event.New over a shared Core). Every test isolates itself in an
// ephemeral schema dropped on cleanup, so the suite is safe to run repeatedly
// against a shared database.
package integration

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	// Register the PostgreSQL driver under the postgres:// scheme.
	_ "github.com/kausys/azync/driver/azyncpgx"

	"github.com/kausys/azync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// schemaCounter guarantees a unique schema suffix even for two schemas created
// within the same nanosecond.
var schemaCounter atomic.Int64

// discardLogger silences the runtime's structured logs during tests.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// requireDB resolves the integration DSN (admin connection, no search_path),
// pings it within one second, and skips the test when the database is
// unavailable. It returns the credential-bearing base DSN.
func requireDB(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("AZYNC_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			envOr("DATABASE_USER", "azync"),
			url.QueryEscape(envOr("DATABASE_PASSWORD", "azync")),
			envOr("DATABASE_HOST", "localhost"),
			envOr("DATABASE_PORT", "5432"),
			envOr("DATABASE_NAME", "azync"),
		)
	}
	base := stripSearchPath(t, dsn)

	pool, err := pgxpool.New(context.Background(), base)
	if err != nil {
		t.Skipf("integration database unavailable: %v", err)
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		t.Skipf("integration database unavailable: %v", err)
	}
	pool.Close()
	return base
}

// stripSearchPath removes any search_path query parameter so the admin
// connection lands on the backend default schema for CREATE/DROP SCHEMA.
func stripSearchPath(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	q := u.Query()
	q.Del("search_path")
	u.RawQuery = q.Encode()
	return u.String()
}

// newSchema mints a unique ephemeral schema name and registers a cleanup that
// drops it (CASCADE) via a dedicated admin pool. The schema itself is created by
// Migrate, which issues CREATE SCHEMA IF NOT EXISTS.
func newSchema(t *testing.T, base string) string {
	t.Helper()
	schema := fmt.Sprintf("azync_it_%d_%d", time.Now().UnixNano(), schemaCounter.Add(1))
	admin, err := pgxpool.New(context.Background(), base)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(),
			`DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		admin.Close()
	})
	return schema
}

// harness bundles a migrated Core over an ephemeral schema with the connection
// details tests need for raw SQL and transactional producers.
type harness struct {
	core   *azync.Core
	base   string
	schema string
}

// fastCoreOptions are the suite's shared fast-runtime settings: short leases
// and tight fetch cadences so lease/reap/wake behavior is provable within a
// test's wall clock.
func fastCoreOptions(schema string) []azync.Option {
	return []azync.Option{
		azync.WithSchema(schema),
		azync.WithLogger(discardLogger()),
		azync.WithLeaseTTL(2 * time.Second),
		azync.WithFetchPollInterval(20 * time.Millisecond),
		azync.WithFetchCooldown(5 * time.Millisecond),
		azync.WithIdleBackoffMax(20 * time.Millisecond),
		azync.WithShutdownDrain(time.Second),
	}
}

// newHarness opens a Core against a fresh ephemeral schema, migrates it, and
// registers Close on cleanup. Extra Core options (e.g. a slow poll interval to
// prove LISTEN wakeups, or PollOnly) are layered after the fast defaults.
func newHarness(t *testing.T, coreOpts ...azync.Option) *harness {
	t.Helper()
	base := requireDB(t)
	schema := newSchema(t, base)
	core, err := azync.Open(base, append(fastCoreOptions(schema), coreOpts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = core.Close(context.Background()) })
	require.NoError(t, core.Migrate(context.Background()))
	return &harness{core: core, base: base, schema: schema}
}

// worker is the minimal surface startWorker drives; both queue.Worker and
// event.Worker satisfy it.
type worker interface {
	Start(ctx context.Context) error
}

// startWorker runs a worker in the background and registers a cleanup that
// cancels it and waits for Start to return.
func startWorker(t *testing.T, w worker) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = w.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
}

// newPool builds a caller-owned pgx pool whose connections carry the harness
// schema in their search_path, for the transactional-producer tests that begin
// their own pgx.Tx against the same tables.
func newPool(t *testing.T, base, schema string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(base)
	require.NoError(t, err)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}
