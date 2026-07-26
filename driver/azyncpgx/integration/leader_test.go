package integration

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// withApplicationName sets application_name on dsn so a connection opened
// from it is identifiable in pg_stat_activity without guessing at other
// concurrently running tests' connections.
func withApplicationName(t *testing.T, dsn, name string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	q := u.Query()
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	return u.String()
}

// TestLeadershipLeaseDetectsLostSessionAndAllowsReacquire proves
// driver.LeaseElector's Valid check reflects a dead backing session, not
// just a released lock: killing the connection backing a held lease at the
// Postgres level (simulating a network partition, an idle-connection kill by
// an intermediary, or the process dying) must flip Valid to false without
// Release ever being called, and a fresh AcquireLeadershipLease of the same
// name must then succeed, because the session (and the advisory lock that
// died with it) is gone.
func TestLeadershipLeaseDetectsLostSessionAndAllowsReacquire(t *testing.T) {
	is := require.New(t)
	base := requireDB(t)
	schema := newSchema(t, base)
	appName := fmt.Sprintf("azync_leader_test_%s", schema)

	core, err := azync.Open(withApplicationName(t, base, appName),
		azync.WithSchema(schema), azync.WithLogger(discardLogger()))
	is.NoError(err)
	t.Cleanup(func() { _ = core.Close(context.Background()) })
	is.NoError(core.Migrate(context.Background()))

	elector, ok := core.Store().(driver.LeaseElector)
	is.True(ok, "azyncpgx.Store must implement driver.LeaseElector")

	ctx := context.Background()
	lease, acquired, err := elector.AcquireLeadershipLease(ctx, "leader-it-test")
	is.NoError(err)
	is.True(acquired)
	is.True(lease.Valid(ctx), "a freshly acquired lease must be valid")

	// Kill every backend carrying this test's application_name from an
	// independent admin connection. The lease's dedicated connection sits
	// idle (holding the session advisory lock) and is the only one doing
	// meaningful work at this point in the test, so this is equivalent to
	// killing exactly the lease's backend.
	admin, err := pgxpool.New(ctx, base)
	is.NoError(err)
	defer admin.Close()

	tag, err := admin.Exec(ctx, `
		SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE application_name = $1 AND pid <> pg_backend_pid()`, appName)
	is.NoError(err)
	is.Positive(tag.RowsAffected(), "expected to find and terminate the lease's backend")

	is.Eventually(func() bool {
		return !lease.Valid(ctx)
	}, 5*time.Second, 20*time.Millisecond, "Valid did not report the lost session")

	// The dead session's advisory lock is gone with it: a fresh acquire of
	// the same name must now succeed.
	second, acquired, err := elector.AcquireLeadershipLease(ctx, "leader-it-test")
	is.NoError(err)
	is.True(acquired, "a new acquire must succeed once the dead session's lock is gone")
	second.Release()
}
