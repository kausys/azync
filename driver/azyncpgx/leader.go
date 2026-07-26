package azyncpgx

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/jackc/pgx/v5"
)

// advisoryUnlockTimeout bounds the best-effort unlock+close a release performs
// after the caller's context may already be cancelled.
const advisoryUnlockTimeout = time.Second

// AcquireLeadership tries to take the named leadership via a PostgreSQL
// session-scoped advisory lock held on a dedicated connection retained for the
// lease. release is idempotent: it unlocks the advisory lock and closes the
// connection. acquired=false means another instance leads.
//
// AcquireLeadership cannot detect a lost leadership (the session dying frees
// the lock server-side with no signal to the caller); prefer
// AcquireLeadershipLease, which exposes a Valid check for exactly that.
func (s *Store) AcquireLeadership(ctx context.Context, name string) (func(), bool, error) {
	lease, acquired, err := s.AcquireLeadershipLease(ctx, name)
	if err != nil || !acquired {
		return func() {}, acquired, err
	}
	return lease.Release, true, nil
}

// AcquireLeadershipLease tries to take the named leadership via a PostgreSQL
// session-scoped advisory lock held on a dedicated connection retained for the
// lease. acquired=false means another instance leads.
func (s *Store) AcquireLeadershipLease(ctx context.Context, name string) (driver.LeadershipLease, bool, error) {
	key := advisoryLockKey(name, s.schema)
	conn, err := pgx.ConnectConfig(ctx, s.pool.Config().ConnConfig.Copy())
	if err != nil {
		return nil, false, fmt.Errorf("azyncpgx: leadership connect: %w", err)
	}

	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil {
		_ = conn.Close(ctx)
		return nil, false, fmt.Errorf("azyncpgx: acquire leadership: %w", err)
	}
	if !acquired {
		_ = conn.Close(ctx)
		return nil, false, nil
	}

	return &pgLease{conn: conn, key: key}, true, nil
}

// pgLease is a held session-scoped advisory lock. The session advisory lock
// lives exactly as long as the backing connection's session, so pinging that
// same connection is a correct, cheap proxy for "is this leadership still
// held" — no second query against the lock itself is needed or would be
// meaningful from the same session.
type pgLease struct {
	conn *pgx.Conn
	key  int64

	once sync.Once
}

// Valid reports whether the connection backing this lease is still alive.
func (l *pgLease) Valid(ctx context.Context) bool {
	return l.conn.Ping(ctx) == nil
}

// Release is idempotent: it unlocks the advisory lock and closes the
// connection. It outlives the caller's context, so a fresh deadline drives
// the unlock and close.
//
//nolint:contextcheck // deliberate: release must run after the caller ctx is cancelled
func (l *pgLease) Release() {
	l.once.Do(func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), advisoryUnlockTimeout)
		defer cancel()
		_, _ = l.conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", l.key)
		_ = l.conn.Close(unlockCtx)
	})
}

// advisoryLockKey derives a stable 64-bit advisory-lock key from the leadership
// name and schema so different names and schemas never collide.
func advisoryLockKey(name, schema string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("azync:" + name + ":" + schema))
	//nolint:gosec // advisory locks accept signed 64-bit keys; the wrap is intentional
	return int64(h.Sum64())
}
