package azyncpgx

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// advisoryUnlockTimeout bounds the best-effort unlock+close a release performs
// after the caller's context may already be cancelled.
const advisoryUnlockTimeout = time.Second

// AcquireLeadership tries to take the named leadership via a PostgreSQL
// session-scoped advisory lock held on a dedicated connection retained for the
// lease. release is idempotent: it unlocks the advisory lock and closes the
// connection. acquired=false means another instance leads.
func (s *Store) AcquireLeadership(ctx context.Context, name string) (func(), bool, error) {
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
		return func() {}, false, nil
	}

	var once sync.Once
	// The release outlives the caller's context, so a fresh deadline drives the
	// unlock and close.
	//nolint:contextcheck // deliberate: release must run after the caller ctx is cancelled
	release := func() {
		once.Do(func() {
			unlockCtx, cancel := context.WithTimeout(context.Background(), advisoryUnlockTimeout)
			defer cancel()
			_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", key)
			_ = conn.Close(unlockCtx)
		})
	}
	return release, true, nil
}

// advisoryLockKey derives a stable 64-bit advisory-lock key from the leadership
// name and schema so different names and schemas never collide.
func advisoryLockKey(name, schema string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("azync:" + name + ":" + schema))
	//nolint:gosec // advisory locks accept signed 64-bit keys; the wrap is intentional
	return int64(h.Sum64())
}
