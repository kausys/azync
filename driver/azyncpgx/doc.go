// Package azyncpgx is the PostgreSQL (pgx v5) driver for azync: a
// [driver.Store] implementation over one unified azync_jobs table, plus its
// optional capabilities — driver.Notifier and driver.ChangeNotifier via
// LISTEN/NOTIFY, driver.LeaderElector via advisory locks, driver.Migrator via
// goose migrations, and driver.TxStore[pgx.Tx] for transactional
// enqueue/publish.
//
// Import it blank to register the "postgres" and "postgresql" DSN schemes with
// azync.Open:
//
//	import _ "github.com/kausys/azync/driver/azyncpgx"
//
// Call New directly instead when azync should operate a *pgxpool.Pool the
// caller already owns and manages the lifecycle of. WithSchema isolates
// azync's tables in a named schema (Migrate creates it if absent). A single
// dedicated LISTEN connection serves push wakeups for every fetch loop; a
// second one, opened lazily on the first Changes call, streams the row-change
// hints migration 00011's triggers emit on the fixed azync_changes channel
// (schema-filtered via the payload). PollOnly disables both in favor of the
// always-correct polling path.
package azyncpgx
