package azyncpgx

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// register wires the driver into the core registry under the postgres schemes,
// database/sql style, so a blank import is all a caller needs.
//
//nolint:gochecknoinits // driver self-registration is the database/sql blank-import convention
func init() {
	azync.RegisterDriver("postgres", open)
	azync.RegisterDriver("postgresql", open)
}

// pingTimeout bounds the connectivity check the opener performs so a dead DSN
// fails fast instead of hanging the caller.
const pingTimeout = 5 * time.Second

// Store is the PostgreSQL (pgx v5) implementation of driver.Store and its
// optional capabilities. It operates the single azync_jobs table, partitioning
// queue jobs and event deliveries by the source discriminator, and keeps one
// dedicated LISTEN connection for push wakeups.
type Store struct {
	pool     *pgxpool.Pool
	ownsPool bool

	schema          string
	notifyChannel   string
	migrationsTable string
	pollOnly        bool
	logger          *slog.Logger

	listener *listener
}

// Option configures a Store built with New. Options are applied in order.
type Option func(*Store)

// WithNotifyChannel sets the LISTEN/NOTIFY wakeup channel. Empty keeps the
// default (azync, or azync_<schema> when a schema is set).
func WithNotifyChannel(channel string) Option {
	return func(s *Store) { s.notifyChannel = channel }
}

// WithMigrationsTable overrides the goose version-tracking table name (default
// azync_migrations).
func WithMigrationsTable(table string) Option {
	return func(s *Store) { s.migrationsTable = table }
}

// WithSchema records the backend schema azync's tables live in. Migrate creates
// it if absent and runs migrations there; the caller's pool is expected to have
// its search_path pointed at the same schema for runtime queries.
func WithSchema(schema string) Option {
	return func(s *Store) { s.schema = schema }
}

// PollOnly disables push wakeups so Wake reports poll-only and the runtime falls
// back to the always-correct polling path.
func PollOnly() Option {
	return func(s *Store) { s.pollOnly = true }
}

// WithLogger sets the structured logger the driver uses (nil keeps
// slog.Default()).
func WithLogger(logger *slog.Logger) Option {
	return func(s *Store) {
		if logger != nil {
			s.logger = logger
		}
	}
}

// New builds a Store over a pool the caller already owns. The caller is
// responsible for the pool's lifecycle: Close stops the driver's listener but
// does not close a caller-supplied pool. Use the registry (azync.Open with a
// blank import of this package) when azync should own the pool.
func New(pool *pgxpool.Pool, opts ...Option) *Store {
	s := &Store{pool: pool, logger: slog.Default()}
	for _, opt := range opts {
		opt(s)
	}
	s.finishInit()
	return s
}

// open is the registered driver.Opener. It builds a pgxpool from the DSN,
// applies the resolved schema via search_path (identifier validated first), and
// verifies connectivity. It never migrates and never includes the DSN — which
// may carry credentials — in an error.
func open(dsn string, cfg driver.Config) (driver.Store, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		// ParseConfig echoes the DSN in its error, so never wrap it.
		return nil, errors.New("azyncpgx: invalid PostgreSQL connection string")
	}

	if cfg.Schema != "" {
		if err := validatePostgresIdentifier(cfg.Schema); err != nil {
			return nil, err
		}
		// RuntimeParams values are transmitted as protocol parameters, not
		// interpolated SQL, but the identifier is validated above regardless.
		config.ConnConfig.RuntimeParams["search_path"] = cfg.Schema
	}
	if cfg.NotifyChannel != "" {
		if err := validatePostgresChannel(cfg.NotifyChannel); err != nil {
			return nil, err
		}
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, errors.New("azyncpgx: cannot create PostgreSQL connection pool")
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	s := &Store{
		pool:            pool,
		ownsPool:        true,
		schema:          cfg.Schema,
		notifyChannel:   cfg.NotifyChannel,
		migrationsTable: cfg.MigrationsTable,
		pollOnly:        cfg.PollOnly,
		logger:          logger,
	}
	s.finishInit()

	// First round trip: verify the connection resolves. current_schema() may be
	// NULL when the schema has not been created yet — Migrate creates it — so a
	// missing schema is not an Open failure; only a connection error is.
	pingCtx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	var resolved *string
	if err := pool.QueryRow(pingCtx, "SELECT current_schema()").Scan(&resolved); err != nil {
		pool.Close()
		return nil, errors.New("azyncpgx: cannot reach PostgreSQL")
	}
	return s, nil
}

// finishInit resolves defaults and builds the listener. Called by both New and
// open once the configured fields are in place.
func (s *Store) finishInit() {
	if s.migrationsTable == "" {
		s.migrationsTable = "azync_migrations"
	}
	if s.notifyChannel == "" {
		s.notifyChannel = defaultChannel(s.schema)
	}
	s.listener = newListener(s.pool, s.notifyChannel, s.pollOnly, s.logger)
}

// defaultChannel is azync, or azync_<schema> when a schema is set and the result
// is still a valid channel identifier (a very long schema falls back to azync).
func defaultChannel(schema string) string {
	if schema == "" {
		return "azync"
	}
	candidate := "azync_" + schema
	if validatePostgresChannel(candidate) != nil {
		return "azync"
	}
	return candidate
}

// Close stops the listener and closes the pool when the driver owns it. A pool
// passed to New is left for the caller to close.
func (s *Store) Close(_ context.Context) error {
	s.listener.close()
	if s.ownsPool {
		s.pool.Close()
	}
	return nil
}

// querier is the shared read/write surface of both the pool and a pgx.Tx, so the
// producer methods can run either standalone (their own short transaction) or
// enlisted in a caller's transaction (the TxStore capability).
type querier interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Interface assertions: the driver implements the mandatory Store plus every
// optional capability, and TxStore over pgx.Tx (one tx type per driver).
var (
	_ driver.Store           = (*Store)(nil)
	_ driver.Notifier        = (*Store)(nil)
	_ driver.LeaderElector   = (*Store)(nil)
	_ driver.Migrator        = (*Store)(nil)
	_ driver.TxStore[pgx.Tx] = (*Store)(nil)
	_ querier                = (*pgxpool.Pool)(nil)
	_ querier                = (pgx.Tx)(nil)
)
