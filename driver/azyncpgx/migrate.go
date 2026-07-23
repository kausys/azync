package azyncpgx

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrate brings the backend schema up to date via goose. Open never migrates.
// When a schema is configured it is created if absent and the migrations run
// there; the version-tracking table is the configured MigrationsTable (default
// azync_migrations).
func (s *Store) Migrate(ctx context.Context) error {
	if s.schema != "" {
		if err := validatePostgresIdentifier(s.schema); err != nil {
			return err
		}
		if _, err := s.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{s.schema}.Sanitize()); err != nil {
			return fmt.Errorf("azyncpgx: create schema: %w", err)
		}
	}

	migrations, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("azyncpgx: load migrations: %w", err)
	}

	db, cleanup, err := s.migrationDB()
	if err != nil {
		return err
	}
	defer cleanup()

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("azyncpgx: configure migration lock: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations,
		goose.WithSessionLocker(locker), goose.WithTableName(s.migrationsTable))
	if err != nil {
		return fmt.Errorf("azyncpgx: configure migrations: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("azyncpgx: migrate: %w", err)
	}
	return nil
}

// migrationDB opens a database/sql handle (goose requires one) whose connections
// carry the configured search_path, so migrations land in the intended schema
// regardless of how the pool was built. The cleanup closes the handle and
// unregisters the transient connection config.
func (s *Store) migrationDB() (*sql.DB, func(), error) {
	cc := s.pool.Config().ConnConfig.Copy()
	if s.schema != "" {
		cc.RuntimeParams["search_path"] = s.schema
	}
	connStr := stdlib.RegisterConnConfig(cc)
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		stdlib.UnregisterConnConfig(connStr)
		return nil, nil, fmt.Errorf("azyncpgx: open migration handle: %w", err)
	}
	cleanup := func() {
		_ = db.Close()
		stdlib.UnregisterConnConfig(connStr)
	}
	return db, cleanup, nil
}
