package azyncpgx

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// Outbox keeps writes committed in a database the runtime's store does not
// operate, and forwards them to that store later. An application whose own
// tables live in one database — or in many, one per tenant — while azync's
// live in another cannot commit a business write and an enqueue, publish or
// DAG creation in one transaction. With an outbox beside its tables it can:
//
//   - Migrate creates the outbox table in a database, with its own history.
//   - TxStore and SQLTx capture a write in the caller's transaction on that
//     database, through the runtimes' TxPublisherVia, TxProducerVia and
//     TxRunnerVia. What is stored is the params the runtime built — id,
//     payload, metadata, trace context, the compiled DAG — so nothing is
//     rebuilt when it is forwarded.
//   - Drain hands what committed to the runtime's store and removes it.
//
// An Outbox holds configuration only, never a connection: the same value
// serves every database it is migrated into, and each call names the
// database or transaction it acts on.
//
// Forwarding is at-least-once and never duplicates: the target store refuses
// an id it already has (driver.ErrAlreadyExists) and Drain treats that as
// done, so a crash between forwarding a row and removing it costs one retry,
// not a second event. The target's retention bounds that guarantee: a row
// held back longer than the target keeps what it was forwarded as could be
// forwarded again.
type Outbox struct {
	table         string // for SQL: sanitized and schema-qualified when a schema is set
	historyTable  string // for goose: the unquoted form it parses
	insertSQL     string
	selectSQL     string
	deleteSQL     string
	quarantineSQL string
	createSchema  string // empty when no schema is set
}

// OutboxOption configures an Outbox built with NewOutbox.
type OutboxOption func(*outboxConfig)

type outboxConfig struct {
	schema string
}

// WithOutboxSchema places the outbox table and its history in schema, which
// Migrate creates if absent, and qualifies every statement with it, so the
// caller's search_path does not matter. It must be a lowercase unquoted
// identifier. Empty (the default) leaves both on the search_path.
func WithOutboxSchema(schema string) OutboxOption {
	return func(c *outboxConfig) { c.schema = schema }
}

const (
	outboxTableName   = "azync_outbox"
	outboxHistoryName = "azync_outbox_migrations"

	// outboxFormat versions how params are encoded in a row. Drain refuses a
	// row whose format it does not know rather than guessing at it.
	outboxFormat = 1

	// outboxLockID is the advisory lock outbox migrations hold, distinct from
	// the store's own, so migrating both in one database never contends.
	outboxLockID int64 = 0x617a796e635f6f62 // "azync_ob"

	// defaultDrainLimit is the batch Drain reads when the caller names none.
	defaultDrainLimit = 100
)

// The kinds of write an outbox row holds, one per transactional capability.
const (
	outboxKindEvent = "event"
	outboxKindJob   = "job"
	outboxKindDAG   = "dag"
)

// NewOutbox builds an outbox. It fails on a schema that is not a lowercase
// unquoted PostgreSQL identifier: the statements qualify tables with it, and
// the migration history names it unquoted, so only a name whose quoted and
// unquoted forms agree is safe for both.
func NewOutbox(opts ...OutboxOption) (*Outbox, error) {
	var cfg outboxConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	o := &Outbox{table: outboxTableName, historyTable: outboxHistoryName}
	if cfg.schema != "" {
		if !isPostgresIdentifier(cfg.schema) || cfg.schema != strings.ToLower(cfg.schema) {
			return nil, fmt.Errorf("azyncpgx: outbox schema %q is not a lowercase identifier", cfg.schema)
		}
		o.table = pgx.Identifier{cfg.schema, outboxTableName}.Sanitize()
		o.historyTable = cfg.schema + "." + outboxHistoryName
		o.createSchema = "CREATE SCHEMA IF NOT EXISTS " + pgx.Identifier{cfg.schema}.Sanitize()
	}
	o.insertSQL = `INSERT INTO ` + o.table + ` (id, kind, topic, format, params) VALUES ($1, $2, $3, $4, $5::jsonb)`
	o.selectSQL = `SELECT seq, id, kind, format, params FROM ` + o.table + `
		WHERE quarantined_at IS NULL ORDER BY seq LIMIT $1 FOR UPDATE SKIP LOCKED`
	o.deleteSQL = `DELETE FROM ` + o.table + ` WHERE seq = $1`
	o.quarantineSQL = `UPDATE ` + o.table + ` SET quarantined_at = now(), last_error = $2 WHERE seq = $1`
	return o, nil
}

// outboxMigrations renders the outbox's schema history. Released versions are
// frozen, exactly like the store's own migrations: a change is a new version.
func (o *Outbox) outboxMigrations() []*goose.Migration {
	v1 := `
CREATE TABLE IF NOT EXISTS ` + o.table + ` (
    seq            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id             uuid        NOT NULL UNIQUE,
    kind           text        NOT NULL CHECK (kind IN ('event', 'job', 'dag')),
    topic          text        NOT NULL,
    format         smallint    NOT NULL,
    params         jsonb       NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    quarantined_at timestamptz NULL,
    last_error     text        NULL
);
CREATE INDEX IF NOT EXISTS azync_outbox_live_idx ON ` + o.table + ` (seq) WHERE quarantined_at IS NULL;`
	exec := func(statement string) *goose.GoFunc {
		return &goose.GoFunc{RunTx: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, statement)
			return err
		}}
	}
	return []*goose.Migration{
		goose.NewGoMigration(1, exec(v1), exec(`DROP TABLE IF EXISTS `+o.table)),
	}
}

// Migrate brings the outbox schema in db up to date, through its own history
// table (azync_outbox_migrations) and its own advisory lock, so it is safe to
// run concurrently and beside the store's Migrate. When a schema is set it is
// created if absent.
func (o *Outbox) Migrate(ctx context.Context, db *sql.DB) error {
	if o.createSchema != "" {
		if _, err := db.ExecContext(ctx, o.createSchema); err != nil {
			return fmt.Errorf("azyncpgx: outbox create schema: %w", err)
		}
	}
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockID(outboxLockID))
	if err != nil {
		return fmt.Errorf("azyncpgx: outbox migration lock: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, nil,
		goose.WithGoMigrations(o.outboxMigrations()...),
		goose.WithDisableGlobalRegistry(true),
		goose.WithSessionLocker(locker),
		goose.WithTableName(o.historyTable))
	if err != nil {
		return fmt.Errorf("azyncpgx: outbox migrations: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("azyncpgx: outbox migrate: %w", err)
	}
	return nil
}

// TxStore returns the capture side for pgx transactions: a
// driver.TxStore[pgx.Tx] and driver.TxDAGStore[pgx.Tx] that write into the
// outbox of the database the transaction runs on. Hand it to a runtime's
// TxPublisherVia, TxProducerVia or TxRunnerVia.
func (o *Outbox) TxStore() *OutboxTxStore { return &OutboxTxStore{outbox: o} }

// SQLTx returns the capture side for database/sql transactions, as TxStore
// does for pgx ones.
func (o *Outbox) SQLTx() *OutboxSQLTxStore { return &OutboxSQLTxStore{outbox: o} }

// OutboxTxStore captures writes into the outbox inside a pgx transaction.
//
// Its results describe the capture, not the eventual write: PublishTx reports
// no deliveries and EnqueueTx and CreateDAGTx report an insert, because fan-out
// and deduplication happen when the row is forwarded, against the target
// store's state at that moment.
type OutboxTxStore struct{ outbox *Outbox }

// OutboxSQLTxStore is OutboxTxStore for database/sql transactions.
type OutboxSQLTxStore struct{ outbox *Outbox }

var (
	_ driver.TxStore[pgx.Tx]     = (*OutboxTxStore)(nil)
	_ driver.TxDAGStore[pgx.Tx]  = (*OutboxTxStore)(nil)
	_ driver.TxStore[*sql.Tx]    = (*OutboxSQLTxStore)(nil)
	_ driver.TxDAGStore[*sql.Tx] = (*OutboxSQLTxStore)(nil)
)

// PublishTx captures an event in tx's outbox.
func (s *OutboxTxStore) PublishTx(ctx context.Context, tx pgx.Tx, p driver.PublishParams) (int, error) {
	return 0, s.outbox.capture(ctx, tx, outboxKindEvent, p.ID, p.Type, p)
}

// EnqueueTx captures a job in tx's outbox.
func (s *OutboxTxStore) EnqueueTx(ctx context.Context, tx pgx.Tx, p driver.EnqueueParams) (bool, error) {
	return true, s.outbox.capture(ctx, tx, outboxKindJob, p.ID, p.Kind, p)
}

// CreateDAGTx captures a DAG in tx's outbox.
func (s *OutboxTxStore) CreateDAGTx(ctx context.Context, tx pgx.Tx, p driver.DAGParams) (bool, uuid.UUID, error) {
	return true, uuid.Nil, s.outbox.capture(ctx, tx, outboxKindDAG, p.ID, p.Name, p)
}

// PublishTx captures an event in tx's outbox.
func (s *OutboxSQLTxStore) PublishTx(ctx context.Context, tx *sql.Tx, p driver.PublishParams) (int, error) {
	return 0, s.outbox.capture(ctx, sqlQuerier{tx: tx}, outboxKindEvent, p.ID, p.Type, p)
}

// EnqueueTx captures a job in tx's outbox.
func (s *OutboxSQLTxStore) EnqueueTx(ctx context.Context, tx *sql.Tx, p driver.EnqueueParams) (bool, error) {
	return true, s.outbox.capture(ctx, sqlQuerier{tx: tx}, outboxKindJob, p.ID, p.Kind, p)
}

// CreateDAGTx captures a DAG in tx's outbox.
func (s *OutboxSQLTxStore) CreateDAGTx(ctx context.Context, tx *sql.Tx, p driver.DAGParams) (bool, uuid.UUID, error) {
	return true, uuid.Nil, s.outbox.capture(ctx, sqlQuerier{tx: tx}, outboxKindDAG, p.ID, p.Name, p)
}

func (o *Outbox) capture(ctx context.Context, q querier, kind string, id uuid.UUID, topic string, params any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("azyncpgx: outbox encode %s %s: %w", kind, id, err)
	}
	if _, err := q.Exec(ctx, o.insertSQL, id, kind, topic, outboxFormat, string(raw)); err != nil {
		return fmt.Errorf("azyncpgx: outbox capture %s %s: %w", kind, id, alreadyExists(err, outboxTableName+"_id_key"))
	}
	return nil
}

// DrainResult reports what one Drain did.
type DrainResult struct {
	// Forwarded counts rows handed to the target and removed, including rows
	// the target already had from an earlier, interrupted drain.
	Forwarded int
	// Quarantined counts rows set aside because they can never be forwarded:
	// their params do not decode, or their format or kind is unknown. They stay
	// in the table with quarantined_at and last_error set, out of every later
	// drain, until someone clears quarantined_at.
	Quarantined int
}

// Drain forwards up to limit committed rows of db's outbox to the target store,
// oldest first, and removes each one it forwarded, in one transaction on db.
// A limit of zero or less reads defaultDrainLimit rows. It returns once the
// batch is done; a caller empties the outbox by draining until Forwarded and
// Quarantined add up to less than the limit.
//
// Rows are claimed with SKIP LOCKED, so any number of drains can run against
// one database at once without handing a row over twice. A row the target
// already has counts as forwarded. A row that can never be forwarded is
// quarantined and the batch goes on. Any other failure — the target
// unreachable, say — stops the batch: the rows forwarded before it are still
// removed, the rest wait for the next drain, and the error is returned.
func (o *Outbox) Drain(ctx context.Context, db *sql.DB, target driver.Store, limit int) (DrainResult, error) {
	if limit <= 0 {
		limit = defaultDrainLimit
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return DrainResult{}, fmt.Errorf("azyncpgx: outbox drain begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	batch, err := o.claim(ctx, tx, limit)
	if err != nil {
		return DrainResult{}, err
	}

	var result DrainResult
	var stopped error
	for _, row := range batch {
		err := o.forward(ctx, target, row)
		var undecodable *undecodableRowError
		switch {
		case err == nil, errors.Is(err, driver.ErrAlreadyExists):
			if _, err := tx.ExecContext(ctx, o.deleteSQL, row.seq); err != nil {
				return DrainResult{}, fmt.Errorf("azyncpgx: outbox remove %s: %w", row.id, err)
			}
			result.Forwarded++
		case errors.As(err, &undecodable):
			if _, err := tx.ExecContext(ctx, o.quarantineSQL, row.seq, undecodable.Error()); err != nil {
				return DrainResult{}, fmt.Errorf("azyncpgx: outbox quarantine %s: %w", row.id, err)
			}
			result.Quarantined++
		default:
			stopped = fmt.Errorf("azyncpgx: outbox forward %s %s: %w", row.kind, row.id, err)
		}
		if stopped != nil {
			break
		}
	}
	if err := tx.Commit(); err != nil {
		return DrainResult{}, fmt.Errorf("azyncpgx: outbox drain commit: %w", err)
	}
	return result, stopped
}

type outboxRow struct {
	seq    int64
	id     uuid.UUID
	kind   string
	format int
	params []byte
}

func (o *Outbox) claim(ctx context.Context, tx *sql.Tx, limit int) ([]outboxRow, error) {
	rows, err := tx.QueryContext(ctx, o.selectSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: outbox claim: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var batch []outboxRow
	for rows.Next() {
		var row outboxRow
		if err := rows.Scan(&row.seq, &row.id, &row.kind, &row.format, &row.params); err != nil {
			return nil, fmt.Errorf("azyncpgx: outbox claim scan: %w", err)
		}
		batch = append(batch, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: outbox claim: %w", err)
	}
	return batch, nil
}

// undecodableRowError marks a row no retry will ever forward.
type undecodableRowError struct{ reason string }

func (e *undecodableRowError) Error() string { return e.reason }

func (o *Outbox) forward(ctx context.Context, target driver.Store, row outboxRow) error {
	if row.format != outboxFormat {
		return &undecodableRowError{reason: fmt.Sprintf("unknown outbox format %d", row.format)}
	}
	decode := func(into any) error {
		if err := json.Unmarshal(row.params, into); err != nil {
			return &undecodableRowError{reason: "decode " + row.kind + " params: " + err.Error()}
		}
		return nil
	}
	switch row.kind {
	case outboxKindEvent:
		var p driver.PublishParams
		if err := decode(&p); err != nil {
			return err
		}
		_, err := target.Publish(ctx, p)
		return err
	case outboxKindJob:
		var p driver.EnqueueParams
		if err := decode(&p); err != nil {
			return err
		}
		_, err := target.Enqueue(ctx, p) // not inserted = deduplicated by a key: forwarded all the same
		return err
	case outboxKindDAG:
		dags, ok := target.(driver.DAGStore)
		if !ok {
			return fmt.Errorf("target store %T cannot create DAGs: %w", target, driver.ErrNotSupported)
		}
		var p driver.DAGParams
		if err := decode(&p); err != nil {
			return err
		}
		_, _, err := dags.CreateDAG(ctx, p)
		return err
	default:
		return &undecodableRowError{reason: fmt.Sprintf("unknown outbox kind %q", row.kind)}
	}
}
