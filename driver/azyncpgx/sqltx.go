package azyncpgx

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SQLTxStore is the Store whose transactional capabilities take a
// database/sql transaction instead of a pgx.Tx: EnqueueTx, PublishTx and
// CreateDAGTx enlist in the caller's *sql.Tx, so an application that reaches
// PostgreSQL through database/sql — directly, or through a library built on
// it — commits its own writes and the outbox atomically. Every other method is
// the embedded Store's: the worker side still runs on the driver's pgx pool.
//
// Build a Core over it with azync.New(store.SQLTx()); the runtimes' TxProducer,
// TxPublisher and TxRunner then resolve for *sql.Tx. A Core serves one
// transaction type, so it no longer resolves them for pgx.Tx.
//
// The caller's transaction must be open against the database azync's tables
// live in, with the same search_path when WithSchema is used. The enlisted
// statements pass only plain scalar arguments, so any database/sql driver for
// PostgreSQL works; the NOTIFY that wakes workers is sent inside the caller's
// transaction and, like the pgx.Tx path, fires only if it commits.
type SQLTxStore struct {
	*Store
}

// SQLTx returns the view of s whose transactional capabilities take *sql.Tx.
func (s *Store) SQLTx() *SQLTxStore { return &SQLTxStore{Store: s} }

// Interface assertions: the view keeps every capability of the Store it
// embeds, and swaps only the transaction type of the Tx ones.
var (
	_ driver.Store               = (*SQLTxStore)(nil)
	_ driver.Notifier            = (*SQLTxStore)(nil)
	_ driver.ChangeNotifier      = (*SQLTxStore)(nil)
	_ driver.LeaderElector       = (*SQLTxStore)(nil)
	_ driver.LeaseElector        = (*SQLTxStore)(nil)
	_ driver.Migrator            = (*SQLTxStore)(nil)
	_ driver.DAGStore            = (*SQLTxStore)(nil)
	_ driver.WorkflowStore       = (*SQLTxStore)(nil)
	_ driver.TxStore[*sql.Tx]    = (*SQLTxStore)(nil)
	_ driver.TxDAGStore[*sql.Tx] = (*SQLTxStore)(nil)
	_ querier                    = sqlQuerier{}
)

// EnqueueTx performs Enqueue within the caller's database/sql transaction.
func (s *SQLTxStore) EnqueueTx(ctx context.Context, tx *sql.Tx, p driver.EnqueueParams) (bool, error) {
	return s.enqueue(ctx, sqlQuerier{tx: tx}, p, true)
}

// PublishTx performs Publish within the caller's database/sql transaction.
func (s *SQLTxStore) PublishTx(ctx context.Context, tx *sql.Tx, p driver.PublishParams) (int, error) {
	return s.publishInTx(ctx, sqlQuerier{tx: tx}, p)
}

// CreateDAGTx performs CreateDAG within the caller's database/sql transaction.
func (s *SQLTxStore) CreateDAGTx(ctx context.Context, tx *sql.Tx, p driver.DAGParams) (bool, uuid.UUID, error) {
	return s.createDAG(ctx, sqlQuerier{tx: tx}, p)
}

// sqlQuerier presents a *sql.Tx as the querier the producer paths are written
// against, so the enlisted statements are the very ones the pgx.Tx path runs.
// It translates the two things callers branch on: sql.ErrNoRows becomes
// pgx.ErrNoRows, and the command tag carries the rows affected.
type sqlQuerier struct {
	tx *sql.Tx
}

func (q sqlQuerier) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	res, err := q.tx.ExecContext(ctx, query, args...) //nolint:gosec // G701: query is one of the driver's own statements, forwarded unchanged
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		// The statement ran; only the count is unavailable from this driver.
		return pgconn.CommandTag{}, nil //nolint:nilerr // see above
	}
	return commandTag(query, n), nil
}

func (q sqlQuerier) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	rows, err := q.tx.QueryContext(ctx, query, args...) //nolint:gosec,rowserrcheck // the driver's own statement; sqlRows owns the rows and callers check Err through it
	if err != nil {
		return nil, err
	}
	return &sqlRows{rows: rows}, nil
}

func (q sqlQuerier) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	return sqlRow{row: q.tx.QueryRowContext(ctx, query, args...)}
}

// commandTag rebuilds the tag PostgreSQL would have reported, from the
// statement's leading keyword and the rows affected: "INSERT 0 n" for an
// insert, "<VERB> n" otherwise.
func commandTag(query string, rowsAffected int64) pgconn.CommandTag {
	verb, _, _ := strings.Cut(strings.TrimSpace(query), " ")
	verb = strings.ToUpper(strings.TrimSpace(verb))
	count := strconv.FormatInt(rowsAffected, 10)
	if verb == "INSERT" {
		return pgconn.NewCommandTag("INSERT 0 " + count)
	}
	return pgconn.NewCommandTag(verb + " " + count)
}

type sqlRow struct {
	row *sql.Row
}

func (r sqlRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	if errors.Is(err, sql.ErrNoRows) {
		return pgx.ErrNoRows
	}
	return err
}

// sqlRows adapts *sql.Rows to pgx.Rows. The pgx-specific accessors that
// database/sql has no equivalent for (field descriptions, raw wire values,
// the underlying connection) report nothing.
type sqlRows struct {
	rows *sql.Rows
	err  error
}

func (r *sqlRows) Close() {
	if err := r.rows.Close(); err != nil && r.err == nil {
		r.err = err
	}
}

func (r *sqlRows) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.rows.Err()
}

func (r *sqlRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *sqlRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *sqlRows) Next() bool { return r.rows.Next() }

func (r *sqlRows) Scan(dest ...any) error { return r.rows.Scan(dest...) }

func (r *sqlRows) Values() ([]any, error) {
	columns, err := r.rows.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]any, len(columns))
	targets := make([]any, len(columns))
	for i := range values {
		targets[i] = &values[i]
	}
	if err := r.rows.Scan(targets...); err != nil {
		return nil, err
	}
	return values, nil
}

func (r *sqlRows) RawValues() [][]byte { return nil }

func (r *sqlRows) Conn() *pgx.Conn { return nil }
