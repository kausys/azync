// Command outbox shows writes committed in the application's own tables — here
// a "shop" schema reached through database/sql — reaching azync, which the
// application does not share a transaction with. Each order and its event
// commit together into the shop's outbox; after the commit a coalesced signal
// asks a worker to drain the outbox, and the drain hands what committed to
// azync, where the event fans out as if it had been published directly. A
// rolled-back order leaves nothing behind. It runs until both committed
// orders are delivered, then exits.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	// Registers the "pgx" database/sql driver the shop connects through.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver/azyncpgx"
	"github.com/kausys/azync/event"
	"github.com/kausys/azync/queue"

	"github.com/google/uuid"
)

// defaultDSN matches the repo's compose.yml; override with DATABASE_URL.
//
//nolint:gosec // not a credential leak: matches compose.yml's dev-only DB
const defaultDSN = "postgres://azync:azync@localhost:5433/azync?sslmode=disable"

// orderPlaced is published in the same transaction that writes the order.
type orderPlaced struct {
	OrderID uuid.UUID `json:"orderId"`
	Total   int       `json:"total"`
}

func (orderPlaced) EventType() string { return "examples.shop.order.placed" }

// drainShop is the signal a commit sends: "the shop's outbox has news". Many
// signals coalesce into one pending drain, and one arriving mid-drain is kept.
type drainShop struct{}

func (drainShop) Kind() string { return "examples.shop.drain" }

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = defaultDSN
	}

	// azync's side.
	core, err := azync.Open(dsn)
	if err != nil {
		return fmt.Errorf("open core: %w", err)
	}
	defer func() { _ = core.Close(context.Background()) }()
	if err := core.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate azync: %w", err)
	}

	// The application's side: its own schema, its own connection, the outbox
	// beside its tables.
	shop, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open shop: %w", err)
	}
	defer func() { _ = shop.Close() }()
	if _, err := shop.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS shop;
		CREATE TABLE IF NOT EXISTS shop.orders (id uuid PRIMARY KEY, total int NOT NULL)`); err != nil {
		return fmt.Errorf("create shop tables: %w", err)
	}
	outbox, err := azyncpgx.NewOutbox(azyncpgx.WithOutboxSchema("shop"))
	if err != nil {
		return err
	}
	if err := outbox.Migrate(ctx, shop); err != nil {
		return err
	}

	events, err := event.New(core)
	if err != nil {
		return err
	}
	jobs, err := queue.New(core, queue.WithCron(false))
	if err != nil {
		return err
	}
	delivered := make(chan orderPlaced, 4)
	if err := events.Worker().RegisterFunc("examples.shop.receipts", func(_ context.Context, o orderPlaced) error {
		slog.Info("receipt sent", "order_id", o.OrderID, "total", o.Total)
		delivered <- o
		return nil
	}); err != nil {
		return err
	}
	// Two drains can overlap: a signal that arrives once a drain has started is
	// kept, and its drain may begin before the first one ends. They split the
	// rows between them (SKIP LOCKED), so one may forward nothing — redundant
	// work, never a row handed over twice.
	if err := jobs.Worker().Register(func(ctx context.Context, _ drainShop) error {
		for {
			res, err := outbox.Drain(ctx, shop, core.Store(), 100)
			if err != nil {
				return err // retried with backoff; the rows wait in the outbox
			}
			slog.Info("outbox drained", "forwarded", res.Forwarded, "quarantined", res.Quarantined)
			if res.Forwarded+res.Quarantined < 100 {
				return nil
			}
		}
	}); err != nil {
		return err
	}
	// Stop the workers and let them settle what they hold before the Core
	// closes: a worker still acking a job against a closed pool leaves it
	// active until the reaper finds it.
	workers, stop := context.WithCancel(ctx)
	var running sync.WaitGroup
	running.Go(func() { _ = events.Worker().Start(workers) })
	running.Go(func() { _ = jobs.Worker().Start(workers) })
	defer func() {
		stop()
		running.Wait()
	}()
	<-events.Worker().Ready()

	// The capture side: the event runtime builds the event, the outbox keeps it
	// in whatever transaction it is handed.
	publisher, err := events.TxPublisherVia(outbox.SQLTx())
	if err != nil {
		return err
	}
	placeOrder := func(total int, commit bool) (uuid.UUID, error) {
		id := uuid.New()
		tx, err := shop.BeginTx(ctx, nil)
		if err != nil {
			return id, err
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, `INSERT INTO shop.orders (id, total) VALUES ($1, $2)`, id, total); err != nil {
			return id, err
		}
		if _, err := publisher.PublishTx(ctx, tx, orderPlaced{OrderID: id, Total: total}); err != nil {
			return id, err
		}
		if !commit {
			return id, nil // rolled back by the defer: no order, no event
		}
		if err := tx.Commit(); err != nil {
			return id, err
		}
		// After the commit, and only then: ask for a drain. Losing this signal
		// delays the event until the next one; it never loses it.
		_, err = jobs.Producer().Enqueue(ctx, drainShop{}, queue.CoalesceKey("shop"))
		return id, err
	}

	abandoned, err := placeOrder(10, false)
	if err != nil {
		return err
	}
	var placed []uuid.UUID
	for _, total := range []int{20, 30} {
		id, err := placeOrder(total, true)
		if err != nil {
			return err
		}
		placed = append(placed, id)
	}

	seen := map[uuid.UUID]bool{}
	for len(seen) < len(placed) {
		select {
		case o := <-delivered:
			if o.OrderID == abandoned {
				return errors.New("a rolled-back order was delivered")
			}
			seen[o.OrderID] = true
		case <-ctx.Done():
			return fmt.Errorf("waiting for deliveries: %w", ctx.Err())
		}
	}
	slog.Info("both committed orders delivered; the rolled-back one never existed")
	return nil
}
