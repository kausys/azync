// Command shared-core shows one Core powering both runtimes at once: a queue
// and an event bus sharing a single connection pool and schema, wired
// together by a projector — an event handler that enqueues a job in response.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	// Blank import registers the "postgres"/"postgresql" DSN schemes with
	// azync.Open.
	_ "github.com/kausys/azync/driver/azyncpgx"

	"github.com/kausys/azync"
	"github.com/kausys/azync/event"
	"github.com/kausys/azync/queue"
)

// defaultDSN matches the repo's compose.yml; override with DATABASE_URL.
//
//nolint:gosec // not a credential leak: matches compose.yml's dev-only DB
const defaultDSN = "postgres://azync:azync@localhost:5432/azync?sslmode=disable"

// orderPlaced is the event the projector reacts to.
type orderPlaced struct {
	OrderID string `json:"orderId"`
}

func (orderPlaced) EventType() string { return "examples.order.placed" }

// sendReceipt is the job the projector enqueues in response; its fields
// mirror orderPlaced so the handler can convert directly between them.
type sendReceipt struct {
	OrderID string `json:"orderId"`
}

func (sendReceipt) Kind() string { return "examples.receipt.send" }

const projector = "examples.receipt-projector"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = defaultDSN
	}

	// One Core, one pool, one schema: both runtimes compose over it.
	core, err := azync.Open(dsn)
	if err != nil {
		return fmt.Errorf("open core: %w", err)
	}
	defer func() {
		if err := core.Close(context.Background()); err != nil {
			log.Printf("close core: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Migrate is always explicit; Open never touches the schema.
	if err := core.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	q, err := queue.New(core)
	if err != nil {
		return fmt.Errorf("new queue runtime: %w", err)
	}
	ev, err := event.New(core)
	if err != nil {
		return fmt.Errorf("new event runtime: %w", err)
	}

	err = queue.Register(q.Worker(), func(_ context.Context, job sendReceipt) error {
		slog.Info("sending receipt", "order_id", job.OrderID)
		return nil
	})
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}

	// The projector pattern: an event handler enqueues a job in response,
	// bridging the durable ledger to durable background work. The handler
	// receives the decoded orderPlaced directly — no envelope, no Unmarshal — and
	// RegisterFunc upserts the durable subscription in Start.
	err = event.RegisterFunc(ev.Worker(), projector, func(ctx context.Context, order orderPlaced) error {
		_, err := q.Producer().Enqueue(ctx, sendReceipt(order))
		return err
	})
	if err != nil {
		return fmt.Errorf("register projector: %w", err)
	}

	slog.Info("workers starting; press ctrl-C to stop")
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := q.Worker().Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("queue worker: %v", err)
		}
	})
	wg.Go(func() {
		if err := ev.Worker().Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("event worker: %v", err)
		}
	})

	// Publish only after the projector's subscription exists (Start upserts it
	// before the worker becomes Ready), so the event fans out a delivery to it.
	select {
	case <-ev.Worker().Ready():
		if _, err := ev.Publisher().Publish(ctx, orderPlaced{OrderID: "ord_9001"}); err != nil {
			log.Printf("publish: %v", err)
		}
	case <-ctx.Done():
	}
	wg.Wait()

	qStats, err := q.Manager().AllStats(context.Background())
	if err != nil {
		return fmt.Errorf("queue stats: %w", err)
	}
	evStats, err := ev.Manager().Stats(context.Background())
	if err != nil {
		return fmt.Errorf("event stats: %w", err)
	}
	slog.Info("queue stats", "pending", qStats.Pending, "succeeded", qStats.Succeeded, "dead", qStats.Dead)
	slog.Info("event stats", "events", evStats.Events, "succeeded", evStats.Succeeded, "dead", evStats.Dead)
	return nil
}
