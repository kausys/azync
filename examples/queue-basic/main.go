// Command queue-basic shows the minimal queue workflow: open a Core, compose a
// queue runtime, register a typed handler, enqueue a few jobs and run the
// worker until interrupted.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Blank import registers the "postgres"/"postgresql" DSN schemes with
	// azync.Open.
	_ "github.com/kausys/azync/driver/azyncpgx"

	"github.com/kausys/azync"
	"github.com/kausys/azync/queue"
)

// defaultDSN matches the repo's compose.yml; override with DATABASE_URL.
//
//nolint:gosec // not a credential leak: matches compose.yml's dev-only DB
const defaultDSN = "postgres://azync:azync@localhost:5432/azync?sslmode=disable"

// emailJob is a typed job argument: Kind is the wire-stable identity,
// decoupled from this Go type's package path.
type emailJob struct {
	To string `json:"to"`
}

func (emailJob) Kind() string { return "examples.email.send" }

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

	err = queue.Register(q.Worker(), func(_ context.Context, job queue.Job[emailJob]) error {
		slog.Info("sending email", "to", job.Args.To, "attempt", job.Attempt)
		return nil
	})
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}

	producer := q.Producer()
	if _, err := producer.Enqueue(ctx, emailJob{To: "welcome@example.com"}); err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	if _, err := producer.Enqueue(ctx, emailJob{To: "digest@example.com"}, queue.Delay(10*time.Second)); err != nil {
		return fmt.Errorf("enqueue delayed: %w", err)
	}
	if _, err := producer.Enqueue(ctx, emailJob{To: "receipt@example.com"}, queue.IdempotencyKey("receipt-42")); err != nil {
		return fmt.Errorf("enqueue idempotent: %w", err)
	}

	slog.Info("worker starting; press ctrl-C to stop")
	if err := q.Worker().Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("worker: %w", err)
	}
	slog.Info("worker stopped cleanly")
	return nil
}
