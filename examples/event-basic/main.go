// Command event-basic shows the minimal event bus workflow: register two
// subscribers on the same event type, publish one event with aggregate and
// metadata, and run the worker until interrupted.
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

	// Blank import registers the "postgres"/"postgresql" DSN schemes with
	// azync.Open.
	_ "github.com/kausys/azync/driver/azyncpgx"

	"github.com/kausys/azync"
	"github.com/kausys/azync/event"
)

// defaultDSN matches the repo's compose.yml; override with DATABASE_URL.
//
//nolint:gosec // not a credential leak: matches compose.yml's dev-only DB
const defaultDSN = "postgres://azync:azync@localhost:5432/azync?sslmode=disable"

// userCreated is a typed event; EventType is the wire-stable identity.
type userCreated struct {
	Email string `json:"email"`
}

func (userCreated) EventType() string { return "examples.user.created" }

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

	ev, err := event.New(core)
	if err != nil {
		return fmt.Errorf("new event runtime: %w", err)
	}

	publisher := ev.Publisher()
	const eventType = "examples.user.created"
	for _, name := range []string{"examples.welcome-email", "examples.crm-sync"} {
		sub := event.Subscriber{Name: name, EventType: eventType}
		if err := publisher.Register(ctx, sub); err != nil {
			return fmt.Errorf("register subscriber %s: %w", name, err)
		}
	}

	// One handler reused by both subscribers; the envelope itself carries
	// which subscriber this delivery is for.
	logDelivery := func(_ context.Context, env event.Envelope) error {
		slog.Info("delivery received",
			"subscriber", env.Subscriber, "type", env.Type,
			"aggregate_id", env.AggregateID, "attempt", env.Attempt)
		return nil
	}
	if err := ev.Worker().Subscribe("examples.welcome-email", logDelivery); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	if err := ev.Worker().Subscribe("examples.crm-sync", logDelivery); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	id, err := publisher.Publish(ctx, userCreated{Email: "ada@example.com"},
		event.WithAggregate("user", "usr_123"),
		event.WithMeta("source", "signup-form"))
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	slog.Info("published", "event_id", id)

	slog.Info("worker starting; press ctrl-C to stop")
	if err := ev.Worker().Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("worker: %w", err)
	}
	slog.Info("worker stopped cleanly")
	return nil
}
