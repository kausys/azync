// Command event-basic shows the minimal event bus workflow: register two
// subscribers on the same event type — one via the Subscriber interface plus an
// On binding, one via the RegisterFunc shorthand — publish one event with
// aggregate and metadata, and run the worker until interrupted. Handlers receive
// the decoded domain event; delivery metadata is read from the context.
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

// welcomeEmailer is a durable event consumer. Implementing the Subscriber
// interface names it; the On bindings passed to Register declare which typed
// events it consumes.
type welcomeEmailer struct{}

func (welcomeEmailer) SubscriberName() string { return "examples.welcome-email" }

// sendWelcome consumes the decoded event; the delivery metadata (which
// subscriber, which attempt, whether this is a retry) comes from the context.
func sendWelcome(ctx context.Context, evt userCreated) error {
	slog.Info("welcome email",
		"to", evt.Email,
		"subscriber", event.SubscriberName(ctx),
		"attempt", event.Attempt(ctx),
		"retry", event.IsRetry(ctx))
	return nil
}

// syncToCRM is a plain handler registered with RegisterFunc under an explicit
// name — the shorthand for a single-type subscriber.
func syncToCRM(ctx context.Context, evt userCreated) error {
	slog.Info("crm sync", "email", evt.Email, "subscriber", event.SubscriberName(ctx))
	return nil
}

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

	// Register the subscribers. Their durable subscriptions are upserted by Start
	// (before the worker becomes Ready), so no manual Publisher.Register is
	// needed here.
	if err := ev.Worker().Register(welcomeEmailer{}, event.On(sendWelcome)); err != nil {
		return fmt.Errorf("register welcome-email: %w", err)
	}
	if err := event.RegisterFunc(ev.Worker(), "examples.crm-sync", syncToCRM); err != nil {
		return fmt.Errorf("register crm-sync: %w", err)
	}

	// Run the worker in the background so this goroutine can publish once its
	// subscriptions exist.
	workerErr := make(chan error, 1)
	go func() {
		err := ev.Worker().Start(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			workerErr <- err
			return
		}
		workerErr <- nil
	}()

	// Ready closes after Start has upserted the subscriptions, so a publish now
	// fans out a delivery to each of them.
	select {
	case <-ev.Worker().Ready():
	case err := <-workerErr:
		return fmt.Errorf("worker failed to start: %w", err)
	}

	id, err := ev.Publisher().Publish(ctx, userCreated{Email: "ada@example.com"},
		event.WithAggregate("user", "usr_123"),
		event.WithMeta("source", "signup-form"))
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	slog.Info("published", "event_id", id)

	slog.Info("worker running; press ctrl-C to stop")
	if err := <-workerErr; err != nil {
		return fmt.Errorf("worker: %w", err)
	}
	slog.Info("worker stopped cleanly")
	return nil
}
