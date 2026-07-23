package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
)

// Worker leases and executes event deliveries on the shared engine. Subscribers
// register via Register or RegisterFunc before Start; Start upserts each
// subscription durably and builds one engine job kind per subscriber, so a
// subscriber name maps to exactly one fetch partition even when it consumes
// several event types (the adapter routes each delivery to the binding for its
// type). A subscriber without a live registration is simply never fetched — the
// "missing handler" case disappears by design.
type Worker struct {
	engine *engine.Engine
	cfg    config
	store  driver.Store
	logger *slog.Logger

	mu         sync.Mutex
	started    bool
	registered bool
	subs       map[string]subRegistration
}

// subRegistration is one subscriber's in-memory registration: its name, its
// (optional) pinned retry budget, and its event-type -> Binding routing table.
type subRegistration struct {
	name        string
	maxAttempts int // 0 means "use the runtime default"
	bindings    map[string]Binding
}

// Register binds a subscriber to one or more typed event handlers. Each Binding
// (built with On) contributes the event type it consumes; the subscriber becomes
// one engine kind that routes each delivery to the binding matching the event's
// type. It fails on an empty subscriber name, no bindings, two bindings for the
// same event type, a duplicate subscriber, or a call after Start.
//
// The subscriber's retry budget is the value it reports through the optional
// interface{ MaxAttempts() int }; without it the runtime's DefaultMaxAttempts
// applies (floored at 1).
//
// Durability caveat: the (name, event type) subscriptions are upserted in Start,
// so a subscription is born on the first Start — events published before it
// created no deliveries for this subscriber. Adding bindings across restarts
// adds subscriptions but never removes stale ones; deleting a subscription is a
// deliberate administrative act, not a side effect of dropping a binding.
func (w *Worker) Register(s Subscriber, bindings ...Binding) error {
	if s == nil {
		return errors.New("event: subscriber is required")
	}
	maxAttempts := 0
	if ma, ok := s.(maxAttempter); ok {
		maxAttempts = ma.MaxAttempts()
	}
	return w.register(s.SubscriberName(), maxAttempts, bindings...)
}

// register is the shared registration path for Register and RegisterFunc.
func (w *Worker) register(name string, maxAttempts int, bindings ...Binding) error {
	if name == "" {
		return errors.New("event: subscriber name is required")
	}
	if len(bindings) == 0 {
		return fmt.Errorf("event: subscriber %q requires at least one binding", name)
	}
	routing := make(map[string]Binding, len(bindings))
	for _, b := range bindings {
		if b.eventType == "" || b.invoke == nil {
			return fmt.Errorf("event: subscriber %q has an invalid binding (build it with On)", name)
		}
		if _, dup := routing[b.eventType]; dup {
			return fmt.Errorf("event: subscriber %q has two bindings for event type %q", name, b.eventType)
		}
		routing[b.eventType] = b
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		return errors.New("event: cannot register after Start")
	}
	if _, exists := w.subs[name]; exists {
		return fmt.Errorf("event: subscriber %q already registered", name)
	}
	w.subs[name] = subRegistration{name: name, maxAttempts: maxAttempts, bindings: routing}
	return nil
}

// resolveMaxAttempts applies the runtime default to a subscriber that did not
// pin its own budget, and floors the result at 1.
func (w *Worker) resolveMaxAttempts(n int) int {
	if n <= 0 {
		n = w.cfg.DefaultMaxAttempts
	}
	if n < 1 {
		n = 1
	}
	return n
}

// Ready closes after wakeup setup succeeds and the polling loops are running.
// Polling-only workers become ready immediately after Start.
func (w *Worker) Ready() <-chan struct{} { return w.engine.Ready() }

// Start upserts every subscriber's durable subscriptions, registers one engine
// kind per subscriber, and runs the engine (fetch, execute, settle, maintenance)
// until ctx is cancelled. The durable upsert runs before the engine starts, so
// once Ready closes every subscription exists and future publishes fan out to
// it. On cancellation in-flight handlers drain for up to the shutdown drain
// budget. Start fails if called twice; a Start that failed during setup (before
// the engine ran) may be retried.
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return errors.New("event: worker already started")
	}
	w.started = true
	registered := w.registered
	subs := maps.Clone(w.subs)
	w.mu.Unlock()

	// Durable, idempotent upsert of every (subscriber, event type) before the
	// engine runs. Re-running it on a retried Start is harmless.
	for _, sub := range subs {
		maxAttempts := w.resolveMaxAttempts(sub.maxAttempts)
		for eventType := range sub.bindings {
			if err := w.store.RegisterSubscriber(ctx, driver.Subscriber{
				Name:        sub.name,
				EventType:   eventType,
				MaxAttempts: maxAttempts,
			}); err != nil {
				w.reset()
				return fmt.Errorf("event: register subscriber %q: %w", sub.name, err)
			}
		}
	}

	// Kinds survive a failed setup on the engine, so register them exactly once
	// even across Start retries.
	if !registered {
		for _, sub := range subs {
			if err := w.engine.Register(engine.Kind{
				Name:        sub.name,
				Concurrency: w.cfg.DefaultConcurrency,
				Timeout:     w.cfg.handlerTimeout,
				Classify:    classify,
				Handler:     w.deliver(sub),
			}); err != nil {
				w.reset()
				return fmt.Errorf("event: %w", err)
			}
		}
		w.mu.Lock()
		w.registered = true
		w.mu.Unlock()
	}

	err := w.engine.Start(ctx)
	if err != nil && !w.engine.Started() {
		// The engine failed during setup and reset itself; allow a retry here too.
		w.reset()
	}
	return err
}

// reset clears the started flag after a setup failure so Start can be retried.
func (w *Worker) reset() {
	w.mu.Lock()
	w.started = false
	w.mu.Unlock()
}

// deliver adapts a subscriber's bindings to the engine's driver.Job handler: it
// rebuilds the Delivery metadata onto ctx, routes to the binding for the event's
// type, and lets the binding decode the ledger payload into the handler's type.
// A delivery whose ledger record is absent (which Publish never produces), or
// whose type has no binding (only the bound types are ever registered), is
// dead-lettered as a permanent error rather than panicking.
func (w *Worker) deliver(sub subRegistration) func(context.Context, driver.Job) error {
	return func(ctx context.Context, job driver.Job) error {
		if job.Event == nil {
			w.logger.Error("event delivery has no ledger record; dead-lettering",
				"delivery", job.ID.String(), "subscriber", sub.name)
			return Permanent(errors.New("event: delivery has no ledger record"))
		}
		b, ok := sub.bindings[job.Event.Type]
		if !ok {
			w.logger.Error("event delivery has no binding for its type; dead-lettering",
				"delivery", job.ID.String(), "subscriber", sub.name, "type", job.Event.Type)
			return Permanent(fmt.Errorf("event: subscriber %q has no binding for event type %q",
				sub.name, job.Event.Type))
		}
		return b.invoke(NewContext(ctx, deliveryFrom(job)), job.Event.Payload)
	}
}
