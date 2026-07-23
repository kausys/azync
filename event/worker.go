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

// Worker leases and executes event deliveries on the shared engine. Handlers
// register via Subscribe before Start; Start then builds one engine job kind
// per subscribed handler, so a subscriber without a live handler is simply
// never fetched — the "missing handler" case disappears by design.
type Worker struct {
	engine *engine.Engine
	cfg    config
	logger *slog.Logger

	mu         sync.Mutex
	started    bool
	registered bool
	handlers   map[string]Handler
}

// Subscribe binds a handler to a subscriber name. It fails on an empty name or
// nil handler, on a duplicate subscriber, and after Start.
func (w *Worker) Subscribe(name string, handler Handler) error {
	if name == "" || handler == nil {
		return errors.New("event: subscriber name and handler are required")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		return errors.New("event: cannot subscribe after Start")
	}
	if _, exists := w.handlers[name]; exists {
		return fmt.Errorf("event: subscriber %q already registered", name)
	}
	w.handlers[name] = handler
	return nil
}

// Ready closes after wakeup setup succeeds and the polling loops are running.
// Polling-only workers become ready immediately after Start.
func (w *Worker) Ready() <-chan struct{} { return w.engine.Ready() }

// Start registers one engine kind per subscribed handler and runs the engine
// (fetch, execute, settle, maintenance) until ctx is cancelled. On cancellation
// in-flight handlers drain for up to the shutdown drain budget. Start fails if
// called twice; a Start that failed during setup (before the engine ran) may be
// retried.
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return errors.New("event: worker already started")
	}
	w.started = true
	registered := w.registered
	handlers := maps.Clone(w.handlers)
	w.mu.Unlock()

	// Kinds survive a failed setup on the engine, so register them exactly once
	// even across Start retries.
	if !registered {
		for name, handler := range handlers {
			if err := w.engine.Register(engine.Kind{
				Name:        name,
				Concurrency: w.cfg.DefaultConcurrency,
				Timeout:     w.cfg.handlerTimeout,
				Classify:    classify,
				Handler:     w.deliver(name, handler),
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

// deliver adapts a subscriber handler to the engine's driver.Job handler,
// rehydrating the Envelope from the job's ledger record. A delivery whose ledger
// record is absent (which Publish never produces) is dead-lettered as a
// permanent error rather than panicking.
func (w *Worker) deliver(name string, handler Handler) func(context.Context, driver.Job) error {
	return func(ctx context.Context, job driver.Job) error {
		if job.Event == nil {
			w.logger.Error("event delivery has no ledger record; dead-lettering",
				"delivery", job.ID.String(), "subscriber", name)
			return Permanent(errors.New("event: delivery has no ledger record"))
		}
		return handler(ctx, envelopeFrom(job))
	}
}

// envelopeFrom rehydrates the Envelope from a leased delivery: every ledger
// field from job.Event, plus the delivery's own Subscriber, Attempt and Replay.
func envelopeFrom(job driver.Job) Envelope {
	e := job.Event
	return Envelope{
		ID:            e.ID,
		Type:          e.Type,
		TenantID:      e.TenantID,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID,
		Version:       e.Version,
		OccurredAt:    e.OccurredAt,
		Payload:       e.Payload,
		Meta:          e.Meta,
		Subscriber:    job.Kind,
		Attempt:       job.Attempt,
		Replay:        job.Replay,
	}
}
