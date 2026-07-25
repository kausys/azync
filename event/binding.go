package event

import (
	"context"
	"encoding/json"
	"fmt"
)

// Subscriber identifies a durable event consumer by name. Implement it on the
// value you pass to Worker.Register; the type stays non-generic (Go interfaces
// cannot be generic) because the event types it consumes are supplied
// separately, as typed bindings built with On.
//
// A subscriber may additionally implement interface{ MaxAttempts() int } to pin
// its retry budget; without it the runtime's DefaultMaxAttempts applies (floored
// at 1).
type Subscriber interface {
	SubscriberName() string
}

// maxAttempter is the optional retry-budget declaration a Subscriber may satisfy
// to override the runtime default.
type maxAttempter interface {
	MaxAttempts() int
}

// Binding pairs one event type with the typed handler that consumes it. It is
// opaque: build one with On, which infers the event type and the payload
// decoding from the handler signature, and hand the results to Worker.Register.
type Binding struct {
	eventType string
	// invoke decodes the raw ledger payload into the handler's type and calls it.
	// A payload that cannot decode is Permanent (straight to the dead letter),
	// since a malformed payload will never decode on retry.
	invoke func(ctx context.Context, payload json.RawMessage) error
}

// On builds a typed Binding for the event type T identifies. T is inferred from
// fn, so the caller never writes it: On(func(ctx, OrderCreated) error) binds the
// "orders.created.v1" type its EventType reports. The handler receives the
// decoded domain event; delivery metadata travels on ctx (Attempt, IsRetry,
// EventID, ...).
func On[T EventArgs](fn func(ctx context.Context, evt T) error) Binding {
	var zero T
	eventType := zero.EventType()
	return Binding{
		eventType: eventType,
		invoke: func(ctx context.Context, payload json.RawMessage) error {
			var evt T
			if err := json.Unmarshal(payload, &evt); err != nil {
				return Permanent(fmt.Errorf("event: decode %s payload: %w", eventType, err))
			}
			return fn(ctx, evt)
		},
	}
}

// registerOptions holds the resolved knobs for RegisterFunc.
type registerOptions struct {
	maxAttempts int
}

// RegisterOption customizes RegisterFunc.
type RegisterOption func(*registerOptions)

// WithMaxAttempts pins the subscriber's retry budget, overriding the runtime
// DefaultMaxAttempts. A non-positive value is ignored (the default applies).
func WithMaxAttempts(n int) RegisterOption {
	return func(o *registerOptions) {
		if n > 0 {
			o.maxAttempts = n
		}
	}
}

// RegisterFunc registers a single-type subscriber under an explicit name: sugar
// over Worker.Register for the common one-event-per-consumer shape, with T
// inferred from fn. It fails on an empty name, a duplicate subscriber, or a call
// after Start. Like Register, it upserts the durable (name, T.EventType())
// subscription in Start (see Worker.Register for the durability caveat).
func RegisterFunc[T EventArgs](w *Worker, name string, fn func(ctx context.Context, evt T) error, opts ...RegisterOption) error {
	o := registerOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	return w.register(name, o.maxAttempts, On(fn))
}
