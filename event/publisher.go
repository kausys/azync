package event

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// Publisher appends events to the durable ledger and registers subscribers.
// Publish atomically snapshots the subscribers matching an event and fans out
// one delivery job per match; the active trace is stamped so the consumer span
// at delivery time joins the publisher's trace.
type Publisher struct {
	store              driver.Store
	defaultMaxAttempts int
}

// Register adds or updates a durable subscription. A subscription registered
// with MaxAttempts <= 0 inherits the runtime's default retry budget (floored at
// 1); registration is an upsert keyed by (Name, EventType). Worker.Register and
// RegisterFunc perform this upsert automatically in Start; call this directly
// only for administrative registration (migrations, or subscribers consumed by
// external processes).
func (p *Publisher) Register(ctx context.Context, subscription Subscription) error {
	if subscription.Name == "" || subscription.EventType == "" {
		return errors.New("event: subscriber name and event type are required")
	}
	if subscription.MaxAttempts <= 0 {
		subscription.MaxAttempts = p.defaultMaxAttempts
	}
	if subscription.MaxAttempts < 1 {
		subscription.MaxAttempts = 1
	}
	return p.store.RegisterSubscriber(ctx, driver.Subscriber(subscription))
}

// Publish appends an event to the ledger. The backend selects the matching
// subscriber snapshot and creates the initial deliveries in one transaction; it
// returns the new event's id.
func (p *Publisher) Publish(ctx context.Context, args EventArgs, opts ...PublishOption) (uuid.UUID, error) {
	params, err := p.makeParams(ctx, args, opts...)
	if err != nil {
		return uuid.Nil, err
	}
	if _, err := p.store.Publish(ctx, params); err != nil {
		return uuid.Nil, err
	}
	return params.ID, nil
}

func (p *Publisher) makeParams(ctx context.Context, args EventArgs, opts ...PublishOption) (driver.PublishParams, error) {
	if args == nil || args.EventType() == "" {
		return driver.PublishParams{}, errors.New("event: event type is required")
	}
	// TraceFromContext is applied before the explicit options so an explicit
	// WithTrace always wins over the ambient span context.
	o := publishOptions{}
	TraceFromContext(ctx)(&o)
	for _, opt := range opts {
		opt(&o)
	}
	payload, err := json.Marshal(args)
	if err != nil {
		return driver.PublishParams{}, fmt.Errorf("event: marshal %s: %w", args.EventType(), err)
	}
	return driver.PublishParams{
		ID:            uuid.New(),
		Type:          args.EventType(),
		TenantID:      o.tenantID,
		AggregateType: o.aggregateType,
		AggregateID:   o.aggregateID,
		Version:       o.version,
		OccurredAt:    time.Now().UTC(),
		Payload:       payload,
		Meta:          o.meta,
		TraceID:       o.traceID,
		SpanID:        o.spanID,
		TraceFlags:    o.traceFlags,
	}, nil
}

type publishOptions struct {
	aggregateType string
	aggregateID   string
	version       int64
	meta          map[string]string
	tenantID      uuid.UUID
	traceID       string
	spanID        string
	traceFlags    int16
}

// PublishOption customizes one Publish.
type PublishOption func(*publishOptions)

// WithAggregate stamps the source aggregate type and id on the event.
func WithAggregate(aggregateType, id string) PublishOption {
	return func(o *publishOptions) {
		o.aggregateType, o.aggregateID = aggregateType, id
	}
}

// WithVersion stamps the aggregate version this event advances to.
func WithVersion(version int64) PublishOption {
	return func(o *publishOptions) { o.version = version }
}

// WithMeta attaches one metadata entry (repeatable).
func WithMeta(key, value string) PublishOption {
	return func(o *publishOptions) {
		if o.meta == nil {
			o.meta = make(map[string]string)
		}
		o.meta[key] = value
	}
}

// WithTenantID stamps a first-class tenant id on the event.
func WithTenantID(id uuid.UUID) PublishOption {
	return func(o *publishOptions) { o.tenantID = id }
}

// WithTrace stamps OpenTelemetry identifiers explicitly. It wins over the
// ambient span context copied by TraceFromContext.
func WithTrace(traceID, spanID string, flags int16) PublishOption {
	return func(o *publishOptions) {
		o.traceID, o.spanID, o.traceFlags = traceID, spanID, flags
	}
}

// TraceFromContext copies a valid span context onto publish options. Publish
// applies it before any explicit option, so an explicit WithTrace overrides it.
func TraceFromContext(ctx context.Context) PublishOption {
	return func(o *publishOptions) {
		sc := trace.SpanContextFromContext(ctx)
		if !sc.IsValid() {
			return
		}
		o.traceID = sc.TraceID().String()
		o.spanID = sc.SpanID().String()
		o.traceFlags = int16(sc.TraceFlags())
	}
}

// TxPublisherClient publishes events inside the caller's own backend
// transaction, so the append and its delivery fan-out commit atomically with
// the caller's writes (outbox pattern). Build one with TxPublisher.
type TxPublisherClient[TTx any] struct {
	store     driver.TxStore[TTx]
	publisher *Publisher
}

// TxPublisher builds the transactional publish client for the driver's
// transaction handle type TTx (e.g. pgx.Tx for the pg driver). It fails
// immediately when the runtime's driver does not support transactional
// publishes for that type.
func TxPublisher[TTx any](r *Runtime) (*TxPublisherClient[TTx], error) {
	store := r.core.Store()
	ts, ok := store.(driver.TxStore[TTx])
	if !ok {
		return nil, fmt.Errorf(
			"event: driver %T does not support transactional publishes with transaction type %s",
			store, reflect.TypeFor[TTx]())
	}
	return &TxPublisherClient[TTx]{store: ts, publisher: r.publisher}, nil
}

// PublishTx performs Publish within tx, letting the caller atomically commit
// application writes and the event fan-out. It returns the new event's id.
func (c *TxPublisherClient[TTx]) PublishTx(ctx context.Context, tx TTx, args EventArgs, opts ...PublishOption) (uuid.UUID, error) {
	params, err := c.publisher.makeParams(ctx, args, opts...)
	if err != nil {
		return uuid.Nil, err
	}
	if _, err := c.store.PublishTx(ctx, tx, params); err != nil {
		return uuid.Nil, err
	}
	return params.ID, nil
}
