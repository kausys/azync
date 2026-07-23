package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// Producer enqueues jobs. The active trace is stamped automatically so the
// consumer span at execution time joins the producer's trace.
type Producer struct {
	store              driver.Store
	defaultMaxAttempts int
}

// EnqueueResult reports the outcome of an Enqueue.
type EnqueueResult struct {
	ID           uuid.UUID
	Deduplicated bool // dropped by an idempotency key
}

type enqueueOptions struct {
	delay              time.Duration
	runAt              time.Time // zero = not set (Delay applies)
	idemKey            string
	idemTTL            time.Duration
	maxRetries         int
	maxRetriesExplicit bool
	meta               map[string]string
}

// EnqueueOption customizes one Enqueue.
type EnqueueOption func(*enqueueOptions)

// Delay schedules the job for now()+d (resolved on the backend clock).
func Delay(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.delay = d }
}

// At schedules the job for an absolute time (wins over Delay).
func At(t time.Time) EnqueueOption {
	return func(o *enqueueOptions) { o.runAt = t }
}

// IdempotencyKey deduplicates while a live job (pending/scheduled/active/
// paused) holds the key; completion or death frees it.
func IdempotencyKey(k string) EnqueueOption {
	return func(o *enqueueOptions) { o.idemKey = k }
}

// IdempotencyKeyTTL deduplicates within a time window that survives job
// completion (cron occurrences, webhook deliveries).
func IdempotencyKeyTTL(k string, window time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.idemKey = k; o.idemTTL = window }
}

// MaxRetries overrides the retry budget for this job.
func MaxRetries(n int) EnqueueOption {
	return func(o *enqueueOptions) {
		if n > 0 {
			o.maxRetries = n
			o.maxRetriesExplicit = true
		}
	}
}

// Meta attaches one metadata entry (repeatable).
func Meta(key, value string) EnqueueOption {
	return func(o *enqueueOptions) {
		if o.meta == nil {
			o.meta = map[string]string{}
		}
		o.meta[key] = value
	}
}

// Enqueue durably inserts a job for args.Kind(). It returns Deduplicated=true
// when an idempotency key dropped the insert.
func (p *Producer) Enqueue(ctx context.Context, args JobArgs, opts ...EnqueueOption) (EnqueueResult, error) {
	params, err := p.makeParams(ctx, args, opts...)
	if err != nil {
		return EnqueueResult{}, err
	}
	inserted, err := p.store.Enqueue(ctx, params)
	if err != nil {
		return EnqueueResult{}, err
	}
	return EnqueueResult{ID: params.ID, Deduplicated: !inserted}, nil
}

func (p *Producer) makeParams(ctx context.Context, args JobArgs, opts ...EnqueueOption) (driver.EnqueueParams, error) {
	o := enqueueOptions{maxRetries: p.defaultMaxAttempts}
	for _, opt := range opts {
		opt(&o)
	}

	payload, err := json.Marshal(args)
	if err != nil {
		return driver.EnqueueParams{}, fmt.Errorf("queue: marshal %s payload: %w", args.Kind(), err)
	}

	params := driver.EnqueueParams{
		ID:                  uuid.New(),
		Kind:                args.Kind(),
		Payload:             payload,
		Meta:                o.meta,
		RunAt:               o.runAt,
		Delay:               o.delay,
		MaxAttempts:         o.maxRetries,
		MaxAttemptsExplicit: o.maxRetriesExplicit,
		IdempotencyKey:      o.idemKey,
		IdempotencyTTL:      o.idemTTL,
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		params.TraceID = sc.TraceID().String()
		params.SpanID = sc.SpanID().String()
		params.TraceFlags = int16(sc.TraceFlags())
	}
	return params, nil
}

// TxProducerClient enqueues jobs inside the caller's own backend transaction,
// so the enqueue commits atomically with the caller's writes (outbox pattern).
// Build one with TxProducer.
type TxProducerClient[TTx any] struct {
	store    driver.TxStore[TTx]
	producer *Producer
}

// TxProducer builds the transactional enqueue client for the driver's
// transaction handle type TTx (e.g. pgx.Tx for the pg driver). It fails
// immediately when the runtime's driver does not support transactional
// enqueues for that type.
func TxProducer[TTx any](r *Runtime) (*TxProducerClient[TTx], error) {
	store := r.core.Store()
	ts, ok := store.(driver.TxStore[TTx])
	if !ok {
		return nil, fmt.Errorf(
			"queue: driver %T does not support transactional enqueues with transaction type %s",
			store, reflect.TypeFor[TTx]())
	}
	return &TxProducerClient[TTx]{store: ts, producer: r.producer}, nil
}

// EnqueueTx performs Enqueue within tx, letting the caller atomically commit
// application writes and the enqueue.
func (c *TxProducerClient[TTx]) EnqueueTx(ctx context.Context, tx TTx, args JobArgs, opts ...EnqueueOption) (EnqueueResult, error) {
	params, err := c.producer.makeParams(ctx, args, opts...)
	if err != nil {
		return EnqueueResult{}, err
	}
	inserted, err := c.store.EnqueueTx(ctx, tx, params)
	if err != nil {
		return EnqueueResult{}, err
	}
	return EnqueueResult{ID: params.ID, Deduplicated: !inserted}, nil
}
