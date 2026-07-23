package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
)

type registerOptions struct {
	concurrency   int
	maxRetries    int
	maxRetriesSet bool
	timeout       time.Duration
}

// RegisterOption customizes Register.
type RegisterOption func(*registerOptions)

// WithConcurrency caps how many jobs of this kind run at once (default
// WithDefaultConcurrency).
func WithConcurrency(n int) RegisterOption {
	return func(o *registerOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// WithMaxRetries overrides the retry budget for jobs of this kind (default
// WithDefaultMaxRetries). The override is resolved durably on a job's first
// lease unless the job was enqueued with an explicit MaxRetries.
func WithMaxRetries(n int) RegisterOption {
	return func(o *registerOptions) {
		if n > 0 {
			o.maxRetries = n
			o.maxRetriesSet = true
		}
	}
}

// WithJobTimeout overrides the per-job wall clock for this kind (default
// WithDefaultJobTimeout on the runtime; 0 = unlimited).
func WithJobTimeout(d time.Duration) RegisterOption {
	return func(o *registerOptions) {
		o.timeout = d
	}
}

// RegisterKind binds a raw handler for an explicit kind string — the seam for
// dynamic kinds, where the handler receives the undecoded JSON payload and reads
// its metadata from ctx (JobID, Kind, Attempt, ...). Same rules as Register: it
// fails on duplicate kinds (typed or raw) and after Start.
func RegisterKind(w *Worker, kind string, handler func(ctx context.Context, payload json.RawMessage) error, opts ...RegisterOption) error {
	o := registerOptions{
		concurrency: w.cfg.DefaultConcurrency,
		maxRetries:  w.cfg.DefaultMaxAttempts,
		timeout:     w.cfg.jobTimeout,
	}
	for _, opt := range opts {
		opt(&o)
	}

	err := w.engine.Register(engine.Kind{
		Name:           kind,
		Concurrency:    o.concurrency,
		Timeout:        o.timeout,
		MaxAttempts:    o.maxRetries,
		MaxAttemptsSet: o.maxRetriesSet,
		Classify:       classify,
		Handler: func(ctx context.Context, job driver.Job) error {
			return handler(NewContext(ctx, jobInfoFrom(job)), job.Payload)
		},
	})
	if err != nil {
		return fmt.Errorf("queue: %w", err)
	}
	return nil
}

// Register binds the handler for T's kind on the worker: sugar over
// RegisterKind that decodes the payload into T and hands the handler the pure
// domain value. Job metadata travels on ctx (JobID, Attempt, IsRetry, ...). It
// fails on duplicate kinds and after Start — registration happens in the
// composition root, before the worker runs.
func Register[T JobArgs](w *Worker, handler func(ctx context.Context, args T) error, opts ...RegisterOption) error {
	var zero T
	kind := zero.Kind()

	return RegisterKind(w, kind, func(ctx context.Context, payload json.RawMessage) error {
		var args T
		if err := json.Unmarshal(payload, &args); err != nil {
			// A payload that cannot decode will never decode — retrying is futile.
			return Abort(fmt.Errorf("queue: decode %s payload: %w", kind, err))
		}
		return handler(ctx, args)
	}, opts...)
}
