package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
)

// None is the result type of a task that produces no output: a handler
// returning None persists no result (ResultOf on it yields the zero value).
type None = struct{}

type registerOptions struct {
	concurrency int
	maxRetries  int
	timeout     time.Duration
}

// RegisterOption customizes Register.
type RegisterOption func(*registerOptions)

// WithConcurrency caps how many tasks of this kind run at once (default
// WithDefaultConcurrency).
func WithConcurrency(n int) RegisterOption {
	return func(o *registerOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// WithMaxRetries overrides the retry budget for tasks of this kind (default
// WithDefaultMaxRetries). The override is resolved durably on a task's first
// lease unless the task was declared with an explicit MaxRetries task option,
// which always wins.
func WithMaxRetries(n int) RegisterOption {
	return func(o *registerOptions) {
		if n > 0 {
			o.maxRetries = n
		}
	}
}

// WithTaskTimeout overrides the per-task wall clock for this kind (default
// WithDefaultTaskTimeout on the runtime; 0 = unlimited).
func WithTaskTimeout(d time.Duration) RegisterOption {
	return func(o *registerOptions) {
		o.timeout = d
	}
}

// RegisterKind binds a raw handler for an explicit kind string — the seam for
// dynamic kinds, where the handler receives the undecoded JSON payload and
// returns the raw result to persist (nil for none). Task metadata travels on
// ctx (WorkflowID, TaskKey, Attempt, ...) and dependency outputs are read with
// ResultOf. Same rules as Register: it fails on a kind with the reserved "$"
// prefix, on duplicate kinds (typed or raw) and after Start.
func RegisterKind(w *Worker, kind string, handler func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error), opts ...RegisterOption) error {
	if strings.HasPrefix(kind, "$") {
		return fmt.Errorf("workflow: kind %q is reserved (the \"$\" prefix belongs to internal tasks)", kind)
	}
	o := registerOptions{
		concurrency: w.cfg.DefaultConcurrency,
		maxRetries:  w.cfg.DefaultMaxAttempts,
		timeout:     w.cfg.taskTimeout,
	}
	for _, opt := range opts {
		opt(&o)
	}

	err := w.engine.Register(engine.Kind{
		Name:        kind,
		Concurrency: o.concurrency,
		Timeout:     o.timeout,
		MaxAttempts: o.maxRetries,
		// Workflow tasks are created with a zero budget unless the MaxRetries
		// task option pinned one, so the kind's budget must always resolve
		// durably on the first lease (task option > register option > runtime
		// default).
		MaxAttemptsSet: true,
		Classify:       classify,
		Handler: func(ctx context.Context, job driver.Job) (json.RawMessage, error) {
			ctx = NewContext(ctx, taskInfoFrom(job))
			ctx = withResolver(ctx, newResultResolver(w.store, job.WorkflowID))
			return handler(ctx, job.Payload)
		},
	})
	if err != nil {
		return fmt.Errorf("workflow: %w", err)
	}
	return nil
}

// Register binds the handler for T's kind on the worker: sugar over
// RegisterKind that decodes the payload into T, hands the handler the pure
// domain value, and persists the returned R as the task's durable result
// (readable downstream through ResultOf). T and R are inferred from the
// handler signature. For a task without a result use R = None, which persists
// nothing. Task metadata travels on ctx (WorkflowID, TaskKey, Attempt, ...).
// It fails on duplicate kinds and after Start — registration happens in the
// composition root, before the worker runs.
func Register[T TaskArgs, R any](w *Worker, fn func(ctx context.Context, task T) (R, error), opts ...RegisterOption) error {
	var zero T
	kind := zero.Kind()

	return RegisterKind(w, kind, func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
		var task T
		if err := json.Unmarshal(payload, &task); err != nil {
			// A payload that cannot decode will never decode — retrying is futile.
			return nil, Abort(fmt.Errorf("workflow: decode %s payload: %w", kind, err))
		}
		out, err := fn(ctx, task)
		if err != nil {
			return nil, err
		}
		if _, none := any(out).(None); none {
			return nil, nil
		}
		result, err := json.Marshal(out)
		if err != nil {
			// A value that cannot marshal will not marshal on a retry either.
			return nil, Abort(fmt.Errorf("workflow: marshal %s result: %w", kind, err))
		}
		return result, nil
	}, opts...)
}
