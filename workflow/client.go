package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// Client creates and signals workflows. The active trace is stamped
// automatically so the task spans at execution time join the creator's trace.
type Client struct {
	store              driver.WorkflowStore
	defaultMaxAttempts int
}

// RunResult reports the outcome of a Run.
type RunResult struct {
	// ID identifies the workflow: the new one, or — when Deduplicated is true —
	// the live execution that already held the idempotency key.
	ID uuid.UUID
	// Deduplicated is true when an idempotency key matched a live execution and
	// nothing was inserted.
	Deduplicated bool
}

type runOptions struct {
	idemKey string
	meta    map[string]string
}

// RunOption customizes one Run.
type RunOption func(*runOptions)

// WithIdempotencyKey deduplicates the run within the definition's name: while
// a workflow with the same (name, key) is live (running, suspended or
// compensating), Run inserts nothing and returns the live execution's id with
// Deduplicated=true. A terminal workflow frees the key, so a finished flow can
// be re-run with the same key.
func WithIdempotencyKey(key string) RunOption {
	return func(o *runOptions) { o.idemKey = key }
}

// WithRunMeta attaches one string-valued annotation to this run (repeatable).
// It merges over the definition's WithMeta entries; on a key conflict the run
// entry wins. (The names differ because both option sets live in this
// package.)
func WithRunMeta(key, value string) RunOption {
	return func(o *runOptions) {
		if o.meta == nil {
			o.meta = map[string]string{}
		}
		o.meta[key] = value
	}
}

// Run validates def and durably inserts the whole workflow — header, tasks and
// dependency edges — in one atomic operation. Dependency-free tasks are
// immediately runnable; the rest start blocked and are promoted by the
// scheduler as their dependencies succeed.
//
// Run is safe to call from inside a task handler of another workflow: combined
// with WithIdempotencyKey it is the barrier pattern for fan-in across
// workflows. When N workflows must collectively start one downstream workflow
// (say, the last task of each upstream flow checks "are all siblings done?"
// and fires the next stage), every one of them simply calls Run with the same
// key: exactly one insert wins and the others get the winner's id with
// Deduplicated=true. The at-least-once re-execution of the calling task is
// absorbed by the same key — no distributed lock needed.
func (c *Client) Run(ctx context.Context, def *Definition, opts ...RunOption) (RunResult, error) {
	params, err := c.makeParams(ctx, def, opts...)
	if err != nil {
		return RunResult{}, err
	}
	inserted, existingID, err := c.store.CreateWorkflow(ctx, params)
	if err != nil {
		return RunResult{}, err
	}
	if !inserted {
		return RunResult{ID: existingID, Deduplicated: true}, nil
	}
	return RunResult{ID: params.ID}, nil
}

// makeParams validates def and assembles the driver params for one run.
func (c *Client) makeParams(ctx context.Context, def *Definition, opts ...RunOption) (driver.WorkflowParams, error) {
	if def == nil {
		return driver.WorkflowParams{}, errors.New("workflow: Run: definition is nil")
	}
	if err := def.validate(); err != nil {
		return driver.WorkflowParams{}, err
	}
	var o runOptions
	for _, opt := range opts {
		opt(&o)
	}

	meta := def.meta
	if len(o.meta) > 0 {
		merged := make(map[string]string, len(def.meta)+len(o.meta))
		maps.Copy(merged, def.meta)
		maps.Copy(merged, o.meta)
		meta = merged
	}

	params := driver.WorkflowParams{
		ID:             uuid.New(),
		Name:           def.name,
		OnFailure:      def.onFailure,
		IdempotencyKey: o.idemKey,
		Meta:           meta,
		Tasks:          make([]driver.WorkflowTask, 0, len(def.tasks)),
	}
	for _, t := range def.tasks {
		task, err := c.makeTask(def, t)
		if err != nil {
			return driver.WorkflowParams{}, err
		}
		params.Tasks = append(params.Tasks, task)
		for _, dep := range t.after {
			params.Deps = append(params.Deps, driver.WorkflowDep{TaskKey: t.key, DependsOnKey: dep})
		}
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		params.TraceID = sc.TraceID().String()
		params.SpanID = sc.SpanID().String()
		params.TraceFlags = int16(sc.TraceFlags())
	}
	return params, nil
}

// makeTask assembles one driver task from its declaration. The task-level
// MaxRetries is stamped durably; without it the budget stays zero and is
// resolved on the task's first lease from the registered kind (register option
// or runtime default) — task option > register option > runtime default.
func (c *Client) makeTask(def *Definition, t taskDecl) (driver.WorkflowTask, error) {
	task := driver.WorkflowTask{
		Key:            t.key,
		Kind:           t.kind,
		MaxAttempts:    t.maxRetries,
		SleepFor:       t.sleepFor,
		IgnoreDeadDeps: t.ignoreDeadDeps,
	}
	if t.args != nil {
		task.Kind = t.args.Kind()
		payload, err := json.Marshal(t.args)
		if err != nil {
			return driver.WorkflowTask{}, fmt.Errorf("workflow: definition %q: marshal task %q payload: %w",
				def.name, t.key, err)
		}
		task.Payload = payload
	}
	switch t.kind {
	case driver.KindSleep, driver.KindSignal:
		// A signal completes the wait task named after it and wakes the timer
		// named after it early.
		task.SignalName = t.key
	}
	if t.compensate != nil {
		task.CompensationKind = t.compensate.Kind()
		payload, err := json.Marshal(t.compensate)
		if err != nil {
			return driver.WorkflowTask{}, fmt.Errorf("workflow: definition %q: marshal task %q compensation payload: %w",
				def.name, t.key, err)
		}
		task.CompensationPayload = payload
	}
	return task, nil
}

// Signal delivers a named signal with payload (marshaled to JSON) to one
// workflow: a waiting WaitSignal task of that name completes with the payload
// as its result, and a pending Sleep timer of that name wakes early. When
// nothing on the workflow was waiting for the name, it returns an error
// wrapping ErrNoSignalMatched — the workflow may have moved on, or not reached
// the wait yet; callers deciding to retry can test with errors.Is.
func (c *Client) Signal(ctx context.Context, id uuid.UUID, name string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("workflow: marshal signal %q payload: %w", name, err)
	}
	matched, err := c.store.Signal(ctx, id, name, body)
	if err != nil {
		return err
	}
	if matched == 0 {
		return fmt.Errorf("workflow: signal %q on workflow %s: %w", name, id, ErrNoSignalMatched)
	}
	return nil
}

// TxRunnerClient creates workflows inside the caller's own backend
// transaction, so the creation commits atomically with the caller's writes
// (outbox pattern). Build one with TxRunner.
type TxRunnerClient[TTx any] struct {
	store  driver.TxWorkflowStore[TTx]
	client *Client
}

// TxRunner builds the transactional workflow-creation client for the driver's
// transaction handle type TTx (e.g. pgx.Tx for the pg driver). It fails
// immediately when the runtime's driver does not support transactional
// workflow creation for that type.
func TxRunner[TTx any](r *Runtime) (*TxRunnerClient[TTx], error) {
	store := r.core.Store()
	ts, ok := store.(driver.TxWorkflowStore[TTx])
	if !ok {
		return nil, fmt.Errorf(
			"workflow: driver %T does not support transactional workflow creation with transaction type %s",
			store, reflect.TypeFor[TTx]())
	}
	return &TxRunnerClient[TTx]{store: ts, client: r.client}, nil
}

// RunTx performs Run within tx, letting the caller atomically commit
// application writes and the workflow creation. Same validation, options and
// dedupe semantics as Run.
func (c *TxRunnerClient[TTx]) RunTx(ctx context.Context, tx TTx, def *Definition, opts ...RunOption) (RunResult, error) {
	params, err := c.client.makeParams(ctx, def, opts...)
	if err != nil {
		return RunResult{}, err
	}
	inserted, existingID, err := c.store.CreateWorkflowTx(ctx, tx, params)
	if err != nil {
		return RunResult{}, err
	}
	if !inserted {
		return RunResult{ID: existingID, Deduplicated: true}, nil
	}
	return RunResult{ID: params.ID}, nil
}
