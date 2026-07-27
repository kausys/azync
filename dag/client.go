package dag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// Client creates and signals dags.
type Client struct {
	store driver.DAGStore
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
// dags. When N dags must collectively start one downstream workflow
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
	inserted, existingID, err := c.store.CreateDAG(ctx, params)
	if err != nil {
		return RunResult{}, err
	}
	if !inserted {
		return RunResult{ID: existingID, Deduplicated: true}, nil
	}
	return RunResult{ID: params.ID}, nil
}

// makeParams validates def and assembles the driver params for one run.
func (c *Client) makeParams(ctx context.Context, def *Definition, opts ...RunOption) (driver.DAGParams, error) {
	if def == nil {
		return driver.DAGParams{}, errors.New("dag: Run: definition is nil")
	}
	if err := def.validate(); err != nil {
		return driver.DAGParams{}, err
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

	params := driver.DAGParams{
		ID:             uuid.New(),
		Name:           def.name,
		OnFailure:      def.onFailure,
		IdempotencyKey: o.idemKey,
		Meta:           meta,
		Tasks:          make([]driver.DAGTask, 0, len(def.tasks)),
	}
	for _, t := range def.tasks {
		task, err := c.makeTask(def, t)
		if err != nil {
			return driver.DAGParams{}, err
		}
		params.Tasks = append(params.Tasks, task)
		for _, dep := range t.after {
			params.Deps = append(params.Deps, driver.DAGDep{TaskKey: t.key, DependsOnKey: dep})
		}
	}
	return params, nil
}

// makeTask assembles one driver task from its declaration. The task-level
// MaxRetries is stamped durably; without it the budget stays zero and is
// resolved on the task's first lease from the registered kind (register option
// or runtime default) — task option > register option > runtime default.
func (c *Client) makeTask(def *Definition, t taskDecl) (driver.DAGTask, error) {
	task := driver.DAGTask{
		Key:            t.key,
		Kind:           t.kind,
		MaxAttempts:    t.maxRetries,
		SleepFor:       t.sleepFor,
		IgnoreDeadDeps: t.ignoreDeadDeps,
		Deadline:       t.deadline,
	}
	if t.args != nil {
		task.Kind = t.args.Kind()
		payload, err := json.Marshal(t.args)
		if err != nil {
			return driver.DAGTask{}, fmt.Errorf("dag: definition %q: marshal task %q payload: %w",
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
			return driver.DAGTask{}, fmt.Errorf("dag: definition %q: marshal task %q compensation payload: %w",
				def.name, t.key, err)
		}
		task.CompensationPayload = payload
	}
	return task, nil
}

// signalOptions collects per-delivery options for Signal.
type signalOptions struct{ messageID string }

// SignalOption customizes one Signal delivery.
type SignalOption func(*signalOptions)

// WithMessageID deduplicates the delivery within (workflow, signal name):
// while an earlier signal with the same id exists on the workflow, a repeat
// is accepted and dropped without effect. Use the sender's event id for
// at-least-once webhooks, so a redelivery never double-fires.
func WithMessageID(id string) SignalOption {
	return func(o *signalOptions) { o.messageID = id }
}

// Signal delivers a named signal with payload (marshaled to JSON) to one live
// workflow: a waiting WaitSignal task of that name completes with the payload
// as its result, and a pending Sleep timer of that name wakes early. The
// signal is durable: when nothing is waiting yet — the task still blocked
// behind dependencies, or the scheduler's promotion racing this call — it is
// buffered and delivered by the scheduler once the task becomes deliverable,
// never lost. It returns an error only on a marshal failure or for a missing
// or terminal workflow (test with IsNotFound).
func (c *Client) Signal(ctx context.Context, id uuid.UUID, name string, payload any, opts ...SignalOption) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("dag: marshal signal %q payload: %w", name, err)
	}
	var o signalOptions
	for _, opt := range opts {
		opt(&o)
	}
	if _, _, err := c.store.Signal(ctx, driver.DAGSignalParams{
		DAGID: id, Name: name, MessageID: o.messageID, Payload: body,
	}); err != nil {
		return fmt.Errorf("dag: signal %q on workflow %s: %w", name, id, err)
	}
	return nil
}

// SignalByKey resolves the live workflow holding (name, idempotencyKey) and
// delivers the signal to it, in one call — the shape a webhook handler
// wants: it knows the provider's business key (the same string passed to
// WithIdempotencyKey at Run), never the run UUID, and needs no bookkeeping
// table mapping one to the other. At most one live run holds a key (the
// dedupe barrier); terminal runs free it, so a webhook arriving after the
// run settled gets a not-found error (test with IsNotFound) — late, not
// wrong. The resolve and the delivery are two calls: a run settling between
// them also surfaces as not-found from the delivery, the same right answer.
func (c *Client) SignalByKey(ctx context.Context, name, idempotencyKey, signalName string, payload any, opts ...SignalOption) error {
	id, err := c.store.FindDAGByKey(ctx, name, idempotencyKey)
	if err != nil {
		return fmt.Errorf("dag: signal by key %q/%q: %w", name, idempotencyKey, err)
	}
	return c.Signal(ctx, id, signalName, payload, opts...)
}

// TxRunnerClient creates dags inside the caller's own backend
// transaction, so the creation commits atomically with the caller's writes
// (outbox pattern). Build one with TxRunner.
type TxRunnerClient[TTx any] struct {
	store  driver.TxDAGStore[TTx]
	client *Client
}

// TxRunner builds the transactional workflow-creation client for the driver's
// transaction handle type TTx (e.g. pgx.Tx for the pg driver). It fails
// immediately when the runtime's driver does not support transactional
// workflow creation for that type.
func TxRunner[TTx any](r *Runtime) (*TxRunnerClient[TTx], error) {
	store := r.core.Store()
	ts, ok := store.(driver.TxDAGStore[TTx])
	if !ok {
		return nil, fmt.Errorf(
			"dag: driver %T does not support transactional workflow creation with transaction type %s",
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
	inserted, existingID, err := c.store.CreateDAGTx(ctx, tx, params)
	if err != nil {
		return RunResult{}, err
	}
	if !inserted {
		return RunResult{ID: existingID, Deduplicated: true}, nil
	}
	return RunResult{ID: params.ID}, nil
}
