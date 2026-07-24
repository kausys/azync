package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// TaskInfo is the cross-cutting metadata of one task execution. Handlers
// receive the decoded arguments as their typed value; everything about the
// task itself — its workflow, key, kind, attempt and the workflow's
// annotations — travels on the context and is read through the package
// accessors (WorkflowID, TaskKey, Attempt, ...).
type TaskInfo struct {
	WorkflowID  uuid.UUID
	TaskKey     string
	Kind        string
	Attempt     int // 1-based: first execution is attempt 1
	MaxAttempts int
	EnqueuedAt  time.Time
	Meta        map[string]string
}

// taskKey is the private context key carrying the TaskInfo of the task in
// flight. A single value per package holds the whole struct; the accessors
// read their field from it.
type taskKey struct{}

// resolverKey is the private context key carrying the task-result resolver the
// worker injects for ResultOf.
type resolverKey struct{}

// NewContext returns a copy of parent carrying info, so a handler can be
// exercised in isolation in a test without a running worker: build a TaskInfo,
// attach it, and the accessors below read from it exactly as they do in
// production. A context built this way carries no result resolver, so ResultOf
// returns a clear error on it.
func NewContext(parent context.Context, info TaskInfo) context.Context {
	return context.WithValue(parent, taskKey{}, info)
}

// TaskFromContext returns the TaskInfo carried by ctx and whether one was
// present. Outside a task (a ctx that never passed through a worker) it
// returns the zero TaskInfo and false.
func TaskFromContext(ctx context.Context) (TaskInfo, bool) {
	info, ok := ctx.Value(taskKey{}).(TaskInfo)
	return info, ok
}

func taskFromContext(ctx context.Context) TaskInfo {
	info, _ := TaskFromContext(ctx)
	return info
}

// taskInfoFrom projects the cross-cutting metadata of a leased task job onto a
// TaskInfo the handler reads through the accessors.
func taskInfoFrom(job driver.Job) TaskInfo {
	return TaskInfo{
		WorkflowID:  job.WorkflowID,
		TaskKey:     job.TaskKey,
		Kind:        job.Kind,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		EnqueuedAt:  job.EnqueuedAt,
		Meta:        job.Meta,
	}
}

// The accessors below read the metadata a handler is running under. Each is
// zero-value-safe: called on a context that did not come from a task (for
// example in a unit test that forgot NewContext), it returns the zero value of
// its result rather than panicking.

// WorkflowID is the id of the workflow the task belongs to. uuid.Nil outside a
// task.
func WorkflowID(ctx context.Context) uuid.UUID { return taskFromContext(ctx).WorkflowID }

// TaskKey is the task's key within its workflow DAG ("comp:<key>" for a
// compensation task). Empty outside a task.
func TaskKey(ctx context.Context) string { return taskFromContext(ctx).TaskKey }

// Attempt is the 1-based execution attempt; the first run is attempt 1. A
// NotReady re-check does not advance it. Zero outside a task.
func Attempt(ctx context.Context) int { return taskFromContext(ctx).Attempt }

// MaxAttempts is the resolved retry budget for the task. Zero outside a task.
func MaxAttempts(ctx context.Context) int { return taskFromContext(ctx).MaxAttempts }

// IsRetry reports whether this is a re-execution (Attempt > 1). False outside
// a task.
func IsRetry(ctx context.Context) bool { return taskFromContext(ctx).Attempt > 1 }

// Metadata returns the workflow's string-valued annotations (definition
// WithMeta plus run WithRunMeta). Nil outside a task.
func Metadata(ctx context.Context) map[string]string { return taskFromContext(ctx).Meta }

// resultResolver fetches the persisted results of the current workflow's
// succeeded tasks, once per handler invocation: the first ResultOf call loads
// every available result in one store round-trip and later calls read the
// cache.
type resultResolver struct {
	fetch func(ctx context.Context) (map[string]json.RawMessage, error)

	once    sync.Once
	results map[string]json.RawMessage
	err     error
}

// resolve returns the raw result for key, whether the key had one, and any
// fetch error.
func (r *resultResolver) resolve(ctx context.Context, key string) (json.RawMessage, bool, error) {
	r.once.Do(func() { r.results, r.err = r.fetch(ctx) })
	if r.err != nil {
		return nil, false, r.err
	}
	raw, ok := r.results[key]
	return raw, ok, nil
}

// withResolver returns a copy of parent carrying the resolver ResultOf reads.
func withResolver(parent context.Context, r *resultResolver) context.Context {
	return context.WithValue(parent, resolverKey{}, r)
}

// newResultResolver builds the per-invocation resolver over the store's
// persisted task results of one workflow.
func newResultResolver(store driver.WorkflowStore, workflowID uuid.UUID) *resultResolver {
	return &resultResolver{
		fetch: func(ctx context.Context) (map[string]json.RawMessage, error) {
			return store.TaskResults(ctx, workflowID, nil)
		},
	}
}

// ResultOf returns the persisted result of the workflow task key, decoded into
// T — the way a task reads the output of a dependency it declared with After
// (a dependency is guaranteed succeeded before its dependents run). For a
// WaitSignal task the result is the signal payload.
//
// The three failure modes are distinguishable: a context without a resolver
// (built by NewContext instead of a worker), a key with no persisted result
// (absent from the workflow or not succeeded), and a result that does not
// decode into T. A task that succeeded without producing a result (a Sleep, or
// a handler returning None) yields T's zero value with a nil error.
func ResultOf[T any](ctx context.Context, key string) (T, error) {
	var out T
	r, ok := ctx.Value(resolverKey{}).(*resultResolver)
	if !ok {
		return out, fmt.Errorf("workflow: ResultOf(%q): no result resolver in context "+
			"(the context did not come from a workflow worker)", key)
	}
	raw, found, err := r.resolve(ctx, key)
	if err != nil {
		return out, fmt.Errorf("workflow: ResultOf(%q): fetch task results: %w", key, err)
	}
	if !found {
		return out, fmt.Errorf("workflow: ResultOf(%q): no persisted result "+
			"(the task is not part of this workflow or has not succeeded)", key)
	}
	if raw == nil {
		return out, nil // succeeded without a result (Sleep, or a None handler)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("workflow: ResultOf(%q): unmarshal result: %w", key, err)
	}
	return out, nil
}
