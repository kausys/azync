package workflow

import (
	"context"
	"encoding/json"
	"fmt"
)

// workflowFunc is the raw (JSON in, JSON out) shape every registered
// workflow function is reduced to; RegisterWorkflow wraps a typed handler
// into one.
type workflowFunc func(ctx Context, input json.RawMessage) (json.RawMessage, error)

// operationFunc is the raw (JSON in, JSON out) shape every registered
// Operation handler is reduced to; RegisterOperation wraps a typed handler
// into one. It takes the standard context.Context — an Operation is the
// only place workflow-as-code code may perform real I/O, so it is
// deliberately not handed the deterministic workflow.Context.
type operationFunc func(ctx context.Context, input json.RawMessage) (json.RawMessage, error)

// registrationKey identifies one (name, version) pair in the worker's
// workflow or operation registry.
func registrationKey(name, version string) string { return name + "@" + version }

// RegisterWorkflow binds a typed workflow function on w under (name,
// version). Workers refuse workflow-task jobs whose execution names a pair
// nothing registered (see ErrUnknownWorkflow). Call every RegisterWorkflow /
// RegisterOperation before Worker.Start.
func RegisterWorkflow[TIn, TOut any](w *Worker, name, version string, fn func(ctx Context, in TIn) (TOut, error)) {
	w.registerWorkflow(name, version, func(ctx Context, raw json.RawMessage) (json.RawMessage, error) {
		var in TIn
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &in); err != nil {
				return nil, fmt.Errorf("workflow: decode %q input: %w", name, err)
			}
		}
		out, err := fn(ctx, in)
		if err != nil {
			return nil, err
		}
		result, err := json.Marshal(out)
		if err != nil {
			return nil, fmt.Errorf("workflow: encode %q result: %w", name, err)
		}
		return result, nil
	})
}

// RegisterOperation binds a typed Operation handler on w under (name,
// version). An Operation is the only allowed external I/O in workflow-as-code
// (see docs/workflow-v1-spec.md §4); its handler receives a plain
// context.Context, never the deterministic workflow.Context.
func RegisterOperation[TIn, TOut any](w *Worker, name, version string, fn func(ctx context.Context, in TIn) (TOut, error)) {
	w.registerOperation(name, version, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var in TIn
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &in); err != nil {
				return nil, fmt.Errorf("workflow: decode operation %q input: %w", name, err)
			}
		}
		out, err := fn(ctx, in)
		if err != nil {
			return nil, err
		}
		result, err := json.Marshal(out)
		if err != nil {
			return nil, fmt.Errorf("workflow: encode operation %q result: %w", name, err)
		}
		return result, nil
	})
}

// registerWorkflow stores fn under (name, version), overwriting any prior
// registration for the same pair — Register* calls happen once at startup,
// before Start, so last-registration-wins needs no separate error path.
func (w *Worker) registerWorkflow(name, version string, fn workflowFunc) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.workflows[registrationKey(name, version)] = fn
}

func (w *Worker) registerOperation(name, version string, fn operationFunc) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.operations[registrationKey(name, version)] = fn
}
