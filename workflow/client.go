package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/workflow/kernel"

	"github.com/google/uuid"
)

// Client starts workflow executions and delivers signals to them.
type Client struct {
	store driver.WorkflowStore
}

// StartResult reports the outcome of Start.
type StartResult struct {
	// WorkflowID identifies the execution: the new one, or — when
	// Deduplicated is true — the live execution that already held the
	// business idempotency key.
	WorkflowID uuid.UUID
	// Deduplicated is true when a BusinessIdempotencyKey matched a live
	// execution and nothing was inserted.
	Deduplicated bool
}

type startOptions struct {
	workflowID  uuid.UUID
	businessKey string
	taskQueue   string
	meta        map[string]string
}

// StartOption customizes one Start call.
type StartOption func(*startOptions)

// WithBusinessIdempotencyKey deduplicates the start within name: while a
// workflow with the same (name, key) is live (running or suspended), Start
// inserts nothing and returns the live execution's WorkflowID with
// Deduplicated=true. A terminal execution never reuses its WorkflowID and
// frees the key for a fresh Start (see docs/workflow-v1-spec.md §2).
func WithBusinessIdempotencyKey(key string) StartOption {
	return func(o *startOptions) { o.businessKey = key }
}

// WithWorkflowID overrides the automatically assigned WorkflowID. The
// caller is responsible for its uniqueness.
func WithWorkflowID(id uuid.UUID) StartOption {
	return func(o *startOptions) { o.workflowID = id }
}

// WithTaskQueue routes this execution's workflow-task jobs through a named
// task queue instead of "default" (see docs/workflow-v1-spec.md §12). The
// MVP worker does not yet filter by task queue; it is recorded on the
// execution header for a later version to act on.
func WithTaskQueue(name string) StartOption {
	return func(o *startOptions) { o.taskQueue = name }
}

// WithStartMeta attaches one string-valued annotation to the execution
// header (repeatable).
func WithStartMeta(key, value string) StartOption {
	return func(o *startOptions) {
		if o.meta == nil {
			o.meta = map[string]string{}
		}
		o.meta[key] = value
	}
}

// Start creates a new execution of the workflow registered as (name,
// version), durably records its WorkflowStarted history event and enqueues
// its first workflow-task job, all before returning. input is marshaled to
// JSON and handed to the registered workflow function.
func (c *Client) Start(ctx context.Context, name, version string, input any, opts ...StartOption) (StartResult, error) {
	var o startOptions
	for _, opt := range opts {
		opt(&o)
	}
	id := o.workflowID
	if id == uuid.Nil {
		id = uuid.New()
	}

	payload, err := json.Marshal(input)
	if err != nil {
		return StartResult{}, fmt.Errorf("workflow: marshal %q input: %w", name, err)
	}

	inserted, existing, err := c.store.StartWorkflow(ctx, driver.WorkflowStartParams{
		ID:                     id,
		Name:                   name,
		Version:                version,
		Input:                  payload,
		BusinessIdempotencyKey: o.businessKey,
		TaskQueue:              o.taskQueue,
		Meta:                   o.meta,
	})
	if err != nil {
		return StartResult{}, err
	}
	if !inserted {
		return StartResult{WorkflowID: existing, Deduplicated: true}, nil
	}

	if _, err := c.store.AppendHistory(ctx, id, string(kernel.EventWorkflowStarted), payload); err != nil {
		return StartResult{}, fmt.Errorf("workflow: record %q start: %w", name, err)
	}
	if err := enqueueWorkflowTask(ctx, c.store, name, id, time.Time{}); err != nil {
		return StartResult{}, err
	}
	return StartResult{WorkflowID: id}, nil
}

type signalOptions struct {
	messageID string
}

// SignalOption customizes one Signal call.
type SignalOption func(*signalOptions)

// WithMessageID deduplicates the signal by (WorkflowID, name, MessageID):
// a repeated MessageID delivers nothing (Client.Signal returns
// delivered=false, nil error). Without it every Signal call is a distinct
// message (see docs/workflow-v1-spec.md §9).
func WithMessageID(id string) SignalOption {
	return func(o *signalOptions) { o.messageID = id }
}

// Signal delivers a named signal (with payload, marshaled to JSON) to one
// workflow execution: the delivery is recorded in the inbox (deduped by
// MessageID) and, when newly delivered, appended directly to the
// execution's history as SignalReceived and a fresh workflow-task job is
// enqueued to wake a parked WaitSignal / Select. Signals may arrive before
// any WaitSignal call ever runs ("early" signals) — the workflow's next
// replay pass finds them in history regardless of arrival order.
func (c *Client) Signal(ctx context.Context, workflowID uuid.UUID, name string, payload any, opts ...SignalOption) (bool, error) {
	var o signalOptions
	for _, opt := range opts {
		opt(&o)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("workflow: marshal signal %q payload: %w", name, err)
	}

	inserted, err := c.store.SignalWorkflow(ctx, driver.SignalParams{
		WorkflowID: workflowID, Name: name, MessageID: o.messageID, Payload: body,
	})
	if err != nil {
		return false, err
	}
	if !inserted {
		return false, nil
	}

	view, err := c.store.GetWorkflowExecution(ctx, workflowID)
	if err != nil {
		return false, err
	}
	if _, err := c.store.AppendHistory(ctx, workflowID, string(kernel.EventSignalReceived),
		marshalOrPanic(signalEventPayload{Name: name, Payload: body})); err != nil {
		return false, fmt.Errorf("workflow: record signal %q: %w", name, err)
	}
	if isTerminalWorkflowState(view.State) {
		return true, nil // recorded for the audit trail; nothing left to wake
	}
	if err := enqueueWorkflowTask(ctx, c.store, view.Name, workflowID, time.Time{}); err != nil {
		return false, err
	}
	return true, nil
}
