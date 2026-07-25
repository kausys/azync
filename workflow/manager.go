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

// Manager administers workflow executions outside the deterministic replay
// path: read, cancel and resolve an uncertain Operation.
type Manager struct {
	store driver.WorkflowStore
	jobs  driver.Store
}

// View is the admin projection of one execution.
type View = driver.WorkflowExecutionView

// Get returns one execution's current header, or a not-found error (see
// IsNotFound) when it does not exist.
func (m *Manager) Get(ctx context.Context, id uuid.UUID) (View, error) {
	return m.store.GetWorkflowExecution(ctx, id)
}

// Cancel administratively cancels a non-terminal execution (see
// docs/workflow-v1-spec.md §15: no programmable cleanup hooks or automatic
// compensation in the MVP). It returns a not-found error when the execution
// is absent or already terminal.
func (m *Manager) Cancel(ctx context.Context, id uuid.UUID) error {
	return m.store.CancelWorkflowExecution(ctx, id)
}

// ResolveUncertain applies an audited decision to an Operation in the
// uncertain state (docs/workflow-v1-spec.md §8):
//
//   - "complete" — record OperationCompleted with result and wake replay
//   - "fail" — record OperationFailed and wake replay
//   - "retry" — re-queue the Operation job without a history outcome
func (m *Manager) ResolveUncertain(ctx context.Context, operationJobID uuid.UUID, decision string, result json.RawMessage) error {
	job, err := m.jobs.GetJob(ctx, driver.SourceWorkflow, operationJobID)
	if err != nil {
		return err
	}
	if job.State != driver.StateUncertain {
		return driver.NewNotFound("resolve uncertain")
	}
	name := job.Meta["op_name"]
	version := job.Meta["op_version"]
	execKey := job.Meta["execution_key"]

	workflowID, workflowName, err := m.store.ResolveUncertain(ctx, operationJobID, decision, result)
	if err != nil {
		return err
	}
	if workflowName == "" {
		workflowName = job.Meta["workflow_name"]
	}

	switch driver.UncertainDecision(decision) {
	case driver.UncertainComplete:
		rp := operationResultPayload{
			Name: name, Version: version, ExecutionKey: execKey, Result: result,
		}
		if _, err := m.store.AppendHistory(ctx, workflowID, string(kernel.EventOperationCompleted), marshalOrPanic(rp)); err != nil {
			return fmt.Errorf("workflow: append OperationCompleted after resolve: %w", err)
		}
		return enqueueWorkflowTask(ctx, m.store, workflowName, workflowID, time.Time{})
	case driver.UncertainFail:
		errMsg := "uncertain: fail"
		if len(result) > 0 {
			var probe struct {
				Error string `json:"error"`
			}
			if json.Unmarshal(result, &probe) == nil && probe.Error != "" {
				errMsg = probe.Error
			}
		}
		rp := operationResultPayload{
			Name: name, Version: version, ExecutionKey: execKey, Error: errMsg,
		}
		if _, err := m.store.AppendHistory(ctx, workflowID, string(kernel.EventOperationFailed), marshalOrPanic(rp)); err != nil {
			return fmt.Errorf("workflow: append OperationFailed after resolve: %w", err)
		}
		return enqueueWorkflowTask(ctx, m.store, workflowName, workflowID, time.Time{})
	case driver.UncertainRetry:
		return nil
	default:
		return fmt.Errorf("workflow: unknown uncertain decision %q", decision)
	}
}
