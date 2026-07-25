package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/workflow/kernel"

	"github.com/google/uuid"
)

// operationJobPayload is the durable body of a leased Operation task job.
// JSON keys are snake_case — stable wire format for azync_jobs payloads.
//
//nolint:tagliatelle // durable wire format
type operationJobPayload struct {
	Name         string          `json:"name"`
	Version      string          `json:"version"`
	Input        json.RawMessage `json:"input"`
	ExecutionKey string          `json:"execution_key"`
	WorkflowName string          `json:"workflow_name"`
	ScheduledSeq int64           `json:"scheduled_seq"`
}

// operationScheduledPayload is recorded in history when an Operation is
// scheduled (before the leased job runs).
//
//nolint:tagliatelle // durable wire format
type operationScheduledPayload struct {
	Name         string          `json:"name"`
	Version      string          `json:"version"`
	JobID        string          `json:"job_id"`
	ExecutionKey string          `json:"execution_key"`
	Input        json.RawMessage `json:"input,omitempty"`
}

// operationResultPayload is the durable encoding of OperationCompleted /
// OperationFailed.
//
//nolint:tagliatelle // durable wire format
type operationResultPayload struct {
	Name         string          `json:"name"`
	Version      string          `json:"version"`
	ExecutionKey string          `json:"execution_key,omitempty"`
	Result       json.RawMessage `json:"result,omitempty"`
	Error        string          `json:"error,omitempty"`
}

// operationTaskKind is the fetch partition for a registered Operation.
func operationTaskKind(name, version string) string {
	return "$op:" + registrationKey(name, version)
}

// ExecuteOperation schedules a leased Operation (or replays its recorded
// outcome). While the Operation is in flight it parks the workflow task;
// the Operation executor appends the terminal history event and wakes a
// fresh workflow-task job. Side effects belong only in RegisterOperation
// handlers (docs/workflow-v1-spec.md §4, §7–8).
func ExecuteOperation(ctx Context, name, version string, input any) Future {
	c := asInternal(ctx)

	if ev, ok := c.cursor.Peek(); ok {
		switch ev.Type {
		case kernel.EventOperationCompleted:
			c.cursor.Next()
			return decodeOperationResult(name, ev)
		case kernel.EventOperationFailed:
			c.cursor.Next()
			return decodeOperationResult(name, ev)
		case kernel.EventOperationScheduled:
			c.cursor.Next()
			if ev2, ok2 := c.cursor.Peek(); ok2 {
				switch ev2.Type {
				case kernel.EventOperationCompleted, kernel.EventOperationFailed:
					c.cursor.Next()
					return decodeOperationResult(name, ev2)
				default:
					// Other history events mean the Operation is still in flight.
				}
			}
			// Still in flight (or uncertain): park until the executor / Manager wakes us.
			c.park(parkInfo{})
			panic("workflow: unreachable: park always unwinds via panic")
		default:
			panic(fmt.Errorf("workflow: expected Operation event, got %s at seq %d", ev.Type, ev.Seq))
		}
	}

	payload, err := json.Marshal(input)
	if err != nil {
		err = fmt.Errorf("workflow: marshal operation %q input: %w", name, err)
		c.record(kernel.EventOperationFailed, marshalOrPanic(operationResultPayload{
			Name: name, Version: version, Error: err.Error(),
		}))
		return failedFuture(err)
	}

	if _, registered := c.operations[registrationKey(name, version)]; !registered {
		err := unknownOperationError(name, version)
		c.record(kernel.EventOperationFailed, marshalOrPanic(operationResultPayload{
			Name: name, Version: version, Error: err.Error(),
		}))
		return failedFuture(err)
	}

	// NextSeq before append is the EventSeq that identifies this command.
	execKey := fmt.Sprintf("%s:%d", c.workflowID.String(), c.history.NextSeq())
	jobBody, err := json.Marshal(operationJobPayload{
		Name: name, Version: version, Input: payload,
		ExecutionKey: execKey, WorkflowName: c.workflowName,
	})
	if err != nil {
		panic(fmt.Errorf("workflow: marshal operation job: %w", err))
	}

	jobID, err := c.store.ScheduleOperation(c.ctx, driver.ScheduleOperationParams{
		WorkflowID:   c.workflowID,
		Kind:         operationTaskKind(name, version),
		Payload:      jobBody,
		ExecutionKey: execKey,
		MaxAttempts:  c.opMaxAttempts,
		Meta: map[string]string{
			"execution_key": execKey,
			"workflow_name": c.workflowName,
			"op_name":       name,
			"op_version":    version,
		},
	})
	if err != nil {
		panic(fmt.Errorf("workflow: schedule operation %q: %w", name, err))
	}

	c.record(kernel.EventOperationScheduled, marshalOrPanic(operationScheduledPayload{
		Name: name, Version: version, JobID: jobID.String(),
		ExecutionKey: execKey, Input: payload,
	}))
	c.park(parkInfo{})
	panic("workflow: unreachable: park always unwinds via panic")
}

func decodeOperationResult(name string, ev kernel.Event) Future {
	var rp operationResultPayload
	if err := json.Unmarshal(ev.Payload, &rp); err != nil {
		panic(fmt.Errorf("workflow: decode recorded operation %q result: %w", name, err))
	}
	if rp.Error != "" || ev.Type == kernel.EventOperationFailed {
		if rp.Error == "" {
			rp.Error = "operation failed"
		}
		return failedFuture(errors.New(rp.Error))
	}
	return readyFuture(rp.Result)
}

// marshalOrPanic marshals v, panicking on error: every caller passes one of
// this package's own small payload structs, which always marshal.
func marshalOrPanic(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Errorf("workflow: marshal %T: %w", v, err))
	}
	return b
}

// processOperationJob runs one leased Operation task to a durable outcome.
func (w *Worker) processOperationJob(ctx context.Context, job driver.Job) error {
	var body operationJobPayload
	if err := json.Unmarshal(job.Payload, &body); err != nil {
		return w.jobs.Dead(ctx, job.ID, job.LeaseToken, "workflow: corrupt operation payload")
	}
	fn, ok := w.lookupOperation(body.Name, body.Version)
	if !ok {
		reason := unknownOperationError(body.Name, body.Version).Error()
		w.appendOpFailed(ctx, job.RunID, body, reason)
		_ = w.jobs.Dead(ctx, job.ID, job.LeaseToken, reason)
		return w.wakeWorkflow(ctx, body.WorkflowName, job.RunID)
	}

	opCtx, cancel := context.WithCancel(withExecutionKey(ctx, body.ExecutionKey))
	defer cancel()
	if w.cfg.operationTimeout > 0 {
		var timeoutCancel context.CancelFunc
		opCtx, timeoutCancel = context.WithTimeout(opCtx, w.cfg.operationTimeout)
		defer timeoutCancel()
	}

	// Heartbeat: renew the lease every LeaseTTL/2 so long Operations survive
	// the reaper; losing the lease cancels the handler (another worker may own it).
	hbDone := make(chan struct{})
	go w.renewOperationLease(opCtx, job.ID, job.LeaseToken, cancel, hbDone)
	defer close(hbDone)

	started := time.Now()
	result, runErr := fn(opCtx, body.Input)
	timedOut := errors.Is(opCtx.Err(), context.DeadlineExceeded)
	leaseLost := opCtx.Err() != nil && !timedOut && ctx.Err() == nil

	if leaseLost && !timedOut {
		// Fencing lost mid-flight: do not settle; the new owner will run it.
		w.logger.Warn("operation lease lost, abandoning settlement",
			"job", job.ID.String(), "workflow_id", job.RunID.String())
		return nil
	}

	if timedOut && w.cfg.operationTimeout > 0 && time.Since(started) >= w.cfg.operationTimeout {
		reason := "operation: start-to-close timeout (uncertain)"
		if err := w.store.MarkUncertain(ctx, job.ID, job.LeaseToken, reason); err != nil {
			if driver.IsNotFound(err) {
				return nil
			}
			return w.safeRelease(ctx, job)
		}
		return nil
	}
	if runErr != nil && IsUncertain(runErr) {
		if err := w.store.MarkUncertain(ctx, job.ID, job.LeaseToken, runErr.Error()); err != nil {
			if driver.IsNotFound(err) {
				return nil
			}
			return w.safeRelease(ctx, job)
		}
		return nil
	}

	if runErr != nil {
		if job.Attempt < job.MaxAttempts {
			err := w.jobs.Reschedule(ctx, job.ID, job.LeaseToken, w.cfg.operationRetryDelay, runErr.Error())
			if driver.IsNotFound(err) {
				return nil
			}
			return err
		}
		w.appendOpFailed(ctx, job.RunID, body, runErr.Error())
		if err := w.jobs.Dead(ctx, job.ID, job.LeaseToken, runErr.Error()); err != nil {
			if driver.IsNotFound(err) {
				return nil
			}
			return err
		}
		return w.wakeWorkflow(ctx, body.WorkflowName, job.RunID)
	}

	rp := operationResultPayload{
		Name: body.Name, Version: body.Version,
		ExecutionKey: body.ExecutionKey, Result: result,
	}
	if _, err := w.store.AppendHistory(ctx, job.RunID, string(kernel.EventOperationCompleted), marshalOrPanic(rp)); err != nil {
		return w.safeRelease(ctx, job)
	}
	if err := w.jobs.Ack(ctx, job.ID, job.LeaseToken); err != nil {
		if driver.IsNotFound(err) {
			return nil
		}
		return err
	}
	return w.wakeWorkflow(ctx, body.WorkflowName, job.RunID)
}

// renewOperationLease extends the Operation job lease every LeaseTTL/2 until
// done is closed or ctx ends. A failed ExtendLease cancels the handler ctx.
func (w *Worker) renewOperationLease(ctx context.Context, id, leaseToken uuid.UUID, cancel context.CancelFunc, done <-chan struct{}) {
	interval := w.cfg.leaseTTL / 2
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.jobs.ExtendLease(ctx, id, leaseToken, w.cfg.leaseTTL); err != nil {
				w.logger.Warn("operation lease renew failed, cancelling handler",
					"job", id.String(), "error", err)
				cancel()
				return
			}
		}
	}
}

func (w *Worker) safeRelease(ctx context.Context, job driver.Job) error {
	err := w.jobs.Release(ctx, job.ID, job.LeaseToken)
	if driver.IsNotFound(err) {
		return nil
	}
	return err
}

func (w *Worker) appendOpFailed(ctx context.Context, workflowID uuid.UUID, body operationJobPayload, reason string) {
	rp := operationResultPayload{
		Name: body.Name, Version: body.Version,
		ExecutionKey: body.ExecutionKey, Error: reason,
	}
	if _, err := w.store.AppendHistory(ctx, workflowID, string(kernel.EventOperationFailed), marshalOrPanic(rp)); err != nil {
		w.logger.Error("workflow: append OperationFailed", "error", err)
	}
}

func (w *Worker) wakeWorkflow(ctx context.Context, workflowName string, workflowID uuid.UUID) error {
	if workflowName == "" {
		view, err := w.store.GetWorkflowExecution(ctx, workflowID)
		if err != nil {
			return err
		}
		workflowName = view.Name
	}
	return enqueueWorkflowTask(ctx, w.store, workflowName, workflowID, time.Time{})
}

func (w *Worker) lookupOperation(name, version string) (operationFunc, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	fn, ok := w.operations[registrationKey(name, version)]
	return fn, ok
}

func (w *Worker) registeredOperationKinds() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	kinds := make([]string, 0, len(w.operations))
	for key := range w.operations {
		kinds = append(kinds, "$op:"+key)
	}
	return kinds
}
