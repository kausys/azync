package drivertest

// The workflow-as-code capability of the Fake: a complete in-memory
// implementation of driver.WorkflowStore honoring the contract's semantics —
// live business-key dedupe, monotonic history append, and MessageID-deduped
// signal delivery. It is the behavioral oracle the workflow runtime is
// developed and conformance-tested against, mirroring fake_dag.go for the
// static-DAG capability.
//
// Every method takes f.mu for its whole critical section, matching the
// single-statement atomicity of the SQL driver.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// fakeExecution is the internal workflow-as-code execution record: the wire
// view plus its append-only history.
type fakeExecution struct {
	driver.WorkflowExecutionView
	history []driver.HistoryEvent
}

// signalDedupeKey identifies one delivered signal message for the MessageID
// dedupe rule; MessageID empty means "no dedupe" and is never used as a key
// (each such Signal is a distinct message).
type signalDedupeKey struct {
	workflowID uuid.UUID
	name       string
	messageID  string
}

// terminal reports whether the execution reached a final state.
func (e *fakeExecution) terminal() bool {
	switch e.State {
	case driver.WorkflowSucceeded, driver.WorkflowFailed, driver.WorkflowCancelled:
		return true
	default:
		return false
	}
}

// live reports whether the execution still holds its business idempotency
// key (running or suspended).
func (e *fakeExecution) live() bool {
	return e.State == driver.WorkflowRunning || e.State == driver.WorkflowSuspended
}

// cloneView projects the execution header defensively. Callers hold f.mu.
func (e *fakeExecution) cloneView() driver.WorkflowExecutionView {
	out := e.WorkflowExecutionView
	out.Input = clonePayload(e.Input)
	out.Result = clonePayload(e.Result)
	out.Meta = cloneMeta(e.Meta)
	return out
}

// workflowTaskKindFmt mirrors workflow/worker.go's unexported
// workflowTaskKind: the fetch partition a workflow's tasks route through.
// Duplicated here (rather than importing the workflow package, which would
// violate the runtime-isolation boundary driver-adjacent packages keep) as a
// stable wire-format constant — like the "WorkflowStarted"/"SignalReceived"
// event-type string literals below, changing it would break every already
// -scheduled task job.
func workflowTaskKindFmt(name string) string { return "$wf:" + name }

// fakeSignalPayload mirrors workflow/signal.go's unexported
// signalEventPayload wire format, for the same reason as workflowTaskKindFmt.
type fakeSignalPayload struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// StartWorkflow atomically inserts one workflow-as-code execution header,
// deduplicating by (Name, BusinessIdempotencyKey) against live (running or
// suspended) executions, and — for a newly inserted execution — records the
// WorkflowStarted history event and schedules the first workflow-task job in
// the same critical section, matching azyncpgx's single-transaction
// contract: a caller never observes an execution with no history or task.
func (f *Fake) StartWorkflow(_ context.Context, p driver.WorkflowStartParams) (bool, uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()

	if p.BusinessIdempotencyKey != "" {
		for _, e := range f.executions {
			if e.Name == p.Name && e.BusinessIdempotencyKey == p.BusinessIdempotencyKey && e.live() {
				return false, e.ID, nil
			}
		}
	}

	if _, exists := f.executions[p.ID]; exists {
		return false, uuid.Nil, fmt.Errorf("drivertest: workflow id %s already exists", p.ID)
	}

	taskQueue := p.TaskQueue
	if taskQueue == "" {
		taskQueue = "default"
	}
	e := &fakeExecution{
		WorkflowExecutionView: driver.WorkflowExecutionView{
			ID:                     p.ID,
			Name:                   p.Name,
			Version:                p.Version,
			State:                  driver.WorkflowRunning,
			BusinessIdempotencyKey: p.BusinessIdempotencyKey,
			TaskQueue:              taskQueue,
			Input:                  clonePayload(p.Input),
			Meta:                   cloneMeta(p.Meta),
			CreatedAt:              now,
			UpdatedAt:              now,
		},
	}
	e.history = append(e.history, driver.HistoryEvent{
		WorkflowID: p.ID, Seq: 1, Type: "WorkflowStarted", Payload: clonePayload(p.Input), CreatedAt: now,
	})
	f.executions[p.ID] = e
	f.scheduleTaskLocked(p.ID, workflowTaskKindFmt(p.Name), time.Time{})
	return true, uuid.Nil, nil
}

// GetWorkflowExecution returns one execution header by id, or a not-found
// error when it does not exist.
func (f *Fake) GetWorkflowExecution(_ context.Context, id uuid.UUID) (driver.WorkflowExecutionView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.executions[id]
	if e == nil {
		return driver.WorkflowExecutionView{}, driver.NewNotFound("get workflow execution")
	}
	return e.cloneView(), nil
}

// AppendHistory appends one durable history record with the next monotonic
// sequence number for the workflow.
// operationResultTypes are the two history event types
// azync_workflow_history_exec_key_idx (migration 00007) dedupes by
// execution_key, mirrored here so the fake matches the SQL driver's
// idempotent AppendHistory.
var operationResultTypes = map[string]bool{"OperationCompleted": true, "OperationFailed": true}

// executionKeyFromPayload extracts the execution_key field from a history
// payload, mirroring azyncpgx's helper of the same name.
func executionKeyFromPayload(payload json.RawMessage) (string, bool) {
	if len(payload) == 0 {
		return "", false
	}
	var v struct {
		//nolint:tagliatelle // durable wire format, must match workflow/operation.go
		ExecutionKey string `json:"execution_key"`
	}
	if err := json.Unmarshal(payload, &v); err != nil || v.ExecutionKey == "" {
		return "", false
	}
	return v.ExecutionKey, true
}

// AppendHistory appends one durable history record. For an
// OperationCompleted/OperationFailed payload carrying an execution_key
// already recorded, it is idempotent: it returns the existing record's seq
// instead of appending a duplicate — closing the crash window between
// AppendHistory and the operation job's Ack (processOperationJob).
func (f *Fake) AppendHistory(_ context.Context, workflowID uuid.UUID, typ string, payload json.RawMessage) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.executions[workflowID]
	if e == nil {
		return 0, driver.NewNotFound("append history")
	}
	if operationResultTypes[typ] {
		if key, ok := executionKeyFromPayload(payload); ok {
			for _, ev := range e.history {
				if ev.Type != typ {
					continue
				}
				if existingKey, ok2 := executionKeyFromPayload(ev.Payload); ok2 && existingKey == key {
					return ev.Seq, nil
				}
			}
		}
	}
	var seq int64 = 1
	if n := len(e.history); n > 0 {
		seq = e.history[n-1].Seq + 1
	}
	e.history = append(e.history, driver.HistoryEvent{
		WorkflowID: workflowID,
		Seq:        seq,
		Type:       typ,
		Payload:    clonePayload(payload),
		CreatedAt:  f.now(),
	})
	return seq, nil
}

// ListHistory returns the workflow's history events in sequence order.
func (f *Fake) ListHistory(_ context.Context, workflowID uuid.UUID) ([]driver.HistoryEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.executions[workflowID]
	if e == nil {
		return nil, driver.NewNotFound("list history")
	}
	out := make([]driver.HistoryEvent, len(e.history))
	for i, ev := range e.history {
		out[i] = ev
		out[i].Payload = clonePayload(ev.Payload)
	}
	return out, nil
}

// SignalWorkflow appends an early signal to the inbox, deduplicating by
// (WorkflowID, Name, MessageID) when MessageID is set; an empty MessageID
// disables dedupe, so every such Signal is a distinct message.
// SignalWorkflow atomically appends the delivery to the inbox (deduped by
// MessageID), the SignalReceived history record, and — when the execution is
// not already terminal — a wake workflow-task job, in the same critical
// section. Matches azyncpgx's single-transaction contract: a newly delivered
// signal is never observed recorded without also being able to wake a
// parked replay (the pre-fix race this closes: a crash between the inbox
// insert and the wake-task schedule left a signal "delivered" with nothing
// to ever act on it).
func (f *Fake) SignalWorkflow(_ context.Context, p driver.SignalParams) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.executions[p.WorkflowID]
	if e == nil {
		return false, driver.NewNotFound("signal workflow")
	}
	if p.MessageID != "" {
		key := signalDedupeKey{workflowID: p.WorkflowID, name: p.Name, messageID: p.MessageID}
		if f.signalKeys[key] {
			return false, nil
		}
		f.signalKeys[key] = true
	}

	seq := int64(1)
	if n := len(e.history); n > 0 {
		seq = e.history[n-1].Seq + 1
	}
	payload, err := json.Marshal(fakeSignalPayload{Name: p.Name, Payload: p.Payload})
	if err != nil {
		return false, fmt.Errorf("drivertest: marshal signal payload: %w", err)
	}
	e.history = append(e.history, driver.HistoryEvent{
		WorkflowID: p.WorkflowID, Seq: seq, Type: "SignalReceived", Payload: payload, CreatedAt: f.now(),
	})
	if !e.terminal() {
		f.scheduleTaskLocked(p.WorkflowID, workflowTaskKindFmt(e.Name), time.Time{})
	}
	return true, nil
}

// CompleteWorkflow settles a workflow as succeeded, persisting the result.
func (f *Fake) CompleteWorkflow(_ context.Context, id uuid.UUID, result json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.executions[id]
	if e == nil {
		return driver.NewNotFound("complete workflow")
	}
	now := f.now()
	e.State = driver.WorkflowSucceeded
	e.Result = clonePayload(result)
	e.UpdatedAt = now
	e.CompletedAt = now
	return nil
}

// FailWorkflow settles a workflow as failed, recording the reason.
func (f *Fake) FailWorkflow(_ context.Context, id uuid.UUID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.executions[id]
	if e == nil {
		return driver.NewNotFound("fail workflow")
	}
	now := f.now()
	e.State = driver.WorkflowFailed
	e.FailureReason = reason
	e.UpdatedAt = now
	e.CompletedAt = now
	return nil
}

// CancelWorkflowExecution settles a non-terminal workflow as cancelled.
func (f *Fake) CancelWorkflowExecution(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.executions[id]
	if e == nil || e.terminal() {
		return driver.NewNotFound("cancel workflow execution")
	}
	now := f.now()
	e.State = driver.WorkflowCancelled
	e.UpdatedAt = now
	e.CompletedAt = now
	return nil
}

// SuspendWorkflow parks a running workflow for a manual decision.
func (f *Fake) SuspendWorkflow(_ context.Context, id uuid.UUID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.executions[id]
	if e == nil || e.terminal() {
		return driver.NewNotFound("suspend workflow")
	}
	e.State = driver.WorkflowSuspended
	e.FailureReason = reason
	e.UpdatedAt = f.now()
	return nil
}

// ResumeWorkflow moves a suspended execution back to running.
func (f *Fake) ResumeWorkflow(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.executions[id]
	if e == nil || e.State != driver.WorkflowSuspended {
		return driver.NewNotFound("resume workflow")
	}
	e.State = driver.WorkflowRunning
	e.FailureReason = ""
	e.UpdatedAt = f.now()
	return nil
}

// ScheduleOperation inserts one Operation task job, deduping by ExecutionKey
// among non-terminal workflow jobs.
func (f *Fake) ScheduleOperation(_ context.Context, p driver.ScheduleOperationParams) (uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.executions[p.WorkflowID]; !ok {
		return uuid.Nil, driver.NewNotFound("schedule operation")
	}
	if p.ExecutionKey != "" {
		for _, j := range f.jobs {
			if j.Source != driver.SourceWorkflow || j.RunID != p.WorkflowID {
				continue
			}
			if j.Meta["execution_key"] != p.ExecutionKey {
				continue
			}
			switch j.State {
			case driver.StatePending, driver.StateScheduled, driver.StateActive, driver.StateUncertain:
				return j.ID, nil
			default:
				// Terminal / non-live states do not satisfy the live-key dedupe.
			}
		}
	}
	now := f.now()
	runAt := p.RunAt
	if runAt.IsZero() {
		runAt = now
	}
	state := driver.StatePending
	if runAt.After(now) {
		state = driver.StateScheduled
	}
	maxAttempts := p.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 25
	}
	meta := cloneMeta(p.Meta)
	if meta == nil {
		meta = map[string]string{}
	}
	if p.ExecutionKey != "" {
		meta["execution_key"] = p.ExecutionKey
	}
	id := uuid.New()
	f.jobs[id] = &fakeJob{
		Job: driver.Job{
			ID:          id,
			Source:      driver.SourceWorkflow,
			Kind:        p.Kind,
			State:       state,
			RunID:       p.WorkflowID,
			Payload:     clonePayload(p.Payload),
			Meta:        meta,
			RunAt:       runAt,
			MaxAttempts: maxAttempts,
			EnqueuedAt:  now,
		},
		maxAttemptsExplicit: true,
		seq:                 f.nextSeq(),
	}
	f.bumpStat(driver.SourceWorkflow, p.Kind, statEnqueued, 1, now)
	f.changeJob(f.jobs[id])
	if state == driver.StatePending {
		f.wake(driver.SourceWorkflow, p.Kind)
	}
	return id, nil
}

// MarkUncertain moves an active Operation to StateUncertain and suspends the
// parent execution.
func (f *Fake) MarkUncertain(_ context.Context, operationJobID, leaseToken uuid.UUID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, err := f.settle("mark uncertain", operationJobID, leaseToken)
	if err != nil {
		return err
	}
	if j.Source != driver.SourceWorkflow {
		return driver.NewNotFound("mark uncertain")
	}
	now := f.now()
	j.State = driver.StateUncertain
	j.LeaseToken = uuid.Nil
	j.LeaseUntil = time.Time{}
	j.LastError = reason
	j.FailedAt = now
	f.changeJob(j)
	e := f.executions[j.RunID]
	if e != nil && !e.terminal() {
		e.State = driver.WorkflowSuspended
		e.FailureReason = reason
		e.UpdatedAt = now
	}
	return nil
}

// ResolveUncertain applies complete/fail/retry to an uncertain Operation.
// History append is the caller's responsibility.
func (f *Fake) ResolveUncertain(_ context.Context, operationJobID uuid.UUID, decision string, result json.RawMessage) (uuid.UUID, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j := f.jobs[operationJobID]
	if j == nil || j.Source != driver.SourceWorkflow || j.State != driver.StateUncertain {
		return uuid.Nil, "", driver.NewNotFound("resolve uncertain")
	}
	e := f.executions[j.RunID]
	if e == nil {
		return uuid.Nil, "", driver.NewNotFound("resolve uncertain")
	}
	now := f.now()
	workflowName := j.Meta["workflow_name"]
	switch driver.UncertainDecision(decision) {
	case driver.UncertainComplete:
		j.State = driver.StateSucceeded
		j.CompletedAt = now
		j.Result = clonePayload(result)
	case driver.UncertainFail:
		j.State = driver.StateDead
		j.LastError = "uncertain: fail"
		j.FailedAt = now
	case driver.UncertainRetry:
		j.State = driver.StatePending
		j.RunAt = now
		j.LastError = ""
		f.wake(driver.SourceWorkflow, j.Kind)
	default:
		return uuid.Nil, "", fmt.Errorf("drivertest: unknown uncertain decision %q", decision)
	}
	f.changeJob(j)
	e.State = driver.WorkflowRunning
	e.FailureReason = ""
	e.UpdatedAt = now
	return j.RunID, workflowName, nil
}

// ScheduleTask durably inserts one workflow-task job (Source
// SourceWorkflow, RunID = workflowID), born pending when runAt is due,
// scheduled otherwise — the same split Enqueue applies to queue jobs.
func (f *Fake) ScheduleTask(_ context.Context, workflowID uuid.UUID, kind string, runAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.executions[workflowID]; !ok {
		return driver.NewNotFound("schedule workflow task")
	}
	f.scheduleTaskLocked(workflowID, kind, runAt)
	return nil
}

// scheduleTaskLocked is ScheduleTask's body, callable while f.mu is already
// held — by StartWorkflow and SignalWorkflow, which bundle the first/wake
// task atomically with their own effect (see their doc comments).
func (f *Fake) scheduleTaskLocked(workflowID uuid.UUID, kind string, runAt time.Time) {
	now := f.now()
	if runAt.IsZero() {
		runAt = now
	}
	state := driver.StatePending
	if runAt.After(now) {
		state = driver.StateScheduled
	}
	id := uuid.New()
	f.jobs[id] = &fakeJob{
		Job: driver.Job{
			ID:         id,
			Source:     driver.SourceWorkflow,
			Kind:       kind,
			State:      state,
			RunID:      workflowID,
			RunAt:      runAt,
			EnqueuedAt: now,
		},
		seq: f.nextSeq(),
	}
	f.bumpStat(driver.SourceWorkflow, kind, statEnqueued, 1, now)
	f.changeJob(f.jobs[id])
	if state == driver.StatePending {
		f.wake(driver.SourceWorkflow, kind)
	}
}

// VacuumWorkflows deletes terminal workflow-as-code executions completed
// before retention ago, cascading to their jobs and history.
func (f *Fake) VacuumWorkflows(_ context.Context, retention time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if retention <= 0 {
		return 0, nil
	}
	cutoff := f.now().Add(-retention)
	var removed int64
	for id, e := range f.executions {
		if !e.terminal() || e.CompletedAt.IsZero() || !e.CompletedAt.Before(cutoff) {
			continue
		}
		for jobID, j := range f.jobs {
			if j.Source == driver.SourceWorkflow && j.RunID == id {
				delete(f.jobs, jobID)
				delete(f.attempts, jobID)
			}
		}
		delete(f.executions, id)
		removed++
	}
	return removed, nil
}

// WorkflowExecutionCount returns the number of workflow-as-code executions
// currently held, for tests that assert on vacuum-style cleanup.
func (f *Fake) WorkflowExecutionCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.executions)
}

// ListStalledWorkflows returns up to limit running executions with no live
// (pending, scheduled, active or uncertain) source=workflow job, updated at
// least olderThan ago. The cutoff is inclusive (After, not !Before): on
// coarse clocks (Windows ticks at up to ~15.6ms) an execution created and
// checked within one tick has UpdatedAt == cutoff at olderThan 0, and a
// strict comparison would hide a genuinely stranded execution.
func (f *Fake) ListStalledWorkflows(_ context.Context, olderThan time.Duration, limit int) ([]driver.StalledWorkflow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	cutoff := f.now().Add(-olderThan)
	var out []driver.StalledWorkflow
	for _, e := range f.executions {
		if e.State != driver.WorkflowRunning || e.UpdatedAt.After(cutoff) {
			continue
		}
		if f.hasLiveWorkflowJobLocked(e.ID) {
			continue
		}
		out = append(out, driver.StalledWorkflow{ID: e.ID, Name: e.Name})
	}
	sort.Slice(out, func(i, j int) bool {
		ei, ej := f.executions[out[i].ID], f.executions[out[j].ID]
		if !ei.UpdatedAt.Equal(ej.UpdatedAt) {
			return ei.UpdatedAt.Before(ej.UpdatedAt)
		}
		return out[i].ID.String() < out[j].ID.String()
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// hasLiveWorkflowJobLocked reports whether workflowID has any non-terminal
// source=workflow job. Callers hold f.mu.
func (f *Fake) hasLiveWorkflowJobLocked(workflowID uuid.UUID) bool {
	for _, j := range f.jobs {
		if j.Source != driver.SourceWorkflow || j.RunID != workflowID {
			continue
		}
		switch j.State {
		case driver.StatePending, driver.StateScheduled, driver.StateActive, driver.StateUncertain:
			return true
		case driver.StateDead, driver.StatePaused, driver.StateSucceeded,
			driver.StateBlocked, driver.StateWaiting, driver.StateCancelled,
			driver.StateSkipped:
			// terminal or otherwise not a live workflow-task/Operation state
		}
	}
	return false
}
