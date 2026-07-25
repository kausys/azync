package driver

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// WorkflowState is the lifecycle of a workflow-as-code execution.
type WorkflowState string

const (
	WorkflowRunning   WorkflowState = "running"
	WorkflowSuspended WorkflowState = "suspended"
	WorkflowSucceeded WorkflowState = "succeeded"
	WorkflowFailed    WorkflowState = "failed"
	WorkflowCancelled WorkflowState = "cancelled"
)

// OperationState is the lifecycle of one Operation task.
type OperationState string

const (
	OperationScheduled OperationState = "scheduled"
	OperationActive    OperationState = "active"
	OperationRetryWait OperationState = "retry_wait"
	OperationCompleted OperationState = "completed"
	OperationFailed    OperationState = "failed"
	OperationCancelled OperationState = "cancelled"
	OperationUncertain OperationState = "uncertain"
)

// WorkflowStartParams creates a new workflow-as-code execution.
type WorkflowStartParams struct {
	ID                     uuid.UUID
	Name                   string
	Version                string
	Input                  json.RawMessage
	BusinessIdempotencyKey string
	TaskQueue              string
	Meta                   map[string]string
}

// WorkflowExecutionView is the admin projection of one execution.
type WorkflowExecutionView struct {
	ID                     uuid.UUID
	Name                   string
	Version                string
	State                  WorkflowState
	BusinessIdempotencyKey string
	TaskQueue              string
	Input                  json.RawMessage
	Result                 json.RawMessage
	FailureReason          string
	Meta                   map[string]string
	CreatedAt              time.Time
	UpdatedAt              time.Time
	CompletedAt            time.Time
}

// HistoryEvent is one durable history record.
type HistoryEvent struct {
	WorkflowID uuid.UUID
	Seq        int64
	Type       string
	Payload    json.RawMessage
	CreatedAt  time.Time
}

// SignalParams appends an early signal to the inbox.
type SignalParams struct {
	WorkflowID uuid.UUID
	Name       string
	MessageID  string // optional dedupe
	Payload    json.RawMessage
}

// ScheduleOperationParams inserts one leased Operation task job
// (source=workflow) for a workflow execution.
type ScheduleOperationParams struct {
	WorkflowID uuid.UUID
	// Kind is the fetch partition (e.g. "$op:name@version").
	Kind string
	// Payload carries name/version/input/execution key for the executor.
	Payload json.RawMessage
	Meta    map[string]string
	// ExecutionKey dedupes ScheduleOperation while a non-terminal Operation
	// job with the same key already exists for the workflow (crash recovery).
	ExecutionKey string
	RunAt        time.Time // zero = now
	MaxAttempts  int       // 0 = driver/runtime default
}

// UncertainDecision is a Manager verb for ResolveUncertain.
type UncertainDecision string

const (
	UncertainComplete UncertainDecision = "complete"
	UncertainFail     UncertainDecision = "fail"
	UncertainRetry    UncertainDecision = "retry"
)

// WorkflowStore is the optional driver capability for workflow-as-code.
// Distinct from [DAGStore].
type WorkflowStore interface {
	StartWorkflow(ctx context.Context, p WorkflowStartParams) (inserted bool, existingID uuid.UUID, err error)
	GetWorkflowExecution(ctx context.Context, id uuid.UUID) (WorkflowExecutionView, error)
	AppendHistory(ctx context.Context, workflowID uuid.UUID, typ string, payload json.RawMessage) (seq int64, err error)
	ListHistory(ctx context.Context, workflowID uuid.UUID) ([]HistoryEvent, error)
	SignalWorkflow(ctx context.Context, p SignalParams) (inserted bool, err error)
	CompleteWorkflow(ctx context.Context, id uuid.UUID, result json.RawMessage) error
	FailWorkflow(ctx context.Context, id uuid.UUID, reason string) error
	CancelWorkflowExecution(ctx context.Context, id uuid.UUID) error
	SuspendWorkflow(ctx context.Context, id uuid.UUID, reason string) error
	ResumeWorkflow(ctx context.Context, id uuid.UUID) error

	// ScheduleOperation inserts (or returns an existing) Operation task job
	// for the execution. Dedupes by ExecutionKey among non-terminal jobs.
	ScheduleOperation(ctx context.Context, p ScheduleOperationParams) (jobID uuid.UUID, err error)

	// MarkUncertain moves an active Operation job to StateUncertain under
	// lease fencing and suspends the parent execution (§8).
	MarkUncertain(ctx context.Context, operationJobID, leaseToken uuid.UUID, reason string) error

	// ResolveUncertain applies an audited decision to an uncertain Operation
	// (complete / fail / retry), resumes the execution when appropriate, and
	// leaves a wake workflow-task for the runtime to schedule when needed.
	// Returns the workflow ID and workflow task kind hint from job meta
	// ("workflow_name") so the caller can ScheduleTask.
	ResolveUncertain(ctx context.Context, operationJobID uuid.UUID, decision string, result json.RawMessage) (workflowID uuid.UUID, workflowName string, err error)

	// ScheduleTask durably inserts one workflow-task job (Source
	// SourceWorkflow, RunID = workflowID) routed by kind, at runAt (zero
	// means "now"). It is how the workflow runtime schedules a replay pass —
	// on Start, on a delivered Signal, and on a parked Sleep's follow-up —
	// reusing azync_jobs per docs/workflow-v1-spec.md §17. Once inserted, the
	// job is leased and settled through the plain Store surface
	// (DequeueBatch/Ack/Release/Dead/...), keyed by (Source, Kind) and
	// fenced by lease token exactly like every other job.
	ScheduleTask(ctx context.Context, workflowID uuid.UUID, kind string, runAt time.Time) error

	// VacuumWorkflows deletes terminal workflow-as-code executions
	// (succeeded/failed/cancelled) completed before retention ago, cascading
	// via FKs to history, signals, timers and azync_jobs linked by run_id.
	// A retention <= 0 removes nothing.
	VacuumWorkflows(ctx context.Context, retention time.Duration) (int64, error)
}
