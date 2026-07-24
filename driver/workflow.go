package driver

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Internal task kinds and reserved key prefixes. The internal kinds are
// resolved entirely by the workflow scheduler ([WorkflowStore.CompleteDueSleeps]
// and [WorkflowStore.Signal]); they are never registered on a worker, so the
// engine's PromoteDue (which promotes only registered kinds) can never move
// them to pending and no handler ever runs for them.
const (
	// KindSleep is the reserved kind of a durable timer task. It is born
	// blocked (or scheduled when it has no dependencies) and, once unblocked,
	// sits scheduled with run_at = now()+SleepFor until CompleteDueSleeps marks
	// it succeeded. A task with SignalName set can be woken early by Signal.
	KindSleep = "$sleep"
	// KindSignal is the reserved kind of a wait-for-signal task. It is born
	// blocked (or waiting when it has no dependencies) and, once unblocked,
	// sits in StateWaiting until Signal completes it with the signal payload as
	// its result.
	KindSignal = "$signal"
	// TaskKeyCompensationPrefix prefixes the task key of every compensation
	// task ("comp:<original key>"). User task keys must never carry it.
	TaskKeyCompensationPrefix = "comp:"
)

// WorkflowState is the persisted lifecycle state of a workflow.
type WorkflowState string

const (
	// WorkflowRunning marks a workflow whose DAG is executing.
	WorkflowRunning WorkflowState = "running"
	// WorkflowSuspended marks a workflow parked for a manual decision (retry,
	// compensate or cancel), either by the suspend failure policy or by a dead
	// compensation task.
	WorkflowSuspended WorkflowState = "suspended"
	// WorkflowCompensating marks a workflow whose compensation chain is
	// executing.
	WorkflowCompensating WorkflowState = "compensating"
	// WorkflowSucceeded is the terminal state of a workflow whose tasks all
	// succeeded.
	WorkflowSucceeded WorkflowState = "succeeded"
	// WorkflowFailed is the terminal state of a failed workflow (after its
	// compensations, if any, finished).
	WorkflowFailed WorkflowState = "failed"
	// WorkflowCancelled is the terminal state of an operator-cancelled
	// workflow.
	WorkflowCancelled WorkflowState = "cancelled"
)

// OnFailurePolicy is a workflow's declared reaction to a dead task, applied
// set-based by [WorkflowStore.ApplyFailurePolicy].
type OnFailurePolicy string

const (
	// OnFailureCancel cancels the remaining tasks, inserts the compensation
	// chain of the succeeded tasks that declared one, and settles the workflow
	// to failed once the compensations finish (immediately when there are
	// none).
	OnFailureCancel OnFailurePolicy = "cancel"
	// OnFailureSuspend parks the workflow as suspended, leaving its tasks
	// untouched, so an operator (or the Manager API) decides between retry,
	// compensate and cancel.
	OnFailureSuspend OnFailurePolicy = "suspend"
)

// WorkflowParams is the durable input for one workflow: its header plus the
// full static DAG (tasks and dependency edges) declared at creation time.
type WorkflowParams struct {
	// ID is the caller-assigned primary key; drivers must not overwrite it.
	ID uuid.UUID
	// Name is the workflow definition name; dedupe scopes to it.
	Name string
	// OnFailure is the declared failure policy. Drivers treat an empty value
	// as OnFailureCancel.
	OnFailure OnFailurePolicy
	// IdempotencyKey deduplicates within Name across live (running, suspended
	// or compensating) executions. Empty disables dedupe; a terminal workflow
	// frees the key.
	IdempotencyKey string
	// Meta carries string-valued annotations, propagated onto every task job.
	Meta map[string]string
	// TraceID, SpanID and TraceFlags carry an optional propagated trace,
	// stamped onto every task job.
	TraceID    string
	SpanID     string
	TraceFlags int16
	// Tasks is the static task set. Task keys must be unique within the
	// workflow.
	Tasks []WorkflowTask
	// Deps are the DAG edges: each entry blocks TaskKey until DependsOnKey
	// succeeded.
	Deps []WorkflowDep
}

// WorkflowTask is one declared task of a workflow DAG.
type WorkflowTask struct {
	// Key identifies the task within its workflow (unique, caller-validated
	// against the reserved "$" and "comp:" prefixes).
	Key string
	// Kind is the handler kind, or an internal kind (KindSleep, KindSignal).
	Kind string
	// Payload is the opaque handler argument.
	Payload json.RawMessage
	// MaxAttempts is the retry budget. Zero defers to the runtime default,
	// resolved durably on the first lease.
	MaxAttempts int
	// CompensationKind, when set, declares a compensation for this task: on a
	// compensating workflow a "comp:<Key>" task of this kind is inserted with
	// CompensationPayload once this task succeeded.
	CompensationKind    string
	CompensationPayload json.RawMessage
	// SignalName names the signal this task reacts to: a KindSignal task
	// completes on it, a KindSleep task is woken early by it. Empty for tasks
	// that ignore signals.
	SignalName string
	// SleepFor is the KindSleep duration, resolved against the backend clock
	// when the timer starts (at creation for a root task, at promotion
	// otherwise).
	SleepFor time.Duration
	// IgnoreDeadDeps lets this task be promoted even when a dependency ended
	// dead or cancelled, treating those dependencies as satisfied. It also
	// exempts a dead dependency from the failure policy when every dependent of
	// that dead task declares IgnoreDeadDeps (see ApplyFailurePolicy): the
	// tolerant branch is allowed to run instead of being cancelled. The
	// exemption is never vacuous — a dead task with no dependents (a leaf)
	// always triggers the policy — and a workflow that runs to completion with
	// any dead task still settles failed, not succeeded (see CompleteWorkflows).
	IgnoreDeadDeps bool
}

// WorkflowDep is one DAG edge: TaskKey waits for DependsOnKey.
type WorkflowDep struct {
	TaskKey      string
	DependsOnKey string
}

// WorkflowView is the backend-neutral projection of a workflow header for the
// admin and manager surfaces.
type WorkflowView struct {
	ID             uuid.UUID
	Name           string
	State          WorkflowState
	OnFailure      OnFailurePolicy
	IdempotencyKey string
	// FailureReason describes why the workflow left the happy path (the dead
	// tasks that triggered the failure policy, or a dead compensation).
	FailureReason string
	// Meta carries string-valued annotations. On reads it is never nil: a
	// workflow with no annotations returns an empty (non-nil) map.
	Meta    map[string]string
	TraceID string
	// CreatedAt and UpdatedAt are lifecycle timestamps; CompletedAt is zero
	// until the workflow reaches a terminal state.
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt time.Time
}

// WorkflowFilter selects workflows for the admin list. A zero field means "no
// bound".
type WorkflowFilter struct {
	Name  string
	State WorkflowState
}

// WorkflowFailure reports one workflow the failure policy acted on in an
// ApplyFailurePolicy pass.
type WorkflowFailure struct {
	WorkflowID uuid.UUID
	// Policy is the policy that was applied.
	Policy OnFailurePolicy
	// DeadTasks are the task keys whose death triggered the policy, sorted.
	DeadTasks []string
}

// WorkflowStore is the optional workflow capability: the DAG scheduler and
// admin contract a backend implements on top of [Store] to support the
// workflow runtime. A backend without it simply cannot run workflows; queue
// and event runtimes never require it.
//
// The scheduler methods (PromoteUnblocked, CompleteDueSleeps,
// ApplyFailurePolicy, CompleteWorkflows) are set-based and idempotent: the
// workflow worker calls them on a fixed tick from every instance without
// leader election, so an operation observing no eligible rows must be a no-op.
// All time comparisons resolve against the backend's own clock.
//
// Implementations must be safe for concurrent use.
type WorkflowStore interface {
	// CreateWorkflow atomically inserts the workflow header, its tasks and its
	// dependency edges, and signals workers for immediately runnable tasks —
	// one transaction, all or nothing. Initial task states: a task with
	// dependencies is blocked; a dependency-free task is pending, except the
	// internal kinds (a root KindSleep is scheduled with run_at =
	// now()+SleepFor, a root KindSignal is waiting). Task keys are unique per
	// workflow.
	//
	// When p.IdempotencyKey is set and a workflow with the same (Name,
	// IdempotencyKey) is live (running, suspended or compensating), nothing is
	// inserted and the existing execution's id is returned as (false,
	// existingID, nil). A terminal workflow frees the key. existingID is
	// meaningful only when inserted is false.
	CreateWorkflow(ctx context.Context, p WorkflowParams) (inserted bool, existingID uuid.UUID, err error)

	// Signal delivers a named signal to one workflow: every waiting KindSignal
	// task with SignalName name completes as succeeded with the payload
	// persisted as its result, and every scheduled KindSleep task with
	// SignalName name is woken early (run_at = now()). It returns the number
	// of tasks affected; zero with a nil error means nothing was waiting.
	Signal(ctx context.Context, workflowID uuid.UUID, name string, payload json.RawMessage) (matched int64, err error)

	// PromoteUnblocked moves every blocked task whose dependencies are all
	// satisfied to its runnable state, chosen by kind: KindSignal to waiting,
	// KindSleep to scheduled with run_at = now()+SleepFor, anything else to
	// pending. A dependency is satisfied when it succeeded; for a task with
	// IgnoreDeadDeps, dead and cancelled dependencies also count as satisfied.
	// It returns the number of tasks promoted.
	PromoteUnblocked(ctx context.Context) (int64, error)

	// CompleteDueSleeps marks every scheduled KindSleep task whose run_at is
	// due as succeeded (stamping completed_at) without running any handler,
	// and returns the count.
	CompleteDueSleeps(ctx context.Context) (int64, error)

	// ApplyFailurePolicy applies each running workflow's OnFailure policy when
	// it has at least one triggering dead task. A dead task triggers the policy
	// unless every one of its dependents declares IgnoreDeadDeps — that lone
	// exemption lets a fully tolerant branch keep running instead of being
	// cancelled. The exemption is never vacuous: a dead task with no dependents
	// (a leaf) always triggers, as there is no tolerant branch to preserve.
	// OnFailureCancel cancels the workflow's non-terminal tasks (pending,
	// scheduled, blocked, waiting), inserts the compensation chain — one
	// "comp:<key>" task per succeeded task that declared a compensation, chained
	// via dependencies in reverse completed_at order, the first pending and the
	// rest blocked — and moves the workflow to compensating (or straight to
	// failed when there is nothing to compensate). OnFailureSuspend moves the
	// workflow to suspended, leaving its tasks untouched. Both record the dead
	// tasks in FailureReason. It returns one WorkflowFailure per workflow acted
	// on.
	//
	// A task that is active (leased by a worker) when the policy fires is left
	// alone: the lease belongs to the worker, so it is neither cancelled nor
	// compensated. Should it complete after the policy pass, it settles on its
	// own and stays outside the compensation chain already inserted — an
	// accepted v1 limitation.
	ApplyFailurePolicy(ctx context.Context) ([]WorkflowFailure, error)

	// CompleteWorkflows settles workflows whose work is finished. A running
	// workflow settles once all of its tasks are terminal (succeeded or dead)
	// AND every dead task is tolerated — each has at least one dependent and all
	// of its dependents declare IgnoreDeadDeps: it becomes succeeded when none
	// died, or failed — with FailureReason listing the dead task keys — when at
	// least one task died but every death was tolerated (the policy never fired
	// and the tolerant branches ran to completion). A running workflow that is
	// all-terminal but carries a NON-tolerated dead task (a dead leaf, or a dead
	// task some dependent does not tolerate) is left running for
	// ApplyFailurePolicy to run its OnFailure policy — cancel inserts the
	// compensation chain, suspend parks it. This tolerance re-check is
	// authoritative: correctness does NOT depend on ApplyFailurePolicy having run
	// earlier in the same worker tick. The two are separate transactions, and a
	// task can die in the window between them (a live worker acking an Abort), so
	// relying on intra-tick ordering would let a Cancel-policy saga settle failed
	// with its compensations silently skipped. Running ApplyFailurePolicy before
	// CompleteWorkflows on each tick remains recommended hygiene — it settles a
	// triggering death one tick sooner — but is no longer required for
	// correctness. A compensating workflow settles once its compensation tasks
	// are all terminal: it becomes failed — or cancelled when the compensation
	// was triggered through CancelWorkflow — and a compensating workflow with a
	// dead compensation task becomes suspended for a manual decision. It returns
	// the number of workflows transitioned.
	CompleteWorkflows(ctx context.Context) (int64, error)

	// TaskResults returns the persisted results of the workflow's succeeded
	// tasks, keyed by task key, restricted to keys when non-empty (an empty
	// keys slice returns every succeeded task). A succeeded task without a
	// result maps to a nil value; tasks not yet succeeded are absent.
	TaskResults(ctx context.Context, workflowID uuid.UUID, keys []string) (map[string]json.RawMessage, error)

	// AckTaskResult completes an active task exactly like Store.Ack and
	// additionally persists result as the task's durable output, atomically.
	// Same lease-token fencing: it returns a not-found error (see IsNotFound)
	// when the token no longer owns an active row.
	AckTaskResult(ctx context.Context, id, leaseToken uuid.UUID, result json.RawMessage) error

	// GetWorkflow returns one workflow header by id, or a not-found error (see
	// IsNotFound) when none matches.
	GetWorkflow(ctx context.Context, id uuid.UUID) (*WorkflowView, error)

	// ListWorkflows lists workflows matching filter, newest first (created_at
	// descending), paginated, returning the page and the total matching count.
	ListWorkflows(ctx context.Context, filter WorkflowFilter, offset, limit int) ([]WorkflowView, int64, error)

	// WorkflowTasks returns every task job of the workflow (compensation tasks
	// included) in creation order, or a not-found error when the workflow does
	// not exist.
	WorkflowTasks(ctx context.Context, id uuid.UUID) ([]Job, error)

	// RetryWorkflow resumes a non-terminal workflow after failures: dead tasks
	// are reset to pending with a fresh budget (attempt and reap_count
	// cleared) and a suspended workflow resumes — to running, or back to
	// compensating when a compensation chain exists (only the dead
	// compensation tasks are reset then, so original tasks never rerun after
	// compensating started). A compensating workflow stays compensating. It
	// returns a not-found error for a missing or terminal workflow.
	RetryWorkflow(ctx context.Context, id uuid.UUID) error

	// CompensateWorkflow manually triggers compensation on a running or
	// suspended workflow: exactly like the OnFailureCancel policy, it cancels
	// the non-terminal tasks, inserts the compensation chain and moves the
	// workflow to compensating (or failed when there is nothing to
	// compensate). It returns a not-found error for a missing workflow or one
	// in any other state.
	CompensateWorkflow(ctx context.Context, id uuid.UUID) error

	// CancelWorkflow cancels a non-terminal workflow without compensating
	// (compensation is its own verb): the non-terminal tasks (pending,
	// scheduled, blocked, waiting) are cancelled and the workflow becomes
	// cancelled. On a compensating workflow the in-flight compensation is
	// allowed to settle first: the workflow keeps compensating and
	// CompleteWorkflows lands it on cancelled instead of failed. It returns a
	// not-found error for a missing or already terminal workflow.
	CancelWorkflow(ctx context.Context, id uuid.UUID) error

	// VacuumWorkflows deletes terminal workflows completed before retention
	// ago, cascading to their task jobs and dependency edges, and returns the
	// number of workflows removed. A retention <= 0 retains all and removes
	// nothing.
	VacuumWorkflows(ctx context.Context, retention time.Duration) (int64, error)
}

// TxWorkflowStore is the optional transactional workflow-creation capability,
// generic over the backend's transaction handle TTx (one concrete type per
// driver, e.g. pgx.Tx). It lets a caller enlist workflow creation in an
// existing business transaction so the workflow commits atomically with the
// caller's writes.
type TxWorkflowStore[TTx any] interface {
	// CreateWorkflowTx performs CreateWorkflow within the caller's
	// transaction.
	CreateWorkflowTx(ctx context.Context, tx TTx, p WorkflowParams) (inserted bool, existingID uuid.UUID, err error)
}
