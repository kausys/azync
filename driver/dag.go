package driver

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Internal task kinds and reserved key prefixes. The internal kinds are
// resolved entirely by the workflow scheduler ([DAGStore.CompleteDueSleeps]
// and [DAGStore.Signal]); they are never registered on a worker, so the
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

// DAGState is the persisted lifecycle state of a workflow.
type DAGState string

const (
	// DAGRunning marks a workflow whose DAG is executing.
	DAGRunning DAGState = "running"
	// DAGSuspended marks a workflow parked for a manual decision (retry,
	// compensate or cancel), either by the suspend failure policy or by a dead
	// compensation task.
	DAGSuspended DAGState = "suspended"
	// DAGCompensating marks a workflow whose compensation chain is
	// executing.
	DAGCompensating DAGState = "compensating"
	// DAGPaused marks a workflow an operator froze (PauseDAG): non-terminal,
	// its live tasks held out of the ready set, nothing promotes or runs
	// until Manager.Retry resumes it. Distinct from DAGSuspended, which
	// records a failure — a paused workflow is healthy, just deliberately
	// stopped (say, while its provider is down).
	DAGPaused DAGState = "paused"
	// DAGSucceeded is the terminal state of a workflow whose tasks all
	// succeeded.
	DAGSucceeded DAGState = "succeeded"
	// DAGFailed is the terminal state of a failed workflow (after its
	// compensations, if any, finished).
	DAGFailed DAGState = "failed"
	// DAGCancelled is the terminal state of an operator-cancelled
	// workflow.
	DAGCancelled DAGState = "cancelled"
)

// OnFailurePolicy is a workflow's declared reaction to a dead task, applied
// set-based by [DAGStore.ApplyFailurePolicy].
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

// DAGParams is the durable input for one workflow: its header plus the
// full static DAG (tasks and dependency edges) declared at creation time.
type DAGParams struct {
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
	// Tasks is the static task set. Task keys must be unique within the
	// workflow.
	Tasks []DAGTask
	// Deps are the DAG edges: each entry blocks TaskKey until DependsOnKey
	// succeeded.
	Deps []DAGDep
}

// DAGTask is one declared task of a workflow DAG.
type DAGTask struct {
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
	// any dead task still settles failed, not succeeded (see CompleteDAGs).
	IgnoreDeadDeps bool
	// Deadline, when positive, bounds the task's snooze/NotReady loop: the
	// driver persists it as the job's snooze budget, and the FIRST Snooze
	// stamps deadline_at = now()+Deadline on the backend clock — the budget
	// measures time spent waiting for the resource, not the workflow's age.
	// A Snooze settled past the deadline dead-letters the task, triggering
	// the workflow's OnFailure policy on the next scheduler pass. Zero means
	// the task can snooze forever. Compensation tasks never inherit it, and
	// RetryDAG clears the stamped deadline so a retried task waits with a
	// fresh budget.
	Deadline time.Duration
}

// DAGDep is one DAG edge: TaskKey waits for DependsOnKey.
type DAGDep struct {
	TaskKey      string
	DependsOnKey string
}

// TaskResult is one settled task outcome as returned by TaskResults: either
// a succeeded task's persisted output (Result, possibly nil for a task that
// returned nothing) or a deliberate skip (Skipped=true, no result).
type TaskResult struct {
	Result  json.RawMessage
	Skipped bool
}

// DAGSignalParams delivers (or buffers) one named signal on a workflow.
type DAGSignalParams struct {
	DAGID uuid.UUID
	// Name is the signal name (the target task's key through the dag
	// runtime).
	Name string
	// MessageID, when non-empty, deduplicates within (DAGID, Name): a repeat
	// delivery of the same id is accepted and dropped. Empty disables dedupe.
	// Use the sender's event id for at-least-once webhooks.
	MessageID string
	Payload   json.RawMessage
}

// DAGView is the backend-neutral projection of a workflow header for the
// admin and manager surfaces.
type DAGView struct {
	ID             uuid.UUID
	Name           string
	State          DAGState
	OnFailure      OnFailurePolicy
	IdempotencyKey string
	// FailureReason describes why the workflow left the happy path (the dead
	// tasks that triggered the failure policy, or a dead compensation).
	FailureReason string
	// Meta carries string-valued annotations. On reads it is never nil: a
	// workflow with no annotations returns an empty (non-nil) map.
	Meta map[string]string
	// CreatedAt and UpdatedAt are lifecycle timestamps; CompletedAt is zero
	// until the workflow reaches a terminal state.
	CreatedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt time.Time
}

// DAGFilter selects dags for the admin list. A zero field means "no
// bound".
type DAGFilter struct {
	Name  string
	State DAGState
}

// DAGFailure reports one workflow the failure policy acted on in an
// ApplyFailurePolicy pass.
type DAGFailure struct {
	DAGID uuid.UUID
	// Policy is the policy that was applied.
	Policy OnFailurePolicy
	// DeadTasks are the task keys whose death triggered the policy, sorted.
	DeadTasks []string
}

// DAGStore is the optional workflow capability: the DAG scheduler and
// admin contract a backend implements on top of [Store] to support the
// workflow runtime. A backend without it simply cannot run dags; queue
// and event runtimes never require it.
//
// The scheduler methods (PromoteUnblocked, CompleteDueSleeps,
// ApplyFailurePolicy, CompleteDAGs) are set-based and idempotent: the
// workflow worker calls them on a fixed tick from every instance without
// leader election, so an operation observing no eligible rows must be a no-op.
// All time comparisons resolve against the backend's own clock.
//
// Implementations must be safe for concurrent use.
type DAGStore interface {
	// CreateDAG atomically inserts the workflow header, its tasks and its
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
	CreateDAG(ctx context.Context, p DAGParams) (inserted bool, existingID uuid.UUID, err error)

	// Signal delivers (or buffers) one named signal on a live workflow,
	// atomically: the delivery is appended to the workflow's signal inbox —
	// deduplicated by MessageID when set: a repeat of the same id is accepted
	// and dropped with deduplicated=true and no other effect — then
	// immediately consumed when a matching task is deliverable (a waiting
	// KindSignal task completes as succeeded with the payload persisted as
	// its result; a scheduled KindSleep task is woken early, run_at = now()).
	// delivered is the number of tasks completed or woken now; delivered == 0
	// with deduplicated == false means the signal was accepted and buffered —
	// DeliverBufferedSignals hands it to its task once that task becomes
	// deliverable, so a signal racing the scheduler's promotion is never
	// lost. It returns a not-found error (see IsNotFound) for a missing or
	// terminal workflow.
	Signal(ctx context.Context, p DAGSignalParams) (delivered int64, deduplicated bool, err error)

	// DeliverBufferedSignals consumes buffered inbox signals whose target
	// task has become deliverable (a waiting KindSignal task, or a scheduled
	// KindSleep task, of a running or compensating workflow), oldest signal
	// first per task, and returns the number delivered. Set-based and
	// idempotent; the scheduler calls it right after PromoteUnblocked so a
	// signal buffered while its task was still blocked lands within one tick.
	DeliverBufferedSignals(ctx context.Context) (int64, error)

	// FindDAGByKey resolves the live workflow (running, suspended,
	// compensating or paused) holding (name, idempotencyKey) — the business
	// key a webhook handler knows, without bookkeeping the run UUID
	// anywhere. At most one live workflow holds a key (the dedupe barrier);
	// terminal workflows free it, so a missing result is a not-found error
	// (see IsNotFound) — the right answer for a webhook arriving after the
	// run settled.
	FindDAGByKey(ctx context.Context, name, idempotencyKey string) (uuid.UUID, error)

	// PauseDAG freezes a RUNNING workflow: the header moves to DAGPaused with
	// reason recorded, and its pending/scheduled tasks move to StatePaused,
	// atomically. Blocked and waiting tasks keep their states — with the
	// header out of running/compensating nothing promotes them, and incoming
	// signals buffer in the inbox for delivery after resume. An active
	// (leased) task keeps its lease and settles on its own; its successors
	// simply never start. Time spent paused never burns a task's snooze
	// budget: RetryDAG — the one resume verb — clears the stamped deadline
	// when it releases the paused tasks. It returns a not-found error for a
	// missing workflow or one in any state but running.
	PauseDAG(ctx context.Context, id uuid.UUID, reason string) error

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
	// tasks in FailureReason. It returns one DAGFailure per workflow acted
	// on.
	//
	// A task that is active (leased by a worker) when the policy fires is left
	// alone: the lease belongs to the worker, so it is neither cancelled nor
	// compensated. Should it complete after the policy pass, it settles on its
	// own and stays outside the compensation chain already inserted — an
	// accepted v1 limitation.
	ApplyFailurePolicy(ctx context.Context) ([]DAGFailure, error)

	// CompleteDAGs settles dags whose work is finished. A running
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
	// CompleteDAGs on each tick remains recommended hygiene — it settles a
	// triggering death one tick sooner — but is not required for correctness. A
	// compensating workflow settles once its compensation tasks
	// are all terminal: it becomes failed — or cancelled when the compensation
	// was triggered through CancelDAG — and a compensating workflow with a
	// dead compensation task becomes suspended for a manual decision. It returns
	// the number of dags transitioned.
	CompleteDAGs(ctx context.Context) (int64, error)

	// TaskResults returns the settled outcomes of the workflow's succeeded
	// and skipped tasks, keyed by task key, restricted to keys when non-empty
	// (an empty keys slice returns every settled task). A succeeded task
	// without a result maps to an entry with a nil Result; a skipped task
	// maps to an entry with Skipped=true and no result — distinguishable, so
	// a consumer never mistakes "deliberately did nothing" for "succeeded
	// without output". Tasks not yet settled are absent.
	TaskResults(ctx context.Context, dagID uuid.UUID, keys []string) (map[string]TaskResult, error)

	// AckTaskResult completes an active task exactly like Store.Ack and
	// additionally persists result as the task's durable output, atomically.
	// Same lease-token fencing: it returns a not-found error (see IsNotFound)
	// when the token no longer owns an active row.
	AckTaskResult(ctx context.Context, id, leaseToken uuid.UUID, result json.RawMessage) error

	// GetDAG returns one workflow header by id, or a not-found error (see
	// IsNotFound) when none matches.
	GetDAG(ctx context.Context, id uuid.UUID) (*DAGView, error)

	// ListDAGs lists dags matching filter, newest first (created_at
	// descending), paginated, returning the page and the total matching count.
	ListDAGs(ctx context.Context, filter DAGFilter, offset, limit int) ([]DAGView, int64, error)

	// DAGTasks returns every task job of the workflow (compensation tasks
	// included) ordered by creation time — tasks created later (a compensation
	// chain) follow tasks created earlier, but the relative order of tasks
	// inserted in the same atomic batch (the initial DAG) is stable, not the
	// declaration order. It returns a not-found error when the workflow does
	// not exist.
	DAGTasks(ctx context.Context, id uuid.UUID) ([]Job, error)

	// DAGDeps returns every dependency edge of the workflow — the static edges
	// declared at Run plus the compensation-chain links inserted when
	// compensation starts — ordered by (task_key, depends_on_key). It is the
	// read half of what CreateDAG persists: the scheduler only ever asks these
	// rows "is this task unblocked?", so without it the graph is unreadable
	// from outside and an admin surface has to guess the shape of a run from
	// timestamps, which silently turns a parallel fan-out into a chain.
	//
	// An unknown id returns no rows rather than an error: callers pair this
	// with DAGTasks, which already reports absence.
	DAGDeps(ctx context.Context, id uuid.UUID) ([]DAGDep, error)

	// DAGNameStateCounts returns how many dags sit in each state, per
	// definition name, counting every dag the backend still retains. Names and
	// states with no dags may be absent rather than present with a zero.
	//
	// It is keyed by name rather than global because the set of names is
	// itself an answer nothing else provides: ListDAGs filters BY name but
	// never enumerates them, so an admin surface offering a definition
	// navigator (as the queue one does over ListKinds) would otherwise have to
	// learn names from whichever page happened to be on screen. Summing the
	// inner maps gives the global per-state counts, so this is one read, not
	// two.
	DAGNameStateCounts(ctx context.Context) (map[string]map[DAGState]int64, error)

	// DAGTaskCounts returns, per requested dag, how many of its tasks sit in
	// each task state — the one read that lets a listing show each run's task
	// breakdown without a DAGTasks call per row. Ids with no tasks (unknown
	// ones included) may be absent from the map rather than present with an
	// empty one; an empty ids slice returns an empty map without querying.
	DAGTaskCounts(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]map[JobState]int64, error)

	// RetryDAG resumes a non-terminal workflow after failures: dead tasks
	// are reset to pending with a fresh budget (attempt and reap_count
	// cleared) and a suspended workflow resumes — to running, or back to
	// compensating when a compensation chain exists (only the dead
	// compensation tasks are reset then, so original tasks never rerun after
	// compensating started). A compensating workflow stays compensating. It
	// returns a not-found error for a missing or terminal workflow.
	RetryDAG(ctx context.Context, id uuid.UUID) error

	// CompensateDAG manually triggers compensation on a running or
	// suspended workflow: exactly like the OnFailureCancel policy, it cancels
	// the non-terminal tasks, inserts the compensation chain and moves the
	// workflow to compensating (or failed when there is nothing to
	// compensate). It returns a not-found error for a missing workflow or one
	// in any other state.
	CompensateDAG(ctx context.Context, id uuid.UUID) error

	// CancelDAG cancels a non-terminal workflow without compensating
	// (compensation is its own verb): the non-terminal tasks (pending,
	// scheduled, blocked, waiting) are cancelled and the workflow becomes
	// cancelled. On a compensating workflow the in-flight compensation is
	// allowed to settle first: the workflow keeps compensating and
	// CompleteDAGs lands it on cancelled instead of failed. It returns a
	// not-found error for a missing or already terminal workflow.
	CancelDAG(ctx context.Context, id uuid.UUID) error

	// VacuumDAGs deletes terminal dags completed before retention
	// ago, cascading to their task jobs and dependency edges, and returns the
	// number of dags removed. A retention <= 0 retains all and removes
	// nothing.
	VacuumDAGs(ctx context.Context, retention time.Duration) (int64, error)
}

// TxDAGStore is the optional transactional workflow-creation capability,
// generic over the backend's transaction handle TTx (one concrete type per
// driver, e.g. pgx.Tx). It lets a caller enlist workflow creation in an
// existing business transaction so the workflow commits atomically with the
// caller's writes.
type TxDAGStore[TTx any] interface {
	// CreateDAGTx performs CreateDAG within the caller's
	// transaction.
	CreateDAGTx(ctx context.Context, tx TTx, p DAGParams) (inserted bool, existingID uuid.UUID, err error)
}
