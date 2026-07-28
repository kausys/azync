package dag

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// Manager is the DAG administration surface: inspection, retry, compensation
// and cancellation. Pure library — no auth, no HTTP; embed it behind your own
// ops endpoints. It operates the dag source only.
type Manager struct {
	store driver.DAGStore
	// jobs is the same backend through the base Store contract, for the
	// job-level admin verbs (RunNow) the DAGStore capability does not carry.
	jobs driver.Store
}

// State is the persisted lifecycle state of a DAG execution.
type State = driver.DAGState

// DAG lifecycle states, re-exported from the driver contract.
const (
	// StateRunning marks a DAG whose tasks are executing.
	StateRunning = driver.DAGRunning
	// StateSuspended marks a DAG parked for a manual decision (Retry,
	// Compensate or Cancel).
	StateSuspended = driver.DAGSuspended
	// StateCompensating marks a DAG whose compensation chain is executing.
	StateCompensating = driver.DAGCompensating
	// StatePaused marks a DAG an operator froze with Pause; Retry resumes it.
	StatePaused = driver.DAGPaused
	// StateSucceeded is the terminal state of a DAG whose tasks all succeeded.
	StateSucceeded = driver.DAGSucceeded
	// StateFailed is the terminal state of a failed DAG (after its
	// compensations, if any, finished).
	StateFailed = driver.DAGFailed
	// StateCancelled is the terminal state of an operator-cancelled DAG.
	StateCancelled = driver.DAGCancelled
)

// TaskState is the persisted lifecycle state of one task job.
type TaskState = driver.JobState

// Task lifecycle states, re-exported from the driver contract.
const (
	// TaskPending marks a task ready to be leased.
	TaskPending = driver.StatePending
	// TaskScheduled marks a task with a future run_at: a started timer, a retry
	// backoff or a NotReady re-check.
	TaskScheduled = driver.StateScheduled
	// TaskActive marks a task currently leased by a worker.
	TaskActive = driver.StateActive
	// TaskBlocked marks a task whose dependencies are not all satisfied yet.
	TaskBlocked = driver.StateBlocked
	// TaskWaiting marks a WaitSignal task parked until its signal arrives.
	TaskWaiting = driver.StateWaiting
	// TaskSucceeded is the terminal state of a completed task.
	TaskSucceeded = driver.StateSucceeded
	// TaskDead marks a task that aborted or exhausted its retry budget.
	TaskDead = driver.StateDead
	// TaskCancelled is the terminal state of a task cancelled by the failure
	// policy or an operator verb.
	TaskCancelled = driver.StateCancelled
	// TaskSkipped is the terminal state of a task that deliberately did no
	// work (the handler returned Skip): it satisfied its dependents like a
	// success, carries no result and never compensates.
	TaskSkipped = driver.StateSkipped
	// TaskPaused marks a task held out of the ready set by an operator
	// (Manager.Pause); Manager.Retry releases it.
	TaskPaused = driver.StatePaused
)

// View is the admin projection of one DAG header.
type View = driver.DAGView

// Filter selects DAGs for List. A zero field means "no bound".
type Filter = driver.DAGFilter

// TaskView is the admin projection of one task job of a DAG. Optional
// timestamps are zero when absent (IsZero reports absence).
type TaskView struct {
	// ID is the task job's primary key (usable with attempt-history admin
	// tooling).
	ID uuid.UUID
	// Key is the task's key within the DAG ("comp:<key>" for a compensation).
	Key string
	// Kind is the handler kind, or an internal kind ("$sleep", "$signal").
	Kind        string
	State       TaskState
	Attempt     int
	MaxAttempts int
	// DependsOn are the keys this task waits for — the edges declared at Run,
	// plus the compensation-chain links for a "comp:" task. Nil for a root
	// task; nil and empty mean the same thing. Together across every task of
	// the DAG these are the graph, so an admin surface can lay a run out by
	// dependency depth instead of inferring an order from timestamps.
	DependsOn []string
	// EnqueuedAt is when the task row was created.
	EnqueuedAt time.Time
	// RunAt is when the task becomes (or became) due.
	RunAt time.Time
	// StartedAt is when the current attempt was leased, so
	// CompletedAt.Sub(StartedAt) is how long the task actually ran. RunAt
	// cannot answer that — promotion, retry backoff and snooze each rewrite it
	// — and CompletedAt.Sub(EnqueuedAt) folds in the wait, which for a task
	// parked on a poll or a signal dwarfs the execution. Zero before the first
	// lease.
	StartedAt time.Time
	// CompletedAt is zero until the task completed (succeeded or cancelled).
	CompletedAt time.Time
	// LastError is the most recent failure message.
	LastError string
	// HasResult reports whether the task persisted a durable result (the
	// payload itself is read by the tasks that depend on it, via ResultOf).
	HasResult bool
}

// Page is one page of dags for the admin list.
type Page struct {
	Items []View
	Page  int
	Size  int
	Total int64
}

// Get returns one DAG header or nil when it does not exist.
func (m *Manager) Get(ctx context.Context, id uuid.UUID) (*View, error) {
	view, err := m.store.GetDAG(ctx, id)
	if err != nil {
		if driver.IsNotFound(err) {
			return nil, nil //nolint:nilnil // absence is not an error for the admin surface
		}
		return nil, err
	}
	return view, nil
}

// Tasks returns every task of the DAG (compensation tasks included)
// ordered by creation time — compensations follow the original tasks; the
// relative order of tasks inserted together at Run is stable, not the
// declaration order — or nil when the DAG does not exist (a DAG
// always has at least one task, so nil unambiguously means absence).
//
// Each view carries its DependsOn keys, so the returned slice is the whole
// graph and not just a list: a caller can lay the run out by dependency depth
// rather than infer an order from timestamps, which would render a parallel
// fan-out as a chain and never look wrong.
func (m *Manager) Tasks(ctx context.Context, id uuid.UUID) ([]TaskView, error) {
	jobs, err := m.store.DAGTasks(ctx, id)
	if err != nil {
		if driver.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	deps, err := m.store.DAGDeps(ctx, id)
	if err != nil {
		return nil, err
	}
	byKey := make(map[string][]string, len(deps))
	for _, d := range deps {
		byKey[d.TaskKey] = append(byKey[d.TaskKey], d.DependsOnKey)
	}
	views := make([]TaskView, 0, len(jobs))
	for _, j := range jobs {
		views = append(views, TaskView{
			ID:          j.ID,
			Key:         j.TaskKey,
			Kind:        j.Kind,
			State:       j.State,
			Attempt:     j.Attempt,
			MaxAttempts: j.MaxAttempts,
			DependsOn:   byKey[j.TaskKey],
			EnqueuedAt:  j.EnqueuedAt,
			RunAt:       j.RunAt,
			StartedAt:   j.StartedAt,
			CompletedAt: j.CompletedAt,
			LastError:   j.LastError,
			HasResult:   j.Result != nil,
		})
	}
	return views, nil
}

// TaskResult returns the persisted result of one settled task — the
// deliberate, single-task read for "what did the provider return here?"
// during an investigation. It is a separate call, not a TaskView field, on
// purpose: task results routinely carry sensitive payloads (PII, raw
// provider responses), so listings never bulk-expose them and the caller can
// gate this accessor behind its own authorization.
//
// A succeeded task without a result returns (nil, nil); a skipped task
// returns ErrTaskSkipped (errors.Is); a task that has not settled — or a
// missing DAG or key — returns a not-found error (IsNotFound).
func (m *Manager) TaskResult(ctx context.Context, id uuid.UUID, taskKey string) (json.RawMessage, error) {
	results, err := m.store.TaskResults(ctx, id, []string{taskKey})
	if err != nil {
		return nil, err
	}
	tr, ok := results[taskKey]
	if !ok {
		return nil, driver.NewNotFound("task result")
	}
	if tr.Skipped {
		return nil, fmt.Errorf("dag: task result %q: %w", taskKey, ErrTaskSkipped)
	}
	return tr.Result, nil
}

// List returns one page of dags matching filter, newest first (page is
// 0-based; size defaults to 50).
func (m *Manager) List(ctx context.Context, filter Filter, page, size int) (Page, error) {
	if size <= 0 {
		size = 50
	}
	if page < 0 {
		page = 0
	}
	rows, total, err := m.store.ListDAGs(ctx, filter, page*size, size)
	if err != nil {
		return Page{}, err
	}
	return Page{Items: rows, Page: page, Size: size, Total: total}, nil
}

// Pause freezes a RUNNING DAG without burning anything: its pending and
// scheduled tasks are held out of the ready set, blocked/waiting tasks stay
// as they are (nothing promotes them while paused), incoming signals buffer
// for delivery after resume, and time spent paused never consumes a task's
// snooze Deadline budget. The one resume verb is Retry. Use it to freeze
// in-flight workflows during a provider outage instead of letting them fail
// their way into suspended. An active (leased) task keeps its lease and
// settles on its own. It returns a not-found error (see IsNotFound) for a
// missing DAG or one in any state but running.
func (m *Manager) Pause(ctx context.Context, id uuid.UUID, reason string) error {
	return m.store.PauseDAG(ctx, id, reason)
}

// RunNow expedites one scheduled task of the DAG — a NotReady re-check, a
// retry backoff or a started Sleep timer — to run immediately, addressed by
// its task key (how an operator thinks, no job UUID needed). It returns a
// not-found error (see IsNotFound) for a missing DAG or key, or a task in
// any state but scheduled.
func (m *Manager) RunNow(ctx context.Context, id uuid.UUID, taskKey string) error {
	jobs, err := m.store.DAGTasks(ctx, id)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.TaskKey == taskKey {
			return m.jobs.RunNow(ctx, driver.SourceDAG, j.ID)
		}
	}
	return driver.NewNotFound("run now")
}

// AttemptError is one recorded failure in a task's retry history.
type AttemptError = driver.AttemptError

// TaskAttempts returns one task's failure history, oldest attempt first,
// addressed by its key within the DAG (how an operator thinks, no job UUID
// needed). LastError on a TaskView is only the most recent failure; this is
// the whole trail, which is what distinguishes "flaky, succeeded on retry 3"
// from "the same error four times".
//
// A task that never failed returns no entries. It returns a not-found error
// (see IsNotFound) for a missing DAG or key.
func (m *Manager) TaskAttempts(ctx context.Context, id uuid.UUID, taskKey string) ([]AttemptError, error) {
	jobs, err := m.store.DAGTasks(ctx, id)
	if err != nil {
		return nil, err
	}
	for _, j := range jobs {
		if j.TaskKey == taskKey {
			return m.jobs.JobAttempts(ctx, driver.SourceDAG, j.ID)
		}
	}
	return nil, driver.NewNotFound("task attempts")
}

// Stats counts DAG executions by state — the one read behind a state-tab bar,
// which would otherwise be a List call per state.
type Stats struct {
	Running      int64
	Suspended    int64
	Compensating int64
	Paused       int64
	Succeeded    int64
	Failed       int64
	Cancelled    int64
	// Total is every DAG the backend still retains; a vacuumed run is counted
	// nowhere, so this is not a lifetime total.
	Total int64
}

// Stats returns how many DAGs sit in each state.
func (m *Manager) Stats(ctx context.Context) (Stats, error) {
	counts, err := m.store.DAGStateCounts(ctx)
	if err != nil {
		return Stats{}, err
	}
	var out Stats
	for state, n := range counts {
		switch state {
		case StateRunning:
			out.Running = n
		case StateSuspended:
			out.Suspended = n
		case StateCompensating:
			out.Compensating = n
		case StatePaused:
			out.Paused = n
		case StateSucceeded:
			out.Succeeded = n
		case StateFailed:
			out.Failed = n
		case StateCancelled:
			out.Cancelled = n
		}
		// An unknown state still counts toward Total: a backend ahead of this
		// library must not make the tab bar's numbers stop adding up.
		out.Total += n
	}
	return out, nil
}

// TaskCounts returns, per DAG id, how many of its tasks sit in each task
// state — the batch read behind a listing that shows each run's progress.
// Doing it per row instead would be one query per listed DAG.
//
// Ids with no tasks (unknown ones included) are absent from the result.
func (m *Manager) TaskCounts(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]map[TaskState]int64, error) {
	return m.store.DAGTaskCounts(ctx, ids)
}

// Retry resumes a non-terminal DAG after failures or an operator Pause:
// dead tasks are reset to pending with a fresh budget, paused tasks return
// to the ready set (both with a fresh snooze Deadline budget), and a
// suspended or paused DAG resumes (to running, or back to compensating when
// a compensation chain exists — original tasks never rerun once
// compensation started). It returns a not-found error (see IsNotFound) for
// a missing or terminal DAG.
func (m *Manager) Retry(ctx context.Context, id uuid.UUID) error {
	return m.store.RetryDAG(ctx, id)
}

// Compensate manually triggers compensation on a running or suspended
// DAG, exactly like the Cancel failure policy: remaining tasks are
// cancelled, the compensation chain of the succeeded tasks that declared one
// is inserted in reverse completion order, and the DAG moves to
// compensating (or straight to failed when there is nothing to compensate).
// It returns a not-found error for a missing DAG or one in any other
// state.
func (m *Manager) Compensate(ctx context.Context, id uuid.UUID) error {
	return m.store.CompensateDAG(ctx, id)
}

// Cancel cancels a non-terminal DAG without compensating (compensation
// is its own verb): remaining tasks are cancelled and the DAG becomes
// cancelled. On a compensating DAG the in-flight compensation settles
// first and the DAG then lands on cancelled. It returns a not-found
// error for a missing or already terminal DAG.
func (m *Manager) Cancel(ctx context.Context, id uuid.UUID) error {
	return m.store.CancelDAG(ctx, id)
}
