package drivertest

// The workflow capability of the Fake: a complete in-memory implementation of
// driver.DAGStore honoring the contract's scheduler semantics — initial
// state placement, set-based promotion with the internal-kind CASE, durable
// timers and signals, failure policies with reverse-order compensation chains,
// completion settlement and the manager verbs. It is the behavioral oracle the
// workflow runtime is developed and conformance-tested against.
//
// Every method takes f.mu for its whole critical section, mirroring the
// single-statement atomicity of the SQL driver.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// fakeDAG is the internal workflow header record: the wire view plus the
// columns the contract does not expose.
type fakeDAG struct {
	driver.DAGView
	// deps are the DAG edges, extended by the compensation chain when it is
	// inserted.
	deps []driver.DAGDep
	// cancelRequested records that CancelDAG was called, so a
	// compensation that settles afterwards lands the workflow on cancelled
	// instead of failed.
	cancelRequested bool
}

// terminal reports whether the workflow reached a final state.
func (w *fakeDAG) terminal() bool {
	switch w.State {
	case driver.DAGSucceeded, driver.DAGFailed, driver.DAGCancelled:
		return true
	default:
		return false
	}
}

// --- creation --------------------------------------------------------------

// CreateDAG atomically inserts the header, tasks and deps, deduplicating
// by (Name, IdempotencyKey) against live executions.
func (f *Fake) CreateDAG(_ context.Context, p driver.DAGParams) (bool, uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()

	if p.IdempotencyKey != "" {
		for _, w := range f.dags {
			if w.Name == p.Name && w.IdempotencyKey == p.IdempotencyKey && !w.terminal() {
				return false, w.ID, nil
			}
		}
	}

	// PRIMARY KEY (id): a caller reusing an id is a hard error, mirroring the
	// SQL insert's PK violation — never a silent overwrite.
	if _, exists := f.dags[p.ID]; exists {
		return false, uuid.Nil, fmt.Errorf("drivertest: dag id %s already exists", p.ID)
	}

	// UNIQUE (dag_id, task_key): validated before any insert so creation
	// stays all-or-nothing like the SQL transaction.
	seen := make(map[string]bool, len(p.Tasks))
	for _, tk := range p.Tasks {
		if seen[tk.Key] {
			return false, uuid.Nil, fmt.Errorf("drivertest: duplicate task key %q in dag %s", tk.Key, p.ID)
		}
		seen[tk.Key] = true
	}

	// An empty policy normalizes to the cancel default at creation, so views
	// and the policy pass agree with the SQL driver's CASE on insert.
	onFailure := p.OnFailure
	if onFailure == "" {
		onFailure = driver.OnFailureCancel
	}

	w := &fakeDAG{
		DAGView: driver.DAGView{
			ID:             p.ID,
			Name:           p.Name,
			State:          driver.DAGRunning,
			OnFailure:      onFailure,
			IdempotencyKey: p.IdempotencyKey,
			Meta:           cloneMeta(p.Meta),
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		deps: slices.Clone(p.Deps),
	}
	f.dags[p.ID] = w

	blocked := make(map[string]bool, len(p.Deps))
	for _, d := range p.Deps {
		blocked[d.TaskKey] = true
	}
	for _, tk := range p.Tasks {
		f.insertDAGTask(w, tk, blocked[tk.Key], now)
	}
	return true, uuid.Nil, nil
}

// insertDAGTask inserts one task job with its initial state: blocked when
// it has dependencies; otherwise pending, except the internal kinds (a root
// $sleep is scheduled with its timer started, a root $signal waits). Callers
// hold f.mu.
func (f *Fake) insertDAGTask(w *fakeDAG, tk driver.DAGTask, hasDeps bool, now time.Time) {
	state := driver.StateBlocked
	runAt := now
	if !hasDeps {
		switch tk.Kind {
		case driver.KindSleep:
			state = driver.StateScheduled
			runAt = now.Add(tk.SleepFor)
		case driver.KindSignal:
			state = driver.StateWaiting
		default:
			state = driver.StatePending
		}
	}
	id := uuid.New()
	f.jobs[id] = &fakeJob{
		Job: driver.Job{
			ID:               id,
			Source:           driver.SourceDAG,
			Kind:             tk.Kind,
			State:            state,
			MaxAttempts:      tk.MaxAttempts,
			Payload:          clonePayload(tk.Payload),
			Meta:             cloneMeta(w.Meta),
			RunAt:            runAt,
			EnqueuedAt:       now,
			DAGID:            w.ID,
			TaskKey:          tk.Key,
			SignalName:       tk.SignalName,
			CompensationKind: tk.CompensationKind,
			IgnoreDeadDeps:   tk.IgnoreDeadDeps,
		},
		maxAttemptsExplicit: tk.MaxAttempts > 0,
		compensationPayload: clonePayload(tk.CompensationPayload),
		sleepFor:            tk.SleepFor,
		seq:                 f.nextSeq(),
	}
	f.bumpStat(driver.SourceDAG, tk.Kind, statEnqueued, 1, now)
	if state == driver.StatePending {
		f.wake(driver.SourceDAG, tk.Kind)
	}
}

// --- scheduler -------------------------------------------------------------

// Signal completes waiting $signal tasks named name (payload as result) and
// wakes named $sleep timers early.
func (f *Fake) Signal(_ context.Context, dagID uuid.UUID, name string, payload json.RawMessage) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	var matched int64
	for _, j := range f.jobs {
		if j.Source != driver.SourceDAG || j.DAGID != dagID || j.SignalName != name {
			continue
		}
		switch {
		case j.Kind == driver.KindSignal && j.State == driver.StateWaiting:
			j.State = driver.StateSucceeded
			j.Result = clonePayload(payload)
			j.CompletedAt = now
			matched++
		case j.Kind == driver.KindSleep && j.State == driver.StateScheduled:
			j.RunAt = now
			matched++
		}
	}
	return matched, nil
}

// PromoteUnblocked releases blocked tasks whose dependencies are all satisfied,
// into the runnable state their kind dictates. Only running and compensating
// dags promote: a suspended workflow starts nothing new until an operator
// verb resumes it.
func (f *Fake) PromoteUnblocked(_ context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	var promoted int64
	for _, j := range f.jobs {
		if j.Source != driver.SourceDAG || j.State != driver.StateBlocked {
			continue
		}
		w := f.dags[j.DAGID]
		if w == nil || (w.State != driver.DAGRunning && w.State != driver.DAGCompensating) {
			continue
		}
		if !f.depsSatisfiedLocked(w, j) {
			continue
		}
		switch j.Kind {
		case driver.KindSignal:
			j.State = driver.StateWaiting
		case driver.KindSleep:
			j.State = driver.StateScheduled
			j.RunAt = now.Add(j.sleepFor)
		default:
			j.State = driver.StatePending
			j.RunAt = now
			f.wake(driver.SourceDAG, j.Kind)
		}
		promoted++
	}
	return promoted, nil
}

// depsSatisfiedLocked reports whether every declared dependency of the task is
// satisfied: succeeded, or — only when the task ignores dead deps — dead or
// cancelled. A dependency without a matching task blocks forever (the runtime
// validates edges client-side). Callers hold f.mu.
func (f *Fake) depsSatisfiedLocked(w *fakeDAG, j *fakeJob) bool {
	for _, d := range w.deps {
		if d.TaskKey != j.TaskKey {
			continue
		}
		dep := f.dagTaskLocked(w.ID, d.DependsOnKey)
		if dep == nil {
			return false
		}
		switch dep.State {
		case driver.StateSucceeded:
		case driver.StateDead, driver.StateCancelled:
			if !j.IgnoreDeadDeps {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// dagTaskLocked finds the task of (workflow, key). Callers hold f.mu.
func (f *Fake) dagTaskLocked(dagID uuid.UUID, key string) *fakeJob {
	for _, j := range f.jobs {
		if j.Source == driver.SourceDAG && j.DAGID == dagID && j.TaskKey == key {
			return j
		}
	}
	return nil
}

// dagJobsLocked returns the workflow's task jobs in creation order.
// Callers hold f.mu.
func (f *Fake) dagJobsLocked(dagID uuid.UUID) []*fakeJob {
	var out []*fakeJob
	for _, j := range f.jobs {
		if j.Source == driver.SourceDAG && j.DAGID == dagID {
			out = append(out, j)
		}
	}
	slices.SortFunc(out, func(a, b *fakeJob) int { return int(a.seq - b.seq) })
	return out
}

// CompleteDueSleeps succeeds every due $sleep timer of a running workflow; no
// handler is involved.
func (f *Fake) CompleteDueSleeps(_ context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	var completed int64
	for _, j := range f.jobs {
		if j.Source != driver.SourceDAG || j.Kind != driver.KindSleep ||
			j.State != driver.StateScheduled || j.RunAt.After(now) {
			continue
		}
		w := f.dags[j.DAGID]
		if w == nil || w.State != driver.DAGRunning {
			continue
		}
		j.State = driver.StateSucceeded
		j.CompletedAt = now
		completed++
	}
	return completed, nil
}

// ApplyFailurePolicy applies each running workflow's OnFailure policy when a
// dead task triggers it. A dead task whose dependents all declare
// IgnoreDeadDeps does not trigger the policy (the ignoring branch keeps
// running); a dead task with no dependents always does.
func (f *Fake) ApplyFailurePolicy(_ context.Context) ([]driver.DAGFailure, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	var out []driver.DAGFailure
	for _, w := range f.dags {
		if w.State != driver.DAGRunning {
			continue
		}
		deadKeys := f.triggeringDeadTasksLocked(w)
		if len(deadKeys) == 0 {
			continue
		}
		w.FailureReason = "dead tasks: " + strings.Join(deadKeys, ", ")
		w.UpdatedAt = now
		policy := w.OnFailure // normalized to cancel at creation when empty
		if policy == driver.OnFailureSuspend {
			w.State = driver.DAGSuspended
		} else {
			f.cancelRemainingTasksLocked(w, now)
			if f.insertCompensationsLocked(w, now) == 0 {
				w.State = driver.DAGFailed
				w.CompletedAt = now
			} else {
				w.State = driver.DAGCompensating
			}
		}
		out = append(out, driver.DAGFailure{DAGID: w.ID, Policy: policy, DeadTasks: deadKeys})
	}
	return out, nil
}

// triggeringDeadTasksLocked returns the sorted keys of the workflow's dead
// tasks that trigger the failure policy: every dead task except one whose
// dependents (at least one) all declare IgnoreDeadDeps. Callers hold f.mu.
func (f *Fake) triggeringDeadTasksLocked(w *fakeDAG) []string {
	var keys []string
	for _, j := range f.dagJobsLocked(w.ID) {
		if j.State != driver.StateDead {
			continue
		}
		if f.ignoredByAllDependentsLocked(w, j.TaskKey) {
			continue
		}
		keys = append(keys, j.TaskKey)
	}
	slices.Sort(keys)
	return keys
}

// ignoredByAllDependentsLocked reports whether the task has at least one
// dependent and every one of them declares IgnoreDeadDeps. Callers hold f.mu.
func (f *Fake) ignoredByAllDependentsLocked(w *fakeDAG, key string) bool {
	dependents := 0
	for _, d := range w.deps {
		if d.DependsOnKey != key {
			continue
		}
		dependents++
		dep := f.dagTaskLocked(w.ID, d.TaskKey)
		if dep == nil || !dep.IgnoreDeadDeps {
			return false
		}
	}
	return dependents > 0
}

// cancelRemainingTasksLocked cancels the workflow's non-terminal, non-active
// tasks (pending, scheduled, blocked, waiting). Active tasks keep their lease
// and settle normally. Callers hold f.mu.
func (f *Fake) cancelRemainingTasksLocked(w *fakeDAG, now time.Time) {
	for _, j := range f.dagJobsLocked(w.ID) {
		switch j.State {
		case driver.StatePending, driver.StateScheduled, driver.StateBlocked, driver.StateWaiting:
			j.State = driver.StateCancelled
			j.CompletedAt = now
		default:
		}
	}
}

// insertCompensationsLocked inserts the compensation chain: one "comp:<key>"
// task per succeeded task that declared a compensation, in reverse completion
// order, chained through dependency edges (the newest completion compensates
// first and is pending; the rest are blocked on their predecessor). It returns
// the total number of compensation tasks the workflow now has. Callers hold
// f.mu.
func (f *Fake) insertCompensationsLocked(w *fakeDAG, now time.Time) int {
	existing := 0
	var candidates []*fakeJob
	for _, j := range f.dagJobsLocked(w.ID) {
		if strings.HasPrefix(j.TaskKey, driver.TaskKeyCompensationPrefix) {
			existing++
			continue
		}
		if j.State == driver.StateSucceeded && j.CompensationKind != "" {
			candidates = append(candidates, j)
		}
	}
	if existing > 0 {
		// The chain was already inserted (e.g. a manual compensate after a
		// policy pass); never double-insert.
		return existing
	}
	slices.SortFunc(candidates, func(a, b *fakeJob) int {
		if c := b.CompletedAt.Compare(a.CompletedAt); c != 0 { // newest first
			return c
		}
		return int(b.seq - a.seq)
	})

	prevKey := ""
	for i, orig := range candidates {
		key := driver.TaskKeyCompensationPrefix + orig.TaskKey
		state := driver.StateBlocked
		if i == 0 {
			state = driver.StatePending
		} else {
			w.deps = append(w.deps, driver.DAGDep{TaskKey: key, DependsOnKey: prevKey})
		}
		id := uuid.New()
		f.jobs[id] = &fakeJob{
			Job: driver.Job{
				ID:          id,
				Source:      driver.SourceDAG,
				Kind:        orig.CompensationKind,
				State:       state,
				MaxAttempts: orig.MaxAttempts,
				Payload:     clonePayload(orig.compensationPayload),
				Meta:        cloneMeta(w.Meta),
				RunAt:       now,
				EnqueuedAt:  now,
				DAGID:       w.ID,
				TaskKey:     key,
			},
			maxAttemptsExplicit: orig.maxAttemptsExplicit,
			seq:                 f.nextSeq(),
		}
		f.bumpStat(driver.SourceDAG, orig.CompensationKind, statEnqueued, 1, now)
		if state == driver.StatePending {
			f.wake(driver.SourceDAG, orig.CompensationKind)
		}
		prevKey = key
	}
	return len(candidates)
}

// CompleteDAGs settles finished dags: a running workflow whose tasks
// are all terminal (succeeded or dead) lands succeeded when none died, or
// failed (recording the dead task keys) when a tolerated task died; a
// compensating workflow settles once its compensation tasks resolve — suspended
// on a dead compensation (unless the compensation was cancelled through
// CancelDAG), failed when the chain finished, or cancelled when it was
// triggered through CancelDAG.
func (f *Fake) CompleteDAGs(_ context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	var settled int64
	for _, w := range f.dags {
		switch w.State {
		case driver.DAGRunning:
			tasks := f.dagJobsLocked(w.ID)
			if len(tasks) == 0 {
				continue
			}
			allTerminal := true
			allTolerated := true
			var deadKeys []string
			for _, j := range tasks {
				switch j.State {
				case driver.StateSucceeded:
				case driver.StateDead:
					deadKeys = append(deadKeys, j.TaskKey)
					// A dead task is tolerated iff it has at least one dependent
					// and every dependent declares IgnoreDeadDeps — the same
					// predicate ApplyFailurePolicy uses to decide it does not
					// trigger the policy.
					if !f.ignoredByAllDependentsLocked(w, j.TaskKey) {
						allTolerated = false
					}
				default:
					allTerminal = false
				}
			}
			if !allTerminal {
				continue
			}
			// A NON-tolerated dead task (a dead leaf, or a dead task some
			// dependent does not tolerate) must be settled by ApplyFailurePolicy,
			// which runs the OnFailure policy — cancel inserts the compensation
			// chain. Settling here would skip it. The tolerance re-check makes
			// this independent of whether ApplyFailurePolicy already ran this
			// tick: a task can die in the window between the two separate
			// transactions, so leave the workflow running for the policy pass
			// (this tick or the next).
			if !allTolerated {
				continue
			}
			if len(deadKeys) > 0 {
				// Every dead task was tolerated (its dependents all ignore dead
				// deps), so the policy never fired and the tolerant branches ran
				// to completion — but something died, so the workflow is failed,
				// honest for the operator ("finally" semantics).
				slices.Sort(deadKeys)
				w.State = driver.DAGFailed
				w.FailureReason = "dead tasks: " + strings.Join(deadKeys, ", ")
			} else {
				w.State = driver.DAGSucceeded
			}
			w.CompletedAt = now
			w.UpdatedAt = now
			settled++

		case driver.DAGCompensating:
			var comps []*fakeJob
			for _, j := range f.dagJobsLocked(w.ID) {
				if strings.HasPrefix(j.TaskKey, driver.TaskKeyCompensationPrefix) {
					comps = append(comps, j)
				}
			}
			if len(comps) == 0 {
				continue
			}
			terminal := true
			dead := false
			for _, j := range comps {
				switch j.State {
				case driver.StateSucceeded, driver.StateCancelled:
				case driver.StateDead:
					dead = true
				default:
					terminal = false
				}
			}
			switch {
			case dead && !w.cancelRequested:
				// A dead compensation is a manual decision; do not wait for
				// the rest of the chain.
				w.State = driver.DAGSuspended
				w.UpdatedAt = now
				settled++
			case terminal:
				if w.cancelRequested {
					w.State = driver.DAGCancelled
				} else {
					w.State = driver.DAGFailed
				}
				w.CompletedAt = now
				w.UpdatedAt = now
				settled++
			}

		default:
		}
	}
	return settled, nil
}

// --- results ---------------------------------------------------------------

// TaskResults returns the persisted results of the workflow's succeeded tasks,
// keyed by task key; an empty keys slice selects every succeeded task.
func (f *Fake) TaskResults(_ context.Context, dagID uuid.UUID, keys []string) (map[string]json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	want := sliceSet(keys)
	out := map[string]json.RawMessage{}
	for _, j := range f.dagJobsLocked(dagID) {
		if j.State != driver.StateSucceeded {
			continue
		}
		if len(keys) > 0 && !want[j.TaskKey] {
			continue
		}
		out[j.TaskKey] = clonePayload(j.Result)
	}
	return out, nil
}

// AckTaskResult completes an active task like Ack and persists its result
// atomically. Same lease-token fencing.
func (f *Fake) AckTaskResult(_ context.Context, id, leaseToken uuid.UUID, result json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, err := f.settle("ack task result", id, leaseToken)
	if err != nil {
		return err
	}
	now := f.now()
	j.State = driver.StateSucceeded
	j.Result = clonePayload(result)
	j.LeaseToken = uuid.Nil
	j.LeaseUntil = time.Time{}
	j.CompletedAt = now
	f.bumpStat(j.Source, j.Kind, statProcessed, 1, now)
	return nil
}

// --- admin / manager verbs -------------------------------------------------

// cloneView projects the workflow header defensively. Callers hold f.mu.
func (w *fakeDAG) cloneView() driver.DAGView {
	out := w.DAGView
	out.Meta = cloneMeta(w.Meta)
	return out
}

// GetDAG returns one workflow header by id.
func (f *Fake) GetDAG(_ context.Context, id uuid.UUID) (*driver.DAGView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.dags[id]
	if w == nil {
		return nil, driver.NewNotFound("get dag")
	}
	view := w.cloneView()
	return &view, nil
}

// ListDAGs lists dags matching the filter, newest first, paginated.
func (f *Fake) ListDAGs(_ context.Context, filter driver.DAGFilter, offset, limit int) ([]driver.DAGView, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var matched []*fakeDAG
	for _, w := range f.dags {
		if filter.Name != "" && w.Name != filter.Name {
			continue
		}
		if filter.State != "" && w.State != filter.State {
			continue
		}
		matched = append(matched, w)
	}
	slices.SortFunc(matched, func(a, b *fakeDAG) int {
		if c := b.CreatedAt.Compare(a.CreatedAt); c != 0 { // newest first
			return c
		}
		return bytes.Compare(b.ID[:], a.ID[:])
	})
	total := int64(len(matched))

	if offset < 0 {
		offset = 0
	}
	if offset > len(matched) {
		offset = len(matched)
	}
	matched = matched[offset:]
	if limit > 0 && limit < len(matched) {
		matched = matched[:limit]
	}
	out := make([]driver.DAGView, 0, len(matched))
	for _, w := range matched {
		out = append(out, w.cloneView())
	}
	return out, total, nil
}

// DAGTasks returns the workflow's task jobs in creation order.
func (f *Fake) DAGTasks(_ context.Context, id uuid.UUID) ([]driver.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dags[id] == nil {
		return nil, driver.NewNotFound("dag tasks")
	}
	jobs := f.dagJobsLocked(id)
	out := make([]driver.Job, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.toJob())
	}
	return out, nil
}

// RetryDAG resumes a non-terminal workflow after failures. On a workflow
// with a compensation chain only the dead compensation tasks are reset and the
// workflow resumes compensating (original tasks never rerun once compensation
// started); otherwise every dead task is reset with a fresh budget and a
// suspended workflow resumes running.
func (f *Fake) RetryDAG(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.dags[id]
	if w == nil || w.terminal() {
		return driver.NewNotFound("retry dag")
	}
	now := f.now()
	hasComps := false
	for _, j := range f.dagJobsLocked(id) {
		if strings.HasPrefix(j.TaskKey, driver.TaskKeyCompensationPrefix) {
			hasComps = true
			break
		}
	}
	for _, j := range f.dagJobsLocked(id) {
		if j.State != driver.StateDead {
			continue
		}
		if hasComps && !strings.HasPrefix(j.TaskKey, driver.TaskKeyCompensationPrefix) {
			continue
		}
		f.resetToPending(j)
	}
	if w.State == driver.DAGSuspended {
		if hasComps {
			w.State = driver.DAGCompensating
		} else {
			w.State = driver.DAGRunning
			w.FailureReason = ""
		}
	}
	w.UpdatedAt = now
	return nil
}

// CompensateDAG manually triggers compensation on a running or suspended
// workflow, exactly like the cancel policy.
func (f *Fake) CompensateDAG(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.dags[id]
	if w == nil || (w.State != driver.DAGRunning && w.State != driver.DAGSuspended) {
		return driver.NewNotFound("compensate dag")
	}
	now := f.now()
	f.cancelRemainingTasksLocked(w, now)
	if f.insertCompensationsLocked(w, now) == 0 {
		w.State = driver.DAGFailed
		w.CompletedAt = now
	} else {
		w.State = driver.DAGCompensating
	}
	w.UpdatedAt = now
	return nil
}

// CancelDAG cancels a non-terminal workflow without compensating. On a
// compensating workflow the not-yet-started compensations are cancelled, the
// origin is recorded, and CompleteDAGs lands the workflow on cancelled
// once the in-flight compensations settle; otherwise the workflow lands on
// cancelled immediately.
func (f *Fake) CancelDAG(_ context.Context, id uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.dags[id]
	if w == nil || w.terminal() {
		return driver.NewNotFound("cancel dag")
	}
	now := f.now()
	w.cancelRequested = true
	f.cancelRemainingTasksLocked(w, now)
	if w.State != driver.DAGCompensating {
		w.State = driver.DAGCancelled
		w.CompletedAt = now
	}
	w.UpdatedAt = now
	return nil
}

// VacuumDAGs deletes terminal dags completed before retention ago,
// cascading to their task jobs, attempt history and dependency edges.
func (f *Fake) VacuumDAGs(_ context.Context, retention time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if retention <= 0 {
		return 0, nil
	}
	cutoff := f.now().Add(-retention)
	var removed int64
	for id, w := range f.dags {
		if !w.terminal() || w.CompletedAt.IsZero() || !w.CompletedAt.Before(cutoff) {
			continue
		}
		for jobID, j := range f.jobs {
			if j.Source == driver.SourceDAG && j.DAGID == id {
				delete(f.jobs, jobID)
				delete(f.attempts, jobID)
			}
		}
		delete(f.dags, id)
		removed++
	}
	return removed, nil
}
