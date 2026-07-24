package azyncpgx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// The workflow capability of the pgx Store: the SQL implementation of
// driver.WorkflowStore and driver.TxWorkflowStore[pgx.Tx]. It operates the
// azync_workflows header table, the azync_workflow_deps edge table and the
// source='workflow' rows of azync_jobs.
//
// $sleep timer encoding: a KindSleep task carries its SleepFor duration in its
// job payload as {"sleepSeconds": <float>}. CreateWorkflow writes it there
// (overwriting any user payload, which a timer has no handler to consume) so
// PromoteUnblocked can resolve the timer's run_at = now() + that many seconds
// against the backend clock when the task is released, exactly as a root timer
// resolves it at creation.

// --- creation --------------------------------------------------------------

const insertWorkflowSQL = `
INSERT INTO azync_workflows
	(id, name, state, on_failure, idempotency_key, meta, trace_id, span_id, trace_flags, created_at, updated_at)
VALUES ($1, $2, 'running',
	CASE WHEN $3 = 'suspend' THEN 'suspend' ELSE 'cancel' END,
	NULLIF($4, ''), $5::jsonb, NULLIF($6, ''), NULLIF($7, ''), $8, now(), now())
ON CONFLICT (name, idempotency_key)
	WHERE idempotency_key IS NOT NULL AND state IN ('running', 'suspended', 'compensating')
DO NOTHING
RETURNING id`

const selectLiveWorkflowSQL = `
SELECT id FROM azync_workflows
WHERE name = $1 AND idempotency_key = $2 AND state IN ('running', 'suspended', 'compensating')`

// insertWorkflowTaskSQL inserts one task job. State is resolved by the caller and
// run_at is computed DB-side: a root $sleep starts its timer at now()+SleepFor,
// every other task runs at now().
const insertWorkflowTaskSQL = `
INSERT INTO azync_jobs
	(id, source, kind, state, run_at, max_attempts, max_attempts_explicit,
	 payload, meta, trace_id, span_id, trace_flags, enqueued_at,
	 workflow_id, task_key, compensation_kind, compensation_payload, signal_name, ignore_dead_deps)
VALUES ($1, 'workflow', $2, $3, now() + make_interval(secs => $4), $5, $6,
	$7::jsonb, $8::jsonb, NULLIF($9, ''), NULLIF($10, ''), $11, now(),
	$12, $13, NULLIF($14, ''), $15::jsonb, NULLIF($16, ''), $17)`

const insertDepSQL = `
INSERT INTO azync_workflow_deps (workflow_id, task_key, depends_on_key) VALUES ($1, $2, $3)`

// CreateWorkflow atomically inserts the workflow header, its tasks and its
// dependency edges, and signals workers for immediately runnable tasks, in one
// transaction. It deduplicates by (Name, IdempotencyKey) against live
// executions, returning (false, existingID) without inserting anything when a
// live execution already holds the key.
func (s *Store) CreateWorkflow(ctx context.Context, p driver.WorkflowParams) (bool, uuid.UUID, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: create workflow begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	inserted, existingID, err := s.createWorkflow(ctx, tx, p)
	if err != nil {
		return false, uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: create workflow commit: %w", err)
	}
	return inserted, existingID, nil
}

// CreateWorkflowTx performs CreateWorkflow within the caller's transaction so the
// workflow commits atomically with the caller's own writes.
func (s *Store) CreateWorkflowTx(ctx context.Context, tx pgx.Tx, p driver.WorkflowParams) (bool, uuid.UUID, error) {
	return s.createWorkflow(ctx, tx, p)
}

func (s *Store) createWorkflow(ctx context.Context, q querier, p driver.WorkflowParams) (bool, uuid.UUID, error) {
	metaJSON, err := json.Marshal(orEmptyMeta(p.Meta))
	if err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: marshal workflow meta: %w", err)
	}

	// Insert the header. The ON CONFLICT targets only the live-idempotency
	// index; a duplicate id violates the PRIMARY KEY (a different constraint),
	// which raises an error rather than DO NOTHING — never a silent overwrite.
	var id pgtype.UUID
	err = q.QueryRow(ctx, insertWorkflowSQL,
		p.ID, p.Name, string(p.OnFailure), p.IdempotencyKey, string(metaJSON),
		p.TraceID, p.SpanID, nullableTraceFlags(p.TraceID, p.TraceFlags),
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// DO NOTHING fired: a live execution holds (Name, IdempotencyKey). This
		// only happens for a non-empty key, since the partial index excludes NULL
		// keys, so the SELECT below always finds exactly one live row.
		var existing pgtype.UUID
		if err := q.QueryRow(ctx, selectLiveWorkflowSQL, p.Name, p.IdempotencyKey).Scan(&existing); err != nil {
			return false, uuid.Nil, fmt.Errorf("azyncpgx: resolve deduplicated workflow: %w", err)
		}
		return false, toUUID(existing), nil
	}
	if err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: insert workflow: %w", err)
	}

	// A task is born blocked when it has dependencies; otherwise its runnable
	// state is dictated by kind.
	hasDeps := make(map[string]bool, len(p.Deps))
	for _, d := range p.Deps {
		hasDeps[d.TaskKey] = true
	}

	notifyKinds := map[string]struct{}{}
	for _, tk := range p.Tasks {
		state := initialTaskState(tk.Kind, hasDeps[tk.Key])

		// The $sleep timer carries its duration in the payload; every other task
		// keeps its opaque handler payload. Only a root timer (born scheduled)
		// starts at creation — a blocked one keeps run_at = now() and
		// PromoteUnblocked resolves its timer when the task is released.
		var runAtOffsetSecs float64
		payload := nullableRawJSON(tk.Payload)
		if tk.Kind == driver.KindSleep {
			enc, err := json.Marshal(sleepPayload{SleepSeconds: tk.SleepFor.Seconds()})
			if err != nil {
				return false, uuid.Nil, fmt.Errorf("azyncpgx: marshal sleep payload: %w", err)
			}
			payload = string(enc)
			if state == string(driver.StateScheduled) {
				runAtOffsetSecs = tk.SleepFor.Seconds()
			}
		}

		if _, err := q.Exec(ctx, insertWorkflowTaskSQL,
			uuid.New(), tk.Kind, state, runAtOffsetSecs, tk.MaxAttempts, tk.MaxAttempts > 0,
			payload, string(metaJSON), p.TraceID, p.SpanID, nullableTraceFlags(p.TraceID, p.TraceFlags),
			p.ID, tk.Key, tk.CompensationKind, nullableRawJSON(tk.CompensationPayload),
			tk.SignalName, tk.IgnoreDeadDeps,
		); err != nil {
			return false, uuid.Nil, fmt.Errorf("azyncpgx: insert workflow task %q: %w", tk.Key, err)
		}
		if err := s.bumpStat(ctx, q, driver.SourceWorkflow, tk.Kind, statEnqueued, 1); err != nil {
			return false, uuid.Nil, err
		}
		if state == string(driver.StatePending) {
			notifyKinds[tk.Kind] = struct{}{}
		}
	}

	for _, d := range p.Deps {
		if _, err := q.Exec(ctx, insertDepSQL, p.ID, d.TaskKey, d.DependsOnKey); err != nil {
			return false, uuid.Nil, fmt.Errorf("azyncpgx: insert workflow dep %s->%s: %w", d.TaskKey, d.DependsOnKey, err)
		}
	}

	if err := s.notifyWorkflowKinds(ctx, q, notifyKinds); err != nil {
		return false, uuid.Nil, err
	}
	return true, uuid.Nil, nil
}

// initialTaskState resolves a task's initial state: blocked when it has
// dependencies; otherwise pending, except the internal kinds (a root $sleep is
// scheduled with its timer started, a root $signal waits).
func initialTaskState(kind string, hasDeps bool) string {
	if hasDeps {
		return string(driver.StateBlocked)
	}
	switch kind {
	case driver.KindSleep:
		return string(driver.StateScheduled)
	case driver.KindSignal:
		return string(driver.StateWaiting)
	default:
		return string(driver.StatePending)
	}
}

// sleepPayload is the on-disk encoding of a $sleep task's duration.
type sleepPayload struct {
	SleepSeconds float64 `json:"sleepSeconds"`
}

// --- scheduler -------------------------------------------------------------

// signalSQL completes waiting $signal tasks named $2 (payload as result) and
// wakes scheduled $sleep timers named $2 early. The two UPDATEs touch disjoint
// (kind, state) rows, so no row is double-counted.
const signalSQL = `
WITH sig AS (
	UPDATE azync_jobs SET
		state = 'succeeded', result = $3::jsonb, completed_at = now(), updated_at = now()
	WHERE source = 'workflow' AND workflow_id = $1 AND signal_name = $2
		AND kind = '$signal' AND state = 'waiting'
	RETURNING 1
),
slp AS (
	UPDATE azync_jobs SET run_at = now(), updated_at = now()
	WHERE source = 'workflow' AND workflow_id = $1 AND signal_name = $2
		AND kind = '$sleep' AND state = 'scheduled'
	RETURNING 1
)
SELECT (SELECT count(*) FROM sig) + (SELECT count(*) FROM slp)`

// Signal delivers a named signal to one workflow: every waiting $signal task
// completes as succeeded with the payload as its result, and every scheduled
// $sleep timer of the name is woken early. It returns the number affected.
func (s *Store) Signal(ctx context.Context, workflowID uuid.UUID, name string, payload json.RawMessage) (int64, error) {
	var matched int64
	if err := s.pool.QueryRow(ctx, signalSQL, workflowID, name, nullableRawJSON(payload)).Scan(&matched); err != nil {
		return 0, fmt.Errorf("azyncpgx: signal: %w", err)
	}
	return matched, nil
}

// promoteUnblockedSQL releases blocked tasks whose dependencies are all
// satisfied, into the runnable state their kind dictates. A dependency is
// satisfied when it succeeded; a task with ignore_dead_deps also tolerates dead
// or cancelled dependencies. Only running and compensating workflows promote. A
// dependency edge whose task is missing keeps the task blocked (the inner NOT
// EXISTS finds no satisfying row).
const promoteUnblockedSQL = `
UPDATE azync_jobs t SET
	state = CASE t.kind WHEN '$signal' THEN 'waiting' WHEN '$sleep' THEN 'scheduled' ELSE 'pending' END,
	run_at = CASE
		WHEN t.kind = '$sleep' THEN now() + make_interval(secs => COALESCE((t.payload->>'sleepSeconds')::double precision, 0))
		WHEN t.kind = '$signal' THEN t.run_at
		ELSE now() END,
	updated_at = now()
FROM azync_workflows w
WHERE t.source = 'workflow' AND t.state = 'blocked'
	AND w.id = t.workflow_id AND w.state IN ('running', 'compensating')
	AND NOT EXISTS (
		SELECT 1 FROM azync_workflow_deps d
		WHERE d.workflow_id = t.workflow_id AND d.task_key = t.task_key
			AND NOT EXISTS (
				SELECT 1 FROM azync_jobs dep
				WHERE dep.workflow_id = d.workflow_id AND dep.task_key = d.depends_on_key
					AND dep.source = 'workflow'
					AND (dep.state = 'succeeded' OR (t.ignore_dead_deps AND dep.state IN ('dead', 'cancelled')))
			)
	)
RETURNING t.kind, (t.kind <> '$sleep' AND t.kind <> '$signal') AS became_pending`

// PromoteUnblocked moves every blocked task whose dependencies are all satisfied
// to its runnable state, waking workers for the newly pending tasks.
func (s *Store) PromoteUnblocked(ctx context.Context) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: promote unblocked begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, promoteUnblockedSQL)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: promote unblocked: %w", err)
	}
	var promoted int64
	notifyKinds := map[string]struct{}{}
	for rows.Next() {
		var (
			kind    string
			pending bool
		)
		if err := rows.Scan(&kind, &pending); err != nil {
			rows.Close()
			return 0, fmt.Errorf("azyncpgx: scan promoted task: %w", err)
		}
		promoted++
		if pending {
			notifyKinds[kind] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("azyncpgx: iterate promoted tasks: %w", err)
	}
	rows.Close()

	if err := s.notifyWorkflowKinds(ctx, tx, notifyKinds); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("azyncpgx: promote unblocked commit: %w", err)
	}
	return promoted, nil
}

const completeDueSleepsSQL = `
UPDATE azync_jobs t SET state = 'succeeded', completed_at = now(), updated_at = now()
FROM azync_workflows w
WHERE t.source = 'workflow' AND t.kind = '$sleep' AND t.state = 'scheduled' AND t.run_at <= now()
	AND w.id = t.workflow_id AND w.state = 'running'`

// CompleteDueSleeps marks every scheduled $sleep timer of a running workflow
// whose run_at is due as succeeded, without running any handler.
func (s *Store) CompleteDueSleeps(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, completeDueSleepsSQL)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: complete due sleeps: %w", err)
	}
	return tag.RowsAffected(), nil
}

// --- failure policy --------------------------------------------------------

// triggeringDeadTasksSQL returns, per running workflow, the keys of its dead
// tasks that trigger the failure policy. A dead task triggers unless it has at
// least one dependent and every dependent declares ignore_dead_deps: the first
// disjunct fires on a dead leaf (no dependents), the second on any dependent
// that does not tolerate the death (missing or ignore_dead_deps = false).
const triggeringDeadTasksSQL = `
SELECT w.id, w.on_failure, t.task_key
FROM azync_workflows w
JOIN azync_jobs t ON t.workflow_id = w.id AND t.source = 'workflow' AND t.state = 'dead'
WHERE w.state = 'running'
	AND (
		NOT EXISTS (SELECT 1 FROM azync_workflow_deps d WHERE d.workflow_id = w.id AND d.depends_on_key = t.task_key)
		OR EXISTS (
			SELECT 1 FROM azync_workflow_deps d
			WHERE d.workflow_id = w.id AND d.depends_on_key = t.task_key
				AND NOT EXISTS (
					SELECT 1 FROM azync_jobs dep
					WHERE dep.workflow_id = d.workflow_id AND dep.task_key = d.task_key
						AND dep.source = 'workflow' AND dep.ignore_dead_deps
				)
		)
	)`

// failedWorkflow accumulates one workflow's triggering dead keys and policy.
type failedWorkflow struct {
	policy   driver.OnFailurePolicy
	deadKeys []string
}

// ApplyFailurePolicy applies each running workflow's OnFailure policy when it has
// at least one triggering dead task, in one transaction. This runs after
// PromoteUnblocked/CompleteDueSleeps and before CompleteWorkflows on the worker's
// tick: a workflow whose dead task triggers is moved out of 'running' here, so
// only tolerated deaths remain for CompleteWorkflows to settle.
//
// Concurrency: triggeringDeadTasksSQL reads without a lock, so two concurrent
// ticks (or a tick racing a manual CompensateWorkflow/CancelWorkflow) can both
// observe the same workflow as 'running' under READ COMMITTED. The
// cancel/compensate branch below re-acquires the workflow row with
// lockWorkflowForUpdate and re-checks its state before touching any task rows:
// the loser of the race sees a state that already left 'running' and skips
// cleanly instead of racing insertCompensations's guard, which would otherwise
// violate the (workflow_id, task_key) unique index.
func (s *Store) ApplyFailurePolicy(ctx context.Context) ([]driver.WorkflowFailure, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: apply failure policy begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, triggeringDeadTasksSQL)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: find triggering dead tasks: %w", err)
	}
	acting := map[uuid.UUID]*failedWorkflow{}
	var order []uuid.UUID
	for rows.Next() {
		var (
			id      pgtype.UUID
			policy  string
			taskKey string
		)
		if err := rows.Scan(&id, &policy, &taskKey); err != nil {
			rows.Close()
			return nil, fmt.Errorf("azyncpgx: scan triggering dead task: %w", err)
		}
		wid := toUUID(id)
		fw := acting[wid]
		if fw == nil {
			fw = &failedWorkflow{policy: driver.OnFailurePolicy(policy)}
			acting[wid] = fw
			order = append(order, wid)
		}
		fw.deadKeys = append(fw.deadKeys, taskKey)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("azyncpgx: iterate triggering dead tasks: %w", err)
	}
	rows.Close()

	// Deterministic workflow order keeps concurrent passes from bumping the same
	// stat rows in opposite orders.
	slices.SortFunc(order, func(a, b uuid.UUID) int { return strings.Compare(a.String(), b.String()) })

	var out []driver.WorkflowFailure
	for _, wid := range order {
		fw := acting[wid]
		slices.Sort(fw.deadKeys)
		reason := deadTasksReason(fw.deadKeys)

		if fw.policy == driver.OnFailureSuspend {
			// The state re-check in the WHERE guards the same race the cancel
			// branch resolves with lockWorkflowForUpdate: a concurrent manual
			// CompensateWorkflow/CancelWorkflow that landed after this pass's
			// unlocked read must not be overwritten with 'suspended'.
			tag, err := tx.Exec(ctx,
				`UPDATE azync_workflows SET state = 'suspended', failure_reason = $2, updated_at = now() WHERE id = $1 AND state = 'running'`,
				wid, reason)
			if err != nil {
				return nil, fmt.Errorf("azyncpgx: suspend workflow: %w", err)
			}
			if tag.RowsAffected() == 0 {
				continue // the workflow already left running; nothing to report
			}
		} else {
			state, err := s.lockWorkflowForUpdate(ctx, tx, wid)
			if err != nil {
				return nil, err
			}
			if state != string(driver.WorkflowRunning) {
				// A concurrent tick already moved this workflow out of
				// running (it won the compensation-insert race, or a manual
				// CompensateWorkflow/CancelWorkflow call landed first):
				// nothing left for this tick to do.
				continue
			}
			if err := s.cancelRemainingTasks(ctx, tx, wid); err != nil {
				return nil, err
			}
			comps, err := s.insertCompensations(ctx, tx, wid)
			if err != nil {
				return nil, err
			}
			if comps == 0 {
				if _, err := tx.Exec(ctx, `UPDATE azync_workflows SET state = 'failed', failure_reason = $2, completed_at = now(), updated_at = now() WHERE id = $1`,
					wid, reason); err != nil {
					return nil, fmt.Errorf("azyncpgx: fail workflow: %w", err)
				}
			} else {
				if _, err := tx.Exec(ctx, `UPDATE azync_workflows SET state = 'compensating', failure_reason = $2, updated_at = now() WHERE id = $1`,
					wid, reason); err != nil {
					return nil, fmt.Errorf("azyncpgx: compensate workflow: %w", err)
				}
			}
		}
		out = append(out, driver.WorkflowFailure{WorkflowID: wid, Policy: fw.policy, DeadTasks: fw.deadKeys})
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("azyncpgx: apply failure policy commit: %w", err)
	}
	return out, nil
}

// deadTasksReason renders the FailureReason from sorted dead task keys.
func deadTasksReason(keys []string) string {
	return "dead tasks: " + strings.Join(keys, ", ")
}

// lockWorkflowForUpdate takes the row lock on the workflow header (FOR UPDATE)
// and returns its current state. Callers on the cancel/compensate path
// (ApplyFailurePolicy, CompensateWorkflow) take this lock before touching any
// task row and re-check the returned state against what they expect: it is the
// serialization point that makes concurrent ticks on the same workflow safe,
// since the compensation-insert guard in insertCompensations is a plain SELECT
// count(*) that is not by itself race-free under READ COMMITTED. Callers run
// inside a transaction; the lock releases on commit or rollback.
func (s *Store) lockWorkflowForUpdate(ctx context.Context, q querier, id uuid.UUID) (string, error) {
	var state string
	if err := q.QueryRow(ctx, `SELECT state FROM azync_workflows WHERE id = $1 FOR UPDATE`, id).Scan(&state); err != nil {
		return "", fmt.Errorf("azyncpgx: lock workflow: %w", err)
	}
	return state, nil
}

const cancelRemainingTasksSQL = `
UPDATE azync_jobs SET state = 'cancelled', completed_at = now(), updated_at = now()
WHERE workflow_id = $1 AND source = 'workflow' AND state IN ('pending', 'scheduled', 'blocked', 'waiting')`

// cancelRemainingTasks cancels the workflow's non-terminal, non-active tasks. An
// active task keeps its lease and settles on its own.
func (s *Store) cancelRemainingTasks(ctx context.Context, q querier, workflowID uuid.UUID) error {
	if _, err := q.Exec(ctx, cancelRemainingTasksSQL, workflowID); err != nil {
		return fmt.Errorf("azyncpgx: cancel remaining tasks: %w", err)
	}
	return nil
}

const compensationCandidatesSQL = `
SELECT task_key, compensation_kind
FROM azync_jobs
WHERE workflow_id = $1 AND source = 'workflow' AND state = 'succeeded'
	AND compensation_kind IS NOT NULL AND compensation_kind <> ''
	AND task_key NOT LIKE 'comp:%'
ORDER BY completed_at DESC, created_at DESC, id DESC`

// insertCompensationTaskSQL clones the original succeeded task's meta, trace and
// declared compensation payload into a fresh comp:<key> task of the declared
// compensation kind. The compensation is itself uncompensated and signal-free.
const insertCompensationTaskSQL = `
INSERT INTO azync_jobs
	(id, source, kind, state, run_at, max_attempts, max_attempts_explicit,
	 payload, meta, trace_id, span_id, trace_flags, enqueued_at, workflow_id, task_key)
SELECT gen_random_uuid(), 'workflow', o.compensation_kind, $3, now(),
	o.max_attempts, o.max_attempts_explicit, o.compensation_payload, o.meta,
	o.trace_id, o.span_id, o.trace_flags, now(), o.workflow_id, $4
FROM azync_jobs o
WHERE o.workflow_id = $1 AND o.source = 'workflow' AND o.task_key = $2`

// insertCompensations inserts the compensation chain: one comp:<key> task per
// succeeded task that declared a compensation, in reverse completion order,
// chained through dependency edges (newest completion first and pending, the
// rest blocked on their predecessor). It returns the total number of
// compensation tasks the workflow has; a workflow that already carries a chain
// is left untouched (guard against double insertion after a policy pass followed
// by a manual compensate). This guard is a plain SELECT count(*) and is not by
// itself race-free under READ COMMITTED against a second concurrent
// cancel/compensate transaction on the same workflow; callers MUST hold the
// workflow row lock from lockWorkflowForUpdate before calling this, which
// serializes the two and makes the guard authoritative. Callers run inside a
// transaction.
func (s *Store) insertCompensations(ctx context.Context, q querier, workflowID uuid.UUID) (int, error) {
	var existing int
	if err := q.QueryRow(ctx,
		`SELECT count(*) FROM azync_jobs WHERE workflow_id = $1 AND source = 'workflow' AND task_key LIKE 'comp:%'`,
		workflowID).Scan(&existing); err != nil {
		return 0, fmt.Errorf("azyncpgx: count existing compensations: %w", err)
	}
	if existing > 0 {
		return existing, nil
	}

	rows, err := q.Query(ctx, compensationCandidatesSQL, workflowID)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: compensation candidates: %w", err)
	}
	type candidate struct{ taskKey, compKind string }
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.taskKey, &c.compKind); err != nil {
			rows.Close()
			return 0, fmt.Errorf("azyncpgx: scan compensation candidate: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("azyncpgx: iterate compensation candidates: %w", err)
	}
	rows.Close()

	prevKey := ""
	for i, c := range candidates {
		compKey := driver.TaskKeyCompensationPrefix + c.taskKey
		state := string(driver.StateBlocked)
		if i == 0 {
			state = string(driver.StatePending)
		}
		if _, err := q.Exec(ctx, insertCompensationTaskSQL, workflowID, c.taskKey, state, compKey); err != nil {
			return 0, fmt.Errorf("azyncpgx: insert compensation %q: %w", compKey, err)
		}
		if i > 0 {
			if _, err := q.Exec(ctx, insertDepSQL, workflowID, compKey, prevKey); err != nil {
				return 0, fmt.Errorf("azyncpgx: chain compensation %q: %w", compKey, err)
			}
		}
		if err := s.bumpStat(ctx, q, driver.SourceWorkflow, c.compKind, statEnqueued, 1); err != nil {
			return 0, err
		}
		if i == 0 {
			if err := s.notifyWorkflowKinds(ctx, q, map[string]struct{}{c.compKind: {}}); err != nil {
				return 0, err
			}
		}
		prevKey = compKey
	}
	return len(candidates), nil
}

// --- completion ------------------------------------------------------------

// completeRunningSucceededSQL settles a running workflow whose tasks are all
// succeeded (none dead) to succeeded.
const completeRunningSucceededSQL = `
UPDATE azync_workflows w SET state = 'succeeded', completed_at = now(), updated_at = now()
WHERE w.state = 'running'
	AND EXISTS (SELECT 1 FROM azync_jobs j WHERE j.workflow_id = w.id AND j.source = 'workflow')
	AND NOT EXISTS (SELECT 1 FROM azync_jobs j WHERE j.workflow_id = w.id AND j.source = 'workflow' AND j.state <> 'succeeded')`

// completeRunningFailedSQL settles a running workflow whose tasks are all
// terminal (succeeded or dead) with at least one dead — every dead task
// tolerated, so the policy never triggered — to failed, recording the sorted
// dead keys. The final NOT EXISTS re-checks tolerance directly instead of
// trusting the worker's tick order: a dead task is tolerated iff it has at least
// one dependent and every dependent declares ignore_dead_deps (the same
// predicate as triggeringDeadTasksSQL, negated). If the workflow carries any
// NON-tolerated dead task — a dead leaf (no dependents), or a dead task some
// dependent does not tolerate — it is left running for ApplyFailurePolicy to run
// its OnFailure policy (this tick or the next). This closes the race where a
// task dies in the window between the separate ApplyFailurePolicy and
// CompleteWorkflows transactions, which would otherwise settle a Cancel-policy
// workflow failed with its compensations skipped. The succeeded branch
// (completeRunningSucceededSQL) needs no such guard: it requires every task
// succeeded, so no dead task can be present.
const completeRunningFailedSQL = `
UPDATE azync_workflows w SET state = 'failed', failure_reason = r.reason, completed_at = now(), updated_at = now()
FROM (
	SELECT j.workflow_id, 'dead tasks: ' || string_agg(j.task_key, ', ' ORDER BY j.task_key) AS reason
	FROM azync_jobs j
	JOIN azync_workflows w2 ON w2.id = j.workflow_id AND w2.state = 'running'
	WHERE j.source = 'workflow' AND j.state = 'dead'
		AND NOT EXISTS (
			SELECT 1 FROM azync_jobs j2
			WHERE j2.workflow_id = j.workflow_id AND j2.source = 'workflow'
				AND j2.state <> 'succeeded' AND j2.state <> 'dead'
		)
		AND NOT EXISTS (
			SELECT 1 FROM azync_jobs jd
			WHERE jd.workflow_id = j.workflow_id AND jd.source = 'workflow' AND jd.state = 'dead'
				AND (
					NOT EXISTS (SELECT 1 FROM azync_workflow_deps d WHERE d.workflow_id = jd.workflow_id AND d.depends_on_key = jd.task_key)
					OR EXISTS (
						SELECT 1 FROM azync_workflow_deps d
						WHERE d.workflow_id = jd.workflow_id AND d.depends_on_key = jd.task_key
							AND NOT EXISTS (
								SELECT 1 FROM azync_jobs dep
								WHERE dep.workflow_id = d.workflow_id AND dep.task_key = d.task_key
									AND dep.source = 'workflow' AND dep.ignore_dead_deps
							)
					)
				)
		)
	GROUP BY j.workflow_id
) r
WHERE w.id = r.workflow_id AND w.state = 'running'`

// completeCompensatingSuspendSQL parks a compensating workflow with a dead
// compensation as suspended for a manual decision — unless the compensation was
// triggered through CancelWorkflow, which rides the chain to cancelled.
const completeCompensatingSuspendSQL = `
UPDATE azync_workflows w SET state = 'suspended', updated_at = now()
WHERE w.state = 'compensating' AND w.cancel_requested = false
	AND EXISTS (
		SELECT 1 FROM azync_jobs j
		WHERE j.workflow_id = w.id AND j.source = 'workflow' AND j.task_key LIKE 'comp:%' AND j.state = 'dead'
	)`

// completeCompensatingSettleSQL settles a compensating workflow whose
// compensation tasks are all terminal: cancelled when the compensation was
// triggered through CancelWorkflow, otherwise failed. Suspend-on-dead workflows
// have already left 'compensating' via completeCompensatingSuspendSQL.
const completeCompensatingSettleSQL = `
UPDATE azync_workflows w SET
	state = CASE WHEN w.cancel_requested THEN 'cancelled' ELSE 'failed' END,
	completed_at = now(), updated_at = now()
WHERE w.state = 'compensating'
	AND EXISTS (SELECT 1 FROM azync_jobs j WHERE j.workflow_id = w.id AND j.source = 'workflow' AND j.task_key LIKE 'comp:%')
	AND NOT EXISTS (
		SELECT 1 FROM azync_jobs j
		WHERE j.workflow_id = w.id AND j.source = 'workflow' AND j.task_key LIKE 'comp:%'
			AND j.state NOT IN ('succeeded', 'cancelled', 'dead')
	)`

// CompleteWorkflows settles workflows whose work is finished, running the four
// disjoint transitions in one transaction (suspend-on-dead-compensation before
// the terminal compensation settle so the two never overlap).
func (s *Store) CompleteWorkflows(ctx context.Context) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: complete workflows begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var settled int64
	for _, sql := range []string{
		completeRunningSucceededSQL,
		completeRunningFailedSQL,
		completeCompensatingSuspendSQL,
		completeCompensatingSettleSQL,
	} {
		tag, err := tx.Exec(ctx, sql)
		if err != nil {
			return 0, fmt.Errorf("azyncpgx: complete workflows: %w", err)
		}
		settled += tag.RowsAffected()
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("azyncpgx: complete workflows commit: %w", err)
	}
	return settled, nil
}

// --- results ---------------------------------------------------------------

const taskResultsSQL = `
SELECT task_key, result::text
FROM azync_jobs
WHERE workflow_id = $1 AND source = 'workflow' AND state = 'succeeded'
	AND ($2 OR task_key = ANY($3::text[]))`

// TaskResults returns the persisted results of the workflow's succeeded tasks,
// keyed by task key, restricted to keys when non-empty. A succeeded task without
// a result maps to a nil value.
func (s *Store) TaskResults(ctx context.Context, workflowID uuid.UUID, keys []string) (map[string]json.RawMessage, error) {
	rows, err := s.pool.Query(ctx, taskResultsSQL, workflowID, len(keys) == 0, keys)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: task results: %w", err)
	}
	defer rows.Close()
	out := map[string]json.RawMessage{}
	for rows.Next() {
		var (
			key    string
			result *string
		)
		if err := rows.Scan(&key, &result); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan task result: %w", err)
		}
		if result == nil {
			out[key] = nil
		} else {
			out[key] = json.RawMessage(*result)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate task results: %w", err)
	}
	return out, nil
}

const ackTaskResultSQL = `
UPDATE azync_jobs
SET state = 'succeeded', result = $3::jsonb, lease_until = NULL, lease_token = NULL, completed_at = now(), updated_at = now()
WHERE id = $1 AND state = 'active' AND lease_token = $2
RETURNING source, kind`

// AckTaskResult completes an active task exactly like Ack and additionally
// persists result as the task's durable output, atomically. Same lease-token
// fencing: a stale token that no longer owns an active row is a not-found error.
func (s *Store) AckTaskResult(ctx context.Context, id, leaseToken uuid.UUID, result json.RawMessage) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("azyncpgx: ack task result begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var source, kind string
	err = tx.QueryRow(ctx, ackTaskResultSQL, id, leaseToken, nullableRawJSON(result)).Scan(&source, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.NewNotFound("ack task result")
	}
	if err != nil {
		return fmt.Errorf("azyncpgx: ack task result: %w", err)
	}
	if err := s.bumpStat(ctx, tx, driver.Source(source), kind, statProcessed, 1); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("azyncpgx: ack task result commit: %w", err)
	}
	return nil
}

// --- admin / manager verbs -------------------------------------------------

const workflowColumns = `
	id, name, state, on_failure, COALESCE(idempotency_key, ''), COALESCE(failure_reason, ''),
	meta::text, COALESCE(trace_id, ''), created_at, updated_at, completed_at`

const getWorkflowSQL = `SELECT ` + workflowColumns + ` FROM azync_workflows WHERE id = $1`

// GetWorkflow returns one workflow header by id, or a not-found error.
func (s *Store) GetWorkflow(ctx context.Context, id uuid.UUID) (*driver.WorkflowView, error) {
	rows, err := s.pool.Query(ctx, getWorkflowSQL, id)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: get workflow: %w", err)
	}
	views, err := scanWorkflowViews(rows)
	if err != nil {
		return nil, err
	}
	if len(views) == 0 {
		return nil, driver.NewNotFound("get workflow")
	}
	return &views[0], nil
}

// ListWorkflows lists workflows matching filter, newest first (created_at then
// id descending), paginated, with the total matching count.
func (s *Store) ListWorkflows(ctx context.Context, filter driver.WorkflowFilter, offset, limit int) ([]driver.WorkflowView, int64, error) {
	where := "TRUE"
	args := []any{}
	if filter.Name != "" {
		args = append(args, filter.Name)
		where += " AND name = $" + strconv.Itoa(len(args))
	}
	if filter.State != "" {
		args = append(args, string(filter.State))
		where += " AND state = $" + strconv.Itoa(len(args))
	}

	var total int64
	// where is built from fixed fragments and bound parameters only.
	//nolint:gosec // G202: no user-controlled SQL identifier is interpolated
	if err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM azync_workflows WHERE "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("azyncpgx: list workflows count: %w", err)
	}

	if offset < 0 {
		offset = 0
	}
	limitIdx := len(args) + 1
	offsetIdx := len(args) + 2
	args = append(args, limitArg(limit), offset)
	//nolint:gosec // G202: no user-controlled SQL identifier is interpolated
	sql := "SELECT " + workflowColumns + " FROM azync_workflows WHERE " + where +
		" ORDER BY created_at DESC, id DESC LIMIT $" + strconv.Itoa(limitIdx) + " OFFSET $" + strconv.Itoa(offsetIdx)
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("azyncpgx: list workflows: %w", err)
	}
	views, err := scanWorkflowViews(rows)
	if err != nil {
		return nil, 0, err
	}
	return views, total, nil
}

func scanWorkflowViews(rows pgx.Rows) ([]driver.WorkflowView, error) {
	defer rows.Close()
	var out []driver.WorkflowView
	for rows.Next() {
		var (
			view        driver.WorkflowView
			id          pgtype.UUID
			meta        string
			completedAt pgtype.Timestamptz
			state       string
			onFailure   string
		)
		if err := rows.Scan(
			&id, &view.Name, &state, &onFailure, &view.IdempotencyKey, &view.FailureReason,
			&meta, &view.TraceID, &view.CreatedAt, &view.UpdatedAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan workflow: %w", err)
		}
		view.ID = toUUID(id)
		view.State = driver.WorkflowState(state)
		view.OnFailure = driver.OnFailurePolicy(onFailure)
		view.CompletedAt = completedAt.Time
		m, err := decodeMeta(meta)
		if err != nil {
			return nil, fmt.Errorf("azyncpgx: decode workflow meta for %s: %w", view.ID, err)
		}
		view.Meta = m
		out = append(out, view)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate workflows: %w", err)
	}
	return out, nil
}

var workflowTasksSQL = `SELECT ` + jobColumns("azync_jobs") +
	` FROM azync_jobs WHERE workflow_id = $1 AND source = 'workflow' ORDER BY created_at, id`

// WorkflowTasks returns every task job of the workflow (compensation tasks
// included) ordered by created_at then id — tasks inserted in the same atomic
// batch share created_at, so their relative order is stable, not the
// declaration order. It returns a not-found error when the workflow does not
// exist.
func (s *Store) WorkflowTasks(ctx context.Context, id uuid.UUID) ([]driver.Job, error) {
	if err := s.requireWorkflow(ctx, "workflow tasks", id); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, workflowTasksSQL, id)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: workflow tasks: %w", err)
	}
	return collectJobs(rows)
}

// requireWorkflow maps a missing workflow header to the contract's not-found.
func (s *Store) requireWorkflow(ctx context.Context, op string, id uuid.UUID) error {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM azync_workflows WHERE id = $1)`, id).Scan(&exists); err != nil {
		return fmt.Errorf("azyncpgx: %s: %w", op, err)
	}
	if !exists {
		return driver.NewNotFound(op)
	}
	return nil
}

// resetDeadTasksSQL resets the workflow's dead tasks to a fresh pending state
// (attempt and reap_count cleared). When the workflow carries a compensation
// chain ($2 true) only the compensation tasks reset, so original tasks never
// rerun once compensation started.
const resetDeadTasksSQL = `
UPDATE azync_jobs SET
	state = 'pending', run_at = now(), attempt = 0, reap_count = 0,
	last_error = NULL, failed_at = NULL, updated_at = now()
WHERE workflow_id = $1 AND source = 'workflow' AND state = 'dead'
	AND (NOT $2 OR task_key LIKE 'comp:%')
RETURNING kind`

// resumeWorkflowSQL resumes a suspended workflow: to compensating when a
// compensation chain exists, otherwise to running with the failure cleared. A
// workflow in any other state keeps its state (only its updated_at advances).
const resumeWorkflowSQL = `
UPDATE azync_workflows SET
	state = CASE
		WHEN state = 'suspended' AND $2 THEN 'compensating'
		WHEN state = 'suspended' AND NOT $2 THEN 'running'
		ELSE state END,
	failure_reason = CASE WHEN state = 'suspended' AND NOT $2 THEN NULL ELSE failure_reason END,
	updated_at = now()
WHERE id = $1`

// RetryWorkflow resumes a non-terminal workflow after failures, resetting its
// dead tasks (or only its dead compensation tasks once a chain exists) and, for
// a suspended workflow, resuming it — to running, or back to compensating.
func (s *Store) RetryWorkflow(ctx context.Context, id uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("azyncpgx: retry workflow begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	hasComps, err := s.requireNonTerminalWorkflow(ctx, tx, "retry workflow", id)
	if err != nil {
		return err
	}

	rows, err := tx.Query(ctx, resetDeadTasksSQL, id, hasComps)
	if err != nil {
		return fmt.Errorf("azyncpgx: reset dead tasks: %w", err)
	}
	notifyKinds := map[string]struct{}{}
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			rows.Close()
			return fmt.Errorf("azyncpgx: scan reset task: %w", err)
		}
		notifyKinds[kind] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("azyncpgx: iterate reset tasks: %w", err)
	}
	rows.Close()

	if _, err := tx.Exec(ctx, resumeWorkflowSQL, id, hasComps); err != nil {
		return fmt.Errorf("azyncpgx: resume workflow: %w", err)
	}
	if err := s.notifyWorkflowKinds(ctx, tx, notifyKinds); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("azyncpgx: retry workflow commit: %w", err)
	}
	return nil
}

// CompensateWorkflow manually triggers compensation on a running or suspended
// workflow, exactly like the OnFailureCancel policy.
//
// Concurrency: the initial state read takes the row lock (FOR UPDATE) so it
// serializes against a concurrent ApplyFailurePolicy tick or another
// CompensateWorkflow call on the same workflow id. The loser blocks until the
// winner commits, then observes the post-commit state: no longer
// running/suspended, so it returns the same not-found this call already
// returns for a workflow that never qualified, instead of racing
// insertCompensations's guard into a unique-violation.
func (s *Store) CompensateWorkflow(ctx context.Context, id uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("azyncpgx: compensate workflow begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM azync_workflows WHERE id = $1 FOR UPDATE`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.NewNotFound("compensate workflow")
	}
	if err != nil {
		return fmt.Errorf("azyncpgx: compensate workflow: %w", err)
	}
	if state != string(driver.WorkflowRunning) && state != string(driver.WorkflowSuspended) {
		return driver.NewNotFound("compensate workflow")
	}

	if err := s.cancelRemainingTasks(ctx, tx, id); err != nil {
		return err
	}
	comps, err := s.insertCompensations(ctx, tx, id)
	if err != nil {
		return err
	}
	if comps == 0 {
		if _, err := tx.Exec(ctx, `UPDATE azync_workflows SET state = 'failed', completed_at = now(), updated_at = now() WHERE id = $1`, id); err != nil {
			return fmt.Errorf("azyncpgx: compensate settle failed: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `UPDATE azync_workflows SET state = 'compensating', updated_at = now() WHERE id = $1`, id); err != nil {
			return fmt.Errorf("azyncpgx: compensate settle compensating: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("azyncpgx: compensate workflow commit: %w", err)
	}
	return nil
}

// cancelWorkflowSQL marks cancel_requested and, unless the workflow is
// compensating (whose in-flight chain is allowed to settle first), lands it on
// cancelled immediately. The CASE references the pre-update state.
const cancelWorkflowSQL = `
UPDATE azync_workflows SET
	cancel_requested = true,
	state = CASE WHEN state = 'compensating' THEN state ELSE 'cancelled' END,
	completed_at = CASE WHEN state = 'compensating' THEN completed_at ELSE now() END,
	updated_at = now()
WHERE id = $1`

// CancelWorkflow cancels a non-terminal workflow without compensating: its
// non-terminal tasks are cancelled and the workflow becomes cancelled, except a
// compensating workflow, which keeps compensating until CompleteWorkflows lands
// it on cancelled.
//
// Concurrency: the initial state read takes the row lock (FOR UPDATE), so a
// cancel racing a scheduler pass (or another verb) that is settling the same
// workflow blocks until the winner commits and then observes the committed
// state — a workflow that just reached a terminal state is a not-found here,
// never flipped to cancelled after the fact.
func (s *Store) CancelWorkflow(ctx context.Context, id uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("azyncpgx: cancel workflow begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM azync_workflows WHERE id = $1 FOR UPDATE`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.NewNotFound("cancel workflow")
	}
	if err != nil {
		return fmt.Errorf("azyncpgx: cancel workflow: %w", err)
	}
	if isTerminalWorkflowState(state) {
		return driver.NewNotFound("cancel workflow")
	}

	if _, err := tx.Exec(ctx, cancelWorkflowSQL, id); err != nil {
		return fmt.Errorf("azyncpgx: cancel workflow update: %w", err)
	}
	if err := s.cancelRemainingTasks(ctx, tx, id); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("azyncpgx: cancel workflow commit: %w", err)
	}
	return nil
}

const vacuumWorkflowsSQL = `
DELETE FROM azync_workflows
WHERE state IN ('succeeded', 'failed', 'cancelled')
	AND completed_at IS NOT NULL
	AND completed_at < now() - make_interval(secs => $1)`

// VacuumWorkflows deletes terminal workflows completed before retention ago,
// cascading (via the FKs) to their task jobs and dependency edges. A retention
// <= 0 removes nothing.
func (s *Store) VacuumWorkflows(ctx context.Context, retention time.Duration) (int64, error) {
	if retention <= 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, vacuumWorkflowsSQL, retention.Seconds())
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: vacuum workflows: %w", err)
	}
	return tag.RowsAffected(), nil
}

// --- helpers ---------------------------------------------------------------

// requireNonTerminalWorkflow loads a workflow for a manager verb, mapping a
// missing or terminal workflow to not-found, and reports whether it carries a
// compensation chain. The read takes the row lock (FOR UPDATE) so the caller
// serializes against a concurrent policy pass or verb on the same workflow:
// both answers are stale the instant a racing transaction commits, and acting
// on them unlocked lets RetryWorkflow reset the original dead task after a
// concurrent ApplyFailurePolicy started compensation. Callers run inside a
// transaction; the lock releases on commit or rollback.
func (s *Store) requireNonTerminalWorkflow(ctx context.Context, q querier, op string, id uuid.UUID) (bool, error) {
	var state string
	err := q.QueryRow(ctx, `SELECT state FROM azync_workflows WHERE id = $1 FOR UPDATE`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, driver.NewNotFound(op)
	}
	if err != nil {
		return false, fmt.Errorf("azyncpgx: %s: %w", op, err)
	}
	if isTerminalWorkflowState(state) {
		return false, driver.NewNotFound(op)
	}
	var hasComps bool
	if err := q.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM azync_jobs WHERE workflow_id = $1 AND source = 'workflow' AND task_key LIKE 'comp:%')`,
		id).Scan(&hasComps); err != nil {
		return false, fmt.Errorf("azyncpgx: %s comps: %w", op, err)
	}
	return hasComps, nil
}

// isTerminalWorkflowState reports whether a workflow state is final.
func isTerminalWorkflowState(state string) bool {
	switch driver.WorkflowState(state) {
	case driver.WorkflowSucceeded, driver.WorkflowFailed, driver.WorkflowCancelled:
		return true
	default:
		return false
	}
}

// notifyWorkflowKinds fires one workflow:<kind> wakeup per kind inside the
// caller's transaction (outbox: delivered only on commit). Postgres coalesces
// duplicate notifications within the transaction.
func (s *Store) notifyWorkflowKinds(ctx context.Context, q querier, kinds map[string]struct{}) error {
	for kind := range kinds {
		if _, err := q.Exec(ctx, `SELECT pg_notify($1, $2)`, s.notifyChannel, notifyPayload(driver.SourceWorkflow, kind)); err != nil {
			return fmt.Errorf("azyncpgx: workflow notify: %w", err)
		}
	}
	return nil
}

// nullableRawJSON maps an empty payload to a SQL NULL argument and any other
// value to its text (cast to jsonb by the statement).
func nullableRawJSON(p json.RawMessage) any {
	if len(p) == 0 {
		return nil
	}
	return string(p)
}
