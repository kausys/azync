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
// driver.DAGStore and driver.TxDAGStore[pgx.Tx]. It operates the
// azync_dags header table, the azync_dag_deps edge table and the
// source='dag' rows of azync_jobs.
//
// $sleep timer encoding: a KindSleep task carries its SleepFor duration in its
// job payload as {"sleepSeconds": <float>}. CreateDAG writes it there
// (overwriting any user payload, which a timer has no handler to consume) so
// PromoteUnblocked can resolve the timer's run_at = now() + that many seconds
// against the backend clock when the task is released, exactly as a root timer
// resolves it at creation.

// --- creation --------------------------------------------------------------

const insertDAGSQL = `
INSERT INTO azync_dags
	(id, name, state, on_failure, idempotency_key, meta, created_at, updated_at)
VALUES ($1, $2, 'running',
	CASE WHEN $3 = 'suspend' THEN 'suspend' ELSE 'cancel' END,
	NULLIF($4, ''), $5::jsonb, now(), now())
ON CONFLICT (name, idempotency_key)
	WHERE idempotency_key IS NOT NULL AND state IN ('running', 'suspended', 'compensating', 'paused')
DO NOTHING
RETURNING id`

const selectLiveDAGSQL = `
SELECT id FROM azync_dags
WHERE name = $1 AND idempotency_key = $2 AND state IN ('running', 'suspended', 'compensating', 'paused')`

// insertDAGTaskSQL inserts one task job. State is resolved by the caller and
// run_at is computed DB-side: a root $sleep starts its timer at now()+SleepFor,
// every other task runs at now().
const insertDAGTaskSQL = `
INSERT INTO azync_jobs
	(id, source, kind, state, run_at, max_attempts, max_attempts_explicit,
	 payload, meta, enqueued_at,
	 dag_id, task_key, compensation_kind, compensation_payload, signal_name, ignore_dead_deps,
	 snooze_budget)
VALUES ($1, 'dag', $2, $3, now() + make_interval(secs => $4), $5, $6,
	$7::jsonb, $8::jsonb, now(),
	$9, $10, NULLIF($11, ''), $12::jsonb, NULLIF($13, ''), $14,
	NULLIF($15::double precision, 0))`

const insertDepSQL = `
INSERT INTO azync_dag_deps (dag_id, task_key, depends_on_key) VALUES ($1, $2, $3)`

// CreateDAG atomically inserts the workflow header, its tasks and its
// dependency edges, and signals workers for immediately runnable tasks, in one
// transaction. It deduplicates by (Name, IdempotencyKey) against live
// executions, returning (false, existingID) without inserting anything when a
// live execution already holds the key.
func (s *Store) CreateDAG(ctx context.Context, p driver.DAGParams) (bool, uuid.UUID, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: create dag begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	inserted, existingID, err := s.createDAG(ctx, tx, p)
	if err != nil {
		return false, uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: create dag commit: %w", err)
	}
	return inserted, existingID, nil
}

// CreateDAGTx performs CreateDAG within the caller's transaction so the
// workflow commits atomically with the caller's own writes.
func (s *Store) CreateDAGTx(ctx context.Context, tx pgx.Tx, p driver.DAGParams) (bool, uuid.UUID, error) {
	return s.createDAG(ctx, tx, p)
}

func (s *Store) createDAG(ctx context.Context, q querier, p driver.DAGParams) (bool, uuid.UUID, error) {
	metaJSON, err := json.Marshal(orEmptyMeta(p.Meta))
	if err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: marshal dag meta: %w", err)
	}

	// Insert the header. The ON CONFLICT targets only the live-idempotency
	// index; a duplicate id violates the PRIMARY KEY (a different constraint),
	// which raises an error rather than DO NOTHING — never a silent overwrite.
	var id pgtype.UUID
	err = q.QueryRow(ctx, insertDAGSQL,
		p.ID, p.Name, string(p.OnFailure), p.IdempotencyKey, string(metaJSON),
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// DO NOTHING fired: a live execution holds (Name, IdempotencyKey). This
		// only happens for a non-empty key, since the partial index excludes NULL
		// keys, so the SELECT below always finds exactly one live row.
		var existing pgtype.UUID
		if err := q.QueryRow(ctx, selectLiveDAGSQL, p.Name, p.IdempotencyKey).Scan(&existing); err != nil {
			return false, uuid.Nil, fmt.Errorf("azyncpgx: resolve deduplicated dag: %w", err)
		}
		return false, toUUID(existing), nil
	}
	if err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: insert dag: %w", err)
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

		if _, err := q.Exec(ctx, insertDAGTaskSQL,
			uuid.New(), tk.Kind, state, runAtOffsetSecs, tk.MaxAttempts, tk.MaxAttempts > 0,
			payload, string(metaJSON),
			p.ID, tk.Key, tk.CompensationKind, nullableRawJSON(tk.CompensationPayload),
			tk.SignalName, tk.IgnoreDeadDeps, tk.Deadline.Seconds(),
		); err != nil {
			return false, uuid.Nil, fmt.Errorf("azyncpgx: insert dag task %q: %w", tk.Key, err)
		}
		if err := s.bumpStat(ctx, q, driver.SourceDAG, tk.Kind, statEnqueued, 1); err != nil {
			return false, uuid.Nil, err
		}
		if state == string(driver.StatePending) {
			notifyKinds[tk.Kind] = struct{}{}
		}
	}

	for _, d := range p.Deps {
		if _, err := q.Exec(ctx, insertDepSQL, p.ID, d.TaskKey, d.DependsOnKey); err != nil {
			return false, uuid.Nil, fmt.Errorf("azyncpgx: insert dag dep %s->%s: %w", d.TaskKey, d.DependsOnKey, err)
		}
	}

	if err := s.notifyDAGKinds(ctx, q, notifyKinds); err != nil {
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

// deliverSignalNowSQL completes waiting $signal tasks named $2 (payload as
// result) and wakes scheduled $sleep timers named $2 early. The two UPDATEs
// touch disjoint (kind, state) rows, so no row is double-counted.
const deliverSignalNowSQL = `
WITH sig AS (
	UPDATE azync_jobs SET
		state = 'succeeded', result = $3::jsonb, completed_at = now(), updated_at = now()
	WHERE source = 'dag' AND dag_id = $1 AND signal_name = $2
		AND kind = '$signal' AND state = 'waiting'
	RETURNING 1
),
slp AS (
	UPDATE azync_jobs SET run_at = now(), updated_at = now()
	WHERE source = 'dag' AND dag_id = $1 AND signal_name = $2
		AND kind = '$sleep' AND state = 'scheduled'
	RETURNING 1
)
SELECT (SELECT count(*) FROM sig) + (SELECT count(*) FROM slp)`

const insertDAGSignalSQL = `
INSERT INTO azync_dag_signals (id, dag_id, name, message_id, payload)
VALUES ($1, $2, $3, NULLIF($4, ''), $5::jsonb)
ON CONFLICT (dag_id, name, message_id) WHERE message_id IS NOT NULL DO NOTHING
RETURNING id`

const consumeDAGSignalSQL = `UPDATE azync_dag_signals SET consumed = true WHERE id = $1`

// Signal delivers (or buffers) one named signal on a live workflow. One
// transaction: the header lock serializes against ApplyFailurePolicy /
// CancelDAG / CompensateDAG (which take the same FOR UPDATE), the inbox
// insert dedupes by MessageID, and the immediate-delivery attempt completes a
// waiting $signal (payload as result) or wakes a scheduled $sleep. A signal
// nothing was waiting for stays buffered, unconsumed, for
// DeliverBufferedSignals — never lost. No notify: $signal/$sleep have no
// handler; dependents are promoted (and notified) by the scheduler tick.
func (s *Store) Signal(ctx context.Context, p driver.DAGSignalParams) (int64, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("azyncpgx: signal begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM azync_dags WHERE id = $1 FOR UPDATE`, p.DAGID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, driver.NewNotFound("signal dag")
	}
	if err != nil {
		return 0, false, fmt.Errorf("azyncpgx: signal lock dag: %w", err)
	}
	if isTerminalDAGState(state) {
		return 0, false, driver.NewNotFound("signal dag")
	}

	var inboxID pgtype.UUID
	err = tx.QueryRow(ctx, insertDAGSignalSQL,
		uuid.New(), p.DAGID, p.Name, p.MessageID, nullableRawJSON(p.Payload),
	).Scan(&inboxID)
	if errors.Is(err, pgx.ErrNoRows) {
		// The dedupe index fired: same (dag, name, message_id) already
		// accepted. Nothing further happens — not even redelivery.
		return 0, true, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("azyncpgx: signal inbox insert: %w", err)
	}

	var delivered int64
	if err := tx.QueryRow(ctx, deliverSignalNowSQL, p.DAGID, p.Name, nullableRawJSON(p.Payload)).Scan(&delivered); err != nil {
		return 0, false, fmt.Errorf("azyncpgx: signal deliver: %w", err)
	}
	if delivered > 0 {
		if _, err := tx.Exec(ctx, consumeDAGSignalSQL, toUUID(inboxID)); err != nil {
			return 0, false, fmt.Errorf("azyncpgx: signal consume: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, false, fmt.Errorf("azyncpgx: signal commit: %w", err)
	}
	return delivered, false, nil
}

// deliverBufferedSignalsSQL reconciles the inbox against deliverable tasks in
// one set-based statement: for each deliverable task (waiting $signal or
// scheduled $sleep of a live workflow) the OLDEST unconsumed signal of its
// name is delivered and marked consumed. DISTINCT ON (t.id) guarantees one
// signal per task per pass even when several are buffered; the leftovers
// deliver on later passes should the task ever wait again (in practice a
// completed $signal never re-waits, so they simply age out with the DAG).
const deliverBufferedSignalsSQL = `
WITH deliverable AS (
	SELECT DISTINCT ON (t.id)
		s.id AS signal_id, t.id AS task_id, t.kind AS task_kind, s.payload
	FROM azync_dag_signals s
	JOIN azync_dags w ON w.id = s.dag_id AND w.state IN ('running', 'compensating')
	JOIN azync_jobs t ON t.source = 'dag' AND t.dag_id = s.dag_id AND t.signal_name = s.name
		AND ((t.kind = '$signal' AND t.state = 'waiting')
		  OR (t.kind = '$sleep'  AND t.state = 'scheduled'))
	WHERE NOT s.consumed
	ORDER BY t.id, s.created_at, s.id
),
sig AS (
	UPDATE azync_jobs t SET
		state = 'succeeded', result = d.payload, completed_at = now(), updated_at = now()
	FROM deliverable d
	WHERE t.id = d.task_id AND d.task_kind = '$signal' AND t.state = 'waiting'
	RETURNING d.signal_id
),
slp AS (
	UPDATE azync_jobs t SET run_at = now(), updated_at = now()
	FROM deliverable d
	WHERE t.id = d.task_id AND d.task_kind = '$sleep' AND t.state = 'scheduled'
	RETURNING d.signal_id
),
done AS (
	UPDATE azync_dag_signals s SET consumed = true
	FROM (SELECT signal_id FROM sig UNION ALL SELECT signal_id FROM slp) u
	WHERE s.id = u.signal_id
	RETURNING 1
)
SELECT count(*) FROM done`

// DeliverBufferedSignals hands buffered inbox signals to tasks that have
// become deliverable since the signal arrived, oldest first per task, and
// returns the count. Set-based and idempotent; called on the scheduler tick
// right after PromoteUnblocked.
func (s *Store) DeliverBufferedSignals(ctx context.Context) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx, deliverBufferedSignalsSQL).Scan(&n); err != nil {
		return 0, fmt.Errorf("azyncpgx: deliver buffered signals: %w", err)
	}
	return n, nil
}

// promoteUnblockedSQL releases blocked tasks whose dependencies are all
// satisfied, into the runnable state their kind dictates. A dependency is
// satisfied when it succeeded or was deliberately skipped; a task with
// ignore_dead_deps also tolerates dead or cancelled dependencies. Only
// running and compensating dags promote. A dependency edge whose task is
// missing keeps the task blocked (the inner NOT EXISTS finds no satisfying
// row).
const promoteUnblockedSQL = `
UPDATE azync_jobs t SET
	state = CASE t.kind WHEN '$signal' THEN 'waiting' WHEN '$sleep' THEN 'scheduled' ELSE 'pending' END,
	run_at = CASE
		WHEN t.kind = '$sleep' THEN now() + make_interval(secs => COALESCE((t.payload->>'sleepSeconds')::double precision, 0))
		WHEN t.kind = '$signal' THEN t.run_at
		ELSE now() END,
	updated_at = now()
FROM azync_dags w
WHERE t.source = 'dag' AND t.state = 'blocked'
	AND w.id = t.dag_id AND w.state IN ('running', 'compensating')
	AND NOT EXISTS (
		SELECT 1 FROM azync_dag_deps d
		WHERE d.dag_id = t.dag_id AND d.task_key = t.task_key
			AND NOT EXISTS (
				SELECT 1 FROM azync_jobs dep
				WHERE dep.dag_id = d.dag_id AND dep.task_key = d.depends_on_key
					AND dep.source = 'dag'
					AND (dep.state IN ('succeeded', 'skipped') OR (t.ignore_dead_deps AND dep.state IN ('dead', 'cancelled')))
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

	if err := s.notifyDAGKinds(ctx, tx, notifyKinds); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("azyncpgx: promote unblocked commit: %w", err)
	}
	return promoted, nil
}

const completeDueSleepsSQL = `
UPDATE azync_jobs t SET state = 'succeeded', completed_at = now(), updated_at = now()
FROM azync_dags w
WHERE t.source = 'dag' AND t.kind = '$sleep' AND t.state = 'scheduled' AND t.run_at <= now()
	AND w.id = t.dag_id AND w.state = 'running'`

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
FROM azync_dags w
JOIN azync_jobs t ON t.dag_id = w.id AND t.source = 'dag' AND t.state = 'dead'
WHERE w.state = 'running'
	AND (
		NOT EXISTS (SELECT 1 FROM azync_dag_deps d WHERE d.dag_id = w.id AND d.depends_on_key = t.task_key)
		OR EXISTS (
			SELECT 1 FROM azync_dag_deps d
			WHERE d.dag_id = w.id AND d.depends_on_key = t.task_key
				AND NOT EXISTS (
					SELECT 1 FROM azync_jobs dep
					WHERE dep.dag_id = d.dag_id AND dep.task_key = d.task_key
						AND dep.source = 'dag' AND dep.ignore_dead_deps
				)
		)
	)`

// failedDAG accumulates one workflow's triggering dead keys and policy.
type failedDAG struct {
	policy   driver.OnFailurePolicy
	deadKeys []string
}

// ApplyFailurePolicy applies each running workflow's OnFailure policy when it has
// at least one triggering dead task, in one transaction. This runs after
// PromoteUnblocked/CompleteDueSleeps and before CompleteDAGs on the worker's
// tick: a workflow whose dead task triggers is moved out of 'running' here, so
// only tolerated deaths remain for CompleteDAGs to settle.
//
// Concurrency: triggeringDeadTasksSQL reads without a lock, so two concurrent
// ticks (or a tick racing a manual CompensateDAG/CancelDAG) can both
// observe the same workflow as 'running' under READ COMMITTED. The
// cancel/compensate branch below re-acquires the workflow row with
// lockDAGForUpdate and re-checks its state before touching any task rows:
// the loser of the race sees a state that already left 'running' and skips
// cleanly instead of racing insertCompensations's guard, which would otherwise
// violate the (dag_id, task_key) unique index.
func (s *Store) ApplyFailurePolicy(ctx context.Context) ([]driver.DAGFailure, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: apply failure policy begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, triggeringDeadTasksSQL)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: find triggering dead tasks: %w", err)
	}
	acting := map[uuid.UUID]*failedDAG{}
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
			fw = &failedDAG{policy: driver.OnFailurePolicy(policy)}
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

	var out []driver.DAGFailure
	for _, wid := range order {
		fw := acting[wid]
		slices.Sort(fw.deadKeys)
		reason := deadTasksReason(fw.deadKeys)

		if fw.policy == driver.OnFailureSuspend {
			// The state re-check in the WHERE guards the same race the cancel
			// branch resolves with lockDAGForUpdate: a concurrent manual
			// CompensateDAG/CancelDAG that landed after this pass's
			// unlocked read must not be overwritten with 'suspended'.
			tag, err := tx.Exec(ctx,
				`UPDATE azync_dags SET state = 'suspended', failure_reason = $2, updated_at = now() WHERE id = $1 AND state = 'running'`,
				wid, reason)
			if err != nil {
				return nil, fmt.Errorf("azyncpgx: suspend dag: %w", err)
			}
			if tag.RowsAffected() == 0 {
				continue // the workflow already left running; nothing to report
			}
		} else {
			state, err := s.lockDAGForUpdate(ctx, tx, wid)
			if err != nil {
				return nil, err
			}
			if state != string(driver.DAGRunning) {
				// A concurrent tick already moved this workflow out of
				// running (it won the compensation-insert race, or a manual
				// CompensateDAG/CancelDAG call landed first):
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
				if _, err := tx.Exec(ctx, `UPDATE azync_dags SET state = 'failed', failure_reason = $2, completed_at = now(), updated_at = now() WHERE id = $1`,
					wid, reason); err != nil {
					return nil, fmt.Errorf("azyncpgx: fail dag: %w", err)
				}
			} else {
				if _, err := tx.Exec(ctx, `UPDATE azync_dags SET state = 'compensating', failure_reason = $2, updated_at = now() WHERE id = $1`,
					wid, reason); err != nil {
					return nil, fmt.Errorf("azyncpgx: compensate dag: %w", err)
				}
			}
		}
		out = append(out, driver.DAGFailure{DAGID: wid, Policy: fw.policy, DeadTasks: fw.deadKeys})
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

// lockDAGForUpdate takes the row lock on the workflow header (FOR UPDATE)
// and returns its current state. Callers on the cancel/compensate path
// (ApplyFailurePolicy, CompensateDAG) take this lock before touching any
// task row and re-check the returned state against what they expect: it is the
// serialization point that makes concurrent ticks on the same workflow safe,
// since the compensation-insert guard in insertCompensations is a plain SELECT
// count(*) that is not by itself race-free under READ COMMITTED. Callers run
// inside a transaction; the lock releases on commit or rollback.
func (s *Store) lockDAGForUpdate(ctx context.Context, q querier, id uuid.UUID) (string, error) {
	var state string
	if err := q.QueryRow(ctx, `SELECT state FROM azync_dags WHERE id = $1 FOR UPDATE`, id).Scan(&state); err != nil {
		return "", fmt.Errorf("azyncpgx: lock dag: %w", err)
	}
	return state, nil
}

const cancelRemainingTasksSQL = `
UPDATE azync_jobs SET state = 'cancelled', completed_at = now(), updated_at = now()
WHERE dag_id = $1 AND source = 'dag' AND state IN ('pending', 'scheduled', 'blocked', 'waiting', 'paused')`

// cancelRemainingTasks cancels the workflow's non-terminal, non-active tasks. An
// active task keeps its lease and settles on its own.
func (s *Store) cancelRemainingTasks(ctx context.Context, q querier, dagID uuid.UUID) error {
	if _, err := q.Exec(ctx, cancelRemainingTasksSQL, dagID); err != nil {
		return fmt.Errorf("azyncpgx: cancel remaining tasks: %w", err)
	}
	return nil
}

const compensationCandidatesSQL = `
SELECT task_key, compensation_kind
FROM azync_jobs
WHERE dag_id = $1 AND source = 'dag' AND state = 'succeeded'
	AND compensation_kind IS NOT NULL AND compensation_kind <> ''
	AND task_key NOT LIKE 'comp:%'
ORDER BY completed_at DESC, created_at DESC, id DESC`

// insertCompensationTaskSQL clones the original succeeded task's meta and
// declared compensation payload into a fresh comp:<key> task of the declared
// compensation kind. The compensation is itself uncompensated and signal-free.
const insertCompensationTaskSQL = `
INSERT INTO azync_jobs
	(id, source, kind, state, run_at, max_attempts, max_attempts_explicit,
	 payload, meta, enqueued_at, dag_id, task_key)
SELECT gen_random_uuid(), 'dag', o.compensation_kind, $3, now(),
	o.max_attempts, o.max_attempts_explicit, o.compensation_payload, o.meta,
	now(), o.dag_id, $4
FROM azync_jobs o
WHERE o.dag_id = $1 AND o.source = 'dag' AND o.task_key = $2`

// insertCompensations inserts the compensation chain: one comp:<key> task per
// succeeded task that declared a compensation, in reverse completion order,
// chained through dependency edges (newest completion first and pending, the
// rest blocked on their predecessor). It returns the total number of
// compensation tasks the workflow has; a workflow that already carries a chain
// is left untouched (guard against double insertion after a policy pass followed
// by a manual compensate). This guard is a plain SELECT count(*) and is not by
// itself race-free under READ COMMITTED against a second concurrent
// cancel/compensate transaction on the same workflow; callers MUST hold the
// workflow row lock from lockDAGForUpdate before calling this, which
// serializes the two and makes the guard authoritative. Callers run inside a
// transaction.
func (s *Store) insertCompensations(ctx context.Context, q querier, dagID uuid.UUID) (int, error) {
	var existing int
	if err := q.QueryRow(ctx,
		`SELECT count(*) FROM azync_jobs WHERE dag_id = $1 AND source = 'dag' AND task_key LIKE 'comp:%'`,
		dagID).Scan(&existing); err != nil {
		return 0, fmt.Errorf("azyncpgx: count existing compensations: %w", err)
	}
	if existing > 0 {
		return existing, nil
	}

	rows, err := q.Query(ctx, compensationCandidatesSQL, dagID)
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
		if _, err := q.Exec(ctx, insertCompensationTaskSQL, dagID, c.taskKey, state, compKey); err != nil {
			return 0, fmt.Errorf("azyncpgx: insert compensation %q: %w", compKey, err)
		}
		if i > 0 {
			if _, err := q.Exec(ctx, insertDepSQL, dagID, compKey, prevKey); err != nil {
				return 0, fmt.Errorf("azyncpgx: chain compensation %q: %w", compKey, err)
			}
		}
		if err := s.bumpStat(ctx, q, driver.SourceDAG, c.compKind, statEnqueued, 1); err != nil {
			return 0, err
		}
		if i == 0 {
			if err := s.notifyDAGKinds(ctx, q, map[string]struct{}{c.compKind: {}}); err != nil {
				return 0, err
			}
		}
		prevKey = compKey
	}
	return len(candidates), nil
}

// --- completion ------------------------------------------------------------

// completeRunningSucceededSQL settles a running workflow whose tasks are all
// succeeded or deliberately skipped (none dead) to succeeded.
const completeRunningSucceededSQL = `
UPDATE azync_dags w SET state = 'succeeded', completed_at = now(), updated_at = now()
WHERE w.state = 'running'
	AND EXISTS (SELECT 1 FROM azync_jobs j WHERE j.dag_id = w.id AND j.source = 'dag')
	AND NOT EXISTS (SELECT 1 FROM azync_jobs j WHERE j.dag_id = w.id AND j.source = 'dag' AND j.state NOT IN ('succeeded', 'skipped'))`

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
// CompleteDAGs transactions, which would otherwise settle a Cancel-policy
// workflow failed with its compensations skipped. The succeeded branch
// (completeRunningSucceededSQL) needs no such guard: it requires every task
// succeeded, so no dead task can be present.
const completeRunningFailedSQL = `
UPDATE azync_dags w SET state = 'failed', failure_reason = r.reason, completed_at = now(), updated_at = now()
FROM (
	SELECT j.dag_id, 'dead tasks: ' || string_agg(j.task_key, ', ' ORDER BY j.task_key) AS reason
	FROM azync_jobs j
	JOIN azync_dags w2 ON w2.id = j.dag_id AND w2.state = 'running'
	WHERE j.source = 'dag' AND j.state = 'dead'
		AND NOT EXISTS (
			SELECT 1 FROM azync_jobs j2
			WHERE j2.dag_id = j.dag_id AND j2.source = 'dag'
				AND j2.state NOT IN ('succeeded', 'skipped', 'dead')
		)
		AND NOT EXISTS (
			SELECT 1 FROM azync_jobs jd
			WHERE jd.dag_id = j.dag_id AND jd.source = 'dag' AND jd.state = 'dead'
				AND (
					NOT EXISTS (SELECT 1 FROM azync_dag_deps d WHERE d.dag_id = jd.dag_id AND d.depends_on_key = jd.task_key)
					OR EXISTS (
						SELECT 1 FROM azync_dag_deps d
						WHERE d.dag_id = jd.dag_id AND d.depends_on_key = jd.task_key
							AND NOT EXISTS (
								SELECT 1 FROM azync_jobs dep
								WHERE dep.dag_id = d.dag_id AND dep.task_key = d.task_key
									AND dep.source = 'dag' AND dep.ignore_dead_deps
							)
					)
				)
		)
	GROUP BY j.dag_id
) r
WHERE w.id = r.dag_id AND w.state = 'running'`

// completeCompensatingSuspendSQL parks a compensating workflow with a dead
// compensation as suspended for a manual decision — unless the compensation was
// triggered through CancelDAG, which rides the chain to cancelled.
const completeCompensatingSuspendSQL = `
UPDATE azync_dags w SET state = 'suspended', updated_at = now()
WHERE w.state = 'compensating' AND w.cancel_requested = false
	AND EXISTS (
		SELECT 1 FROM azync_jobs j
		WHERE j.dag_id = w.id AND j.source = 'dag' AND j.task_key LIKE 'comp:%' AND j.state = 'dead'
	)`

// completeCompensatingSettleSQL settles a compensating workflow whose
// compensation tasks are all terminal: cancelled when the compensation was
// triggered through CancelDAG, otherwise failed. Suspend-on-dead dags
// have already left 'compensating' via completeCompensatingSuspendSQL.
const completeCompensatingSettleSQL = `
UPDATE azync_dags w SET
	state = CASE WHEN w.cancel_requested THEN 'cancelled' ELSE 'failed' END,
	completed_at = now(), updated_at = now()
WHERE w.state = 'compensating'
	AND EXISTS (SELECT 1 FROM azync_jobs j WHERE j.dag_id = w.id AND j.source = 'dag' AND j.task_key LIKE 'comp:%')
	AND NOT EXISTS (
		SELECT 1 FROM azync_jobs j
		WHERE j.dag_id = w.id AND j.source = 'dag' AND j.task_key LIKE 'comp:%'
			AND j.state NOT IN ('succeeded', 'cancelled', 'dead')
	)`

// CompleteDAGs settles dags whose work is finished, running the four
// disjoint transitions in one transaction (suspend-on-dead-compensation before
// the terminal compensation settle so the two never overlap).
func (s *Store) CompleteDAGs(ctx context.Context) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: complete dags begin: %w", err)
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
			return 0, fmt.Errorf("azyncpgx: complete dags: %w", err)
		}
		settled += tag.RowsAffected()
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("azyncpgx: complete dags commit: %w", err)
	}
	return settled, nil
}

// --- results ---------------------------------------------------------------

const taskResultsSQL = `
SELECT task_key, result::text, (state = 'skipped') AS skipped
FROM azync_jobs
WHERE dag_id = $1 AND source = 'dag' AND state IN ('succeeded', 'skipped')
	AND ($2 OR task_key = ANY($3::text[]))`

// TaskResults returns the settled outcomes of the workflow's succeeded and
// skipped tasks, keyed by task key, restricted to keys when non-empty. A
// succeeded task without a result maps to an entry with a nil Result; a
// skipped one to Skipped=true — distinguishable, never a silent zero value.
func (s *Store) TaskResults(ctx context.Context, dagID uuid.UUID, keys []string) (map[string]driver.TaskResult, error) {
	rows, err := s.pool.Query(ctx, taskResultsSQL, dagID, len(keys) == 0, keys)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: task results: %w", err)
	}
	defer rows.Close()
	out := map[string]driver.TaskResult{}
	for rows.Next() {
		var (
			key     string
			result  *string
			skipped bool
		)
		if err := rows.Scan(&key, &result, &skipped); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan task result: %w", err)
		}
		tr := driver.TaskResult{Skipped: skipped}
		if result != nil && !skipped {
			tr.Result = json.RawMessage(*result)
		}
		out[key] = tr
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

const dagColumns = `
	id, name, state, on_failure, COALESCE(idempotency_key, ''), COALESCE(failure_reason, ''),
	meta::text, created_at, updated_at, completed_at`

const getDAGSQL = `SELECT ` + dagColumns + ` FROM azync_dags WHERE id = $1`

// GetDAG returns one workflow header by id, or a not-found error.
func (s *Store) GetDAG(ctx context.Context, id uuid.UUID) (*driver.DAGView, error) {
	rows, err := s.pool.Query(ctx, getDAGSQL, id)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: get dag: %w", err)
	}
	views, err := scanDAGViews(rows)
	if err != nil {
		return nil, err
	}
	if len(views) == 0 {
		return nil, driver.NewNotFound("get dag")
	}
	return &views[0], nil
}

// ListDAGs lists dags matching filter, newest first (created_at then
// id descending), paginated, with the total matching count.
func (s *Store) ListDAGs(ctx context.Context, filter driver.DAGFilter, offset, limit int) ([]driver.DAGView, int64, error) {
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
	if err := s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM azync_dags WHERE "+where, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("azyncpgx: list dags count: %w", err)
	}

	if offset < 0 {
		offset = 0
	}
	limitIdx := len(args) + 1
	offsetIdx := len(args) + 2
	args = append(args, limitArg(limit), offset)
	//nolint:gosec // G202: no user-controlled SQL identifier is interpolated
	sql := "SELECT " + dagColumns + " FROM azync_dags WHERE " + where +
		" ORDER BY created_at DESC, id DESC LIMIT $" + strconv.Itoa(limitIdx) + " OFFSET $" + strconv.Itoa(offsetIdx)
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("azyncpgx: list dags: %w", err)
	}
	views, err := scanDAGViews(rows)
	if err != nil {
		return nil, 0, err
	}
	return views, total, nil
}

func scanDAGViews(rows pgx.Rows) ([]driver.DAGView, error) {
	defer rows.Close()
	var out []driver.DAGView
	for rows.Next() {
		var (
			view        driver.DAGView
			id          pgtype.UUID
			meta        string
			completedAt pgtype.Timestamptz
			state       string
			onFailure   string
		)
		if err := rows.Scan(
			&id, &view.Name, &state, &onFailure, &view.IdempotencyKey, &view.FailureReason,
			&meta, &view.CreatedAt, &view.UpdatedAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan dag: %w", err)
		}
		view.ID = toUUID(id)
		view.State = driver.DAGState(state)
		view.OnFailure = driver.OnFailurePolicy(onFailure)
		view.CompletedAt = completedAt.Time
		m, err := decodeMeta(meta)
		if err != nil {
			return nil, fmt.Errorf("azyncpgx: decode dag meta for %s: %w", view.ID, err)
		}
		view.Meta = m
		out = append(out, view)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate dags: %w", err)
	}
	return out, nil
}

var workflowTasksSQL = `SELECT ` + jobColumns("azync_jobs") +
	` FROM azync_jobs WHERE dag_id = $1 AND source = 'dag' ORDER BY created_at, id`

// DAGTasks returns every task job of the workflow (compensation tasks
// included) ordered by created_at then id — tasks inserted in the same atomic
// batch share created_at, so their relative order is stable, not the
// declaration order. It returns a not-found error when the workflow does not
// exist.
func (s *Store) DAGTasks(ctx context.Context, id uuid.UUID) ([]driver.Job, error) {
	if err := s.requireDAG(ctx, "dag tasks", id); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, workflowTasksSQL, id)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: dag tasks: %w", err)
	}
	return collectJobs(rows)
}

// dagDepsSQL reads the workflow's edges. The primary key is
// (dag_id, task_key, depends_on_key), so this is an index-only scan and the
// ORDER BY is free.
const dagDepsSQL = `
SELECT task_key, depends_on_key FROM azync_dag_deps
WHERE dag_id = $1 ORDER BY task_key, depends_on_key`

// DAGDeps returns every dependency edge of the workflow, compensation-chain
// links included. An unknown id yields no rows, not an error.
func (s *Store) DAGDeps(ctx context.Context, id uuid.UUID) ([]driver.DAGDep, error) {
	rows, err := s.pool.Query(ctx, dagDepsSQL, id)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: dag deps: %w", err)
	}
	defer rows.Close()
	var out []driver.DAGDep
	for rows.Next() {
		var d driver.DAGDep
		if err := rows.Scan(&d.TaskKey, &d.DependsOnKey); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan dag dep: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate dag deps: %w", err)
	}
	return out, nil
}

// dagNameStateCountsSQL counts every retained dag by (definition, state).
// Backed by azync_dags_name_state_idx (00010) — the 00002/00009 indexes are
// all partial on the live states, so without it this scans the full table
// including terminal history.
const dagNameStateCountsSQL = `SELECT name, state, COUNT(*)::bigint FROM azync_dags GROUP BY name, state`

// DAGNameStateCounts returns how many dags sit in each state, per definition.
func (s *Store) DAGNameStateCounts(ctx context.Context) (map[string]map[driver.DAGState]int64, error) {
	rows, err := s.pool.Query(ctx, dagNameStateCountsSQL)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: dag name state counts: %w", err)
	}
	defer rows.Close()
	out := make(map[string]map[driver.DAGState]int64)
	for rows.Next() {
		var name, state string
		var n int64
		if err := rows.Scan(&name, &state, &n); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan dag name state count: %w", err)
		}
		if out[name] == nil {
			out[name] = make(map[driver.DAGState]int64)
		}
		out[name][driver.DAGState(state)] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate dag name state counts: %w", err)
	}
	return out, nil
}

// dagTaskCountsSQL breaks one page of dags down by task state in a single
// round trip, served by the unique (dag_id, task_key) index.
const dagTaskCountsSQL = `
SELECT dag_id, state, COUNT(*)::bigint FROM azync_jobs
WHERE source = 'dag' AND dag_id = ANY($1) GROUP BY dag_id, state`

// DAGTaskCounts returns each requested dag's task breakdown by state. Ids with
// no tasks are absent from the result; an empty ids slice queries nothing.
func (s *Store) DAGTaskCounts(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]map[driver.JobState]int64, error) {
	out := make(map[uuid.UUID]map[driver.JobState]int64, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, dagTaskCountsSQL, ids)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: dag task counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id pgtype.UUID
		var state string
		var n int64
		if err := rows.Scan(&id, &state, &n); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan dag task count: %w", err)
		}
		dagID := toUUID(id)
		if out[dagID] == nil {
			out[dagID] = make(map[driver.JobState]int64)
		}
		out[dagID][driver.JobState(state)] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate dag task counts: %w", err)
	}
	return out, nil
}

// requireDAG maps a missing workflow header to the contract's not-found.
func (s *Store) requireDAG(ctx context.Context, op string, id uuid.UUID) error {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM azync_dags WHERE id = $1)`, id).Scan(&exists); err != nil {
		return fmt.Errorf("azyncpgx: %s: %w", op, err)
	}
	if !exists {
		return driver.NewNotFound(op)
	}
	return nil
}

// resetDeadTasksSQL resets the workflow's dead tasks to a fresh pending state
// (attempt and reap_count cleared, and the stamped snooze deadline dropped so
// a retried task waits with a fresh budget — without this, a task that died
// on its deadline would re-die on its very next snooze). When the workflow
// carries a compensation chain ($2 true) only the compensation tasks reset,
// so original tasks never rerun once compensation started.
const resetDeadTasksSQL = `
UPDATE azync_jobs SET
	state = 'pending', run_at = now(), attempt = 0, reap_count = 0,
	last_error = NULL, failed_at = NULL, deadline_at = NULL, started_at = NULL, updated_at = now()
WHERE dag_id = $1 AND source = 'dag' AND state = 'dead'
	AND (NOT $2 OR task_key LIKE 'comp:%')
RETURNING kind`

// unpauseTasksSQL releases the workflow's operator-paused tasks back to the
// ready set per their run_at, also dropping the stamped snooze deadline: time
// spent paused (say, waiting out a provider outage) must not burn the wait
// budget, so it re-stamps on the next first snooze.
const unpauseTasksSQL = `
UPDATE azync_jobs SET
	state = CASE WHEN run_at <= now() THEN 'pending' ELSE 'scheduled' END,
	deadline_at = NULL, updated_at = now()
WHERE dag_id = $1 AND source = 'dag' AND state = 'paused'
RETURNING kind, (run_at <= now()) AS runnable`

// resumeDAGSQL resumes a suspended or operator-paused workflow: to
// compensating when a compensation chain exists, otherwise to running with
// the recorded reason cleared. A workflow in any other state keeps its state
// (only its updated_at advances).
const resumeDAGSQL = `
UPDATE azync_dags SET
	state = CASE
		WHEN state IN ('suspended', 'paused') AND $2 THEN 'compensating'
		WHEN state IN ('suspended', 'paused') AND NOT $2 THEN 'running'
		ELSE state END,
	failure_reason = CASE WHEN state IN ('suspended', 'paused') AND NOT $2 THEN NULL ELSE failure_reason END,
	updated_at = now()
WHERE id = $1`

// RetryDAG resumes a non-terminal workflow after failures or an operator
// pause: dead tasks reset (or only dead compensation tasks once a chain
// exists), paused tasks return to the ready set, and a suspended or paused
// workflow resumes — to running, or back to compensating. Both paths clear
// the tasks' stamped snooze deadline, so a resumed wait starts with a fresh
// budget.
func (s *Store) RetryDAG(ctx context.Context, id uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("azyncpgx: retry dag begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	hasComps, err := s.requireNonTerminalDAG(ctx, tx, "retry dag", id)
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

	rows, err = tx.Query(ctx, unpauseTasksSQL, id)
	if err != nil {
		return fmt.Errorf("azyncpgx: unpause tasks: %w", err)
	}
	for rows.Next() {
		var kind string
		var runnable bool
		if err := rows.Scan(&kind, &runnable); err != nil {
			rows.Close()
			return fmt.Errorf("azyncpgx: scan unpaused task: %w", err)
		}
		if runnable {
			notifyKinds[kind] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("azyncpgx: iterate unpaused tasks: %w", err)
	}
	rows.Close()

	if _, err := tx.Exec(ctx, resumeDAGSQL, id, hasComps); err != nil {
		return fmt.Errorf("azyncpgx: resume dag: %w", err)
	}
	if err := s.notifyDAGKinds(ctx, tx, notifyKinds); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("azyncpgx: retry dag commit: %w", err)
	}
	return nil
}

// FindDAGByKey resolves the live workflow holding (name, idempotencyKey) —
// the business-key lookup a webhook handler needs. Served by the partial
// unique dedupe index, so at most one row can match.
func (s *Store) FindDAGByKey(ctx context.Context, name, idempotencyKey string) (uuid.UUID, error) {
	var id pgtype.UUID
	err := s.pool.QueryRow(ctx, selectLiveDAGSQL, name, idempotencyKey).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, driver.NewNotFound("find dag by key")
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("azyncpgx: find dag by key: %w", err)
	}
	return toUUID(id), nil
}

const pauseDAGHeaderSQL = `
UPDATE azync_dags SET state = 'paused', failure_reason = $2, updated_at = now()
WHERE id = $1 AND state = 'running'`

const pauseDAGTasksSQL = `
UPDATE azync_jobs SET state = 'paused', updated_at = now()
WHERE dag_id = $1 AND source = 'dag' AND state IN ('pending', 'scheduled')`

// PauseDAG freezes a running workflow: header to paused (reason recorded in
// failure_reason — the "why is this stopped" column for both suspensions and
// pauses), pending/scheduled tasks to paused, one transaction. Blocked and
// waiting tasks keep their states: with the header out of
// running/compensating, PromoteUnblocked and DeliverBufferedSignals skip the
// workflow, and incoming signals buffer for delivery after RetryDAG resumes
// it. The row lock serializes against the scheduler's policy/cancel passes.
func (s *Store) PauseDAG(ctx context.Context, id uuid.UUID, reason string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("azyncpgx: pause dag begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM azync_dags WHERE id = $1 FOR UPDATE`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.NewNotFound("pause dag")
	}
	if err != nil {
		return fmt.Errorf("azyncpgx: pause dag: %w", err)
	}
	if state != string(driver.DAGRunning) {
		return driver.NewNotFound("pause dag")
	}
	if _, err := tx.Exec(ctx, pauseDAGHeaderSQL, id, reason); err != nil {
		return fmt.Errorf("azyncpgx: pause dag header: %w", err)
	}
	if _, err := tx.Exec(ctx, pauseDAGTasksSQL, id); err != nil {
		return fmt.Errorf("azyncpgx: pause dag tasks: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("azyncpgx: pause dag commit: %w", err)
	}
	return nil
}

// CompensateDAG manually triggers compensation on a running or suspended
// workflow, exactly like the OnFailureCancel policy.
//
// Concurrency: the initial state read takes the row lock (FOR UPDATE) so it
// serializes against a concurrent ApplyFailurePolicy tick or another
// CompensateDAG call on the same workflow id. The loser blocks until the
// winner commits, then observes the post-commit state: no longer
// running/suspended, so it returns the same not-found this call already
// returns for a workflow that never qualified, instead of racing
// insertCompensations's guard into a unique-violation.
func (s *Store) CompensateDAG(ctx context.Context, id uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("azyncpgx: compensate dag begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM azync_dags WHERE id = $1 FOR UPDATE`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.NewNotFound("compensate dag")
	}
	if err != nil {
		return fmt.Errorf("azyncpgx: compensate dag: %w", err)
	}
	if state != string(driver.DAGRunning) && state != string(driver.DAGSuspended) &&
		state != string(driver.DAGPaused) {
		return driver.NewNotFound("compensate dag")
	}

	if err := s.cancelRemainingTasks(ctx, tx, id); err != nil {
		return err
	}
	comps, err := s.insertCompensations(ctx, tx, id)
	if err != nil {
		return err
	}
	if comps == 0 {
		if _, err := tx.Exec(ctx, `UPDATE azync_dags SET state = 'failed', completed_at = now(), updated_at = now() WHERE id = $1`, id); err != nil {
			return fmt.Errorf("azyncpgx: compensate settle failed: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `UPDATE azync_dags SET state = 'compensating', updated_at = now() WHERE id = $1`, id); err != nil {
			return fmt.Errorf("azyncpgx: compensate settle compensating: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("azyncpgx: compensate dag commit: %w", err)
	}
	return nil
}

// cancelDAGSQL marks cancel_requested and, unless the workflow is
// compensating (whose in-flight chain is allowed to settle first), lands it on
// cancelled immediately. The CASE references the pre-update state.
const cancelDAGSQL = `
UPDATE azync_dags SET
	cancel_requested = true,
	state = CASE WHEN state = 'compensating' THEN state ELSE 'cancelled' END,
	completed_at = CASE WHEN state = 'compensating' THEN completed_at ELSE now() END,
	updated_at = now()
WHERE id = $1`

// CancelDAG cancels a non-terminal workflow without compensating: its
// non-terminal tasks are cancelled and the workflow becomes cancelled, except a
// compensating workflow, which keeps compensating until CompleteDAGs lands
// it on cancelled.
//
// Concurrency: the initial state read takes the row lock (FOR UPDATE), so a
// cancel racing a scheduler pass (or another verb) that is settling the same
// workflow blocks until the winner commits and then observes the committed
// state — a workflow that just reached a terminal state is a not-found here,
// never flipped to cancelled after the fact.
func (s *Store) CancelDAG(ctx context.Context, id uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("azyncpgx: cancel dag begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM azync_dags WHERE id = $1 FOR UPDATE`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.NewNotFound("cancel dag")
	}
	if err != nil {
		return fmt.Errorf("azyncpgx: cancel dag: %w", err)
	}
	if isTerminalDAGState(state) {
		return driver.NewNotFound("cancel dag")
	}

	if _, err := tx.Exec(ctx, cancelDAGSQL, id); err != nil {
		return fmt.Errorf("azyncpgx: cancel dag update: %w", err)
	}
	if err := s.cancelRemainingTasks(ctx, tx, id); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("azyncpgx: cancel dag commit: %w", err)
	}
	return nil
}

const vacuumDAGsSQL = `
DELETE FROM azync_dags
WHERE state IN ('succeeded', 'failed', 'cancelled')
	AND completed_at IS NOT NULL
	AND completed_at < now() - make_interval(secs => $1)`

// VacuumDAGs deletes terminal dags completed before retention ago,
// cascading (via the FKs) to their task jobs and dependency edges. A retention
// <= 0 removes nothing.
func (s *Store) VacuumDAGs(ctx context.Context, retention time.Duration) (int64, error) {
	if retention <= 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, vacuumDAGsSQL, retention.Seconds())
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: vacuum dags: %w", err)
	}
	return tag.RowsAffected(), nil
}

// --- helpers ---------------------------------------------------------------

// requireNonTerminalDAG loads a workflow for a manager verb, mapping a
// missing or terminal workflow to not-found, and reports whether it carries a
// compensation chain. The read takes the row lock (FOR UPDATE) so the caller
// serializes against a concurrent policy pass or verb on the same workflow:
// both answers are stale the instant a racing transaction commits, and acting
// on them unlocked lets RetryDAG reset the original dead task after a
// concurrent ApplyFailurePolicy started compensation. Callers run inside a
// transaction; the lock releases on commit or rollback.
func (s *Store) requireNonTerminalDAG(ctx context.Context, q querier, op string, id uuid.UUID) (bool, error) {
	var state string
	err := q.QueryRow(ctx, `SELECT state FROM azync_dags WHERE id = $1 FOR UPDATE`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, driver.NewNotFound(op)
	}
	if err != nil {
		return false, fmt.Errorf("azyncpgx: %s: %w", op, err)
	}
	if isTerminalDAGState(state) {
		return false, driver.NewNotFound(op)
	}
	var hasComps bool
	if err := q.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM azync_jobs WHERE dag_id = $1 AND source = 'dag' AND task_key LIKE 'comp:%')`,
		id).Scan(&hasComps); err != nil {
		return false, fmt.Errorf("azyncpgx: %s comps: %w", op, err)
	}
	return hasComps, nil
}

// isTerminalDAGState reports whether a workflow state is final.
func isTerminalDAGState(state string) bool {
	switch driver.DAGState(state) {
	case driver.DAGSucceeded, driver.DAGFailed, driver.DAGCancelled:
		return true
	default:
		return false
	}
}

// notifyDAGKinds fires one workflow:<kind> wakeup per kind inside the
// caller's transaction (outbox: delivered only on commit). Postgres coalesces
// duplicate notifications within the transaction.
func (s *Store) notifyDAGKinds(ctx context.Context, q querier, kinds map[string]struct{}) error {
	for kind := range kinds {
		if _, err := q.Exec(ctx, `SELECT pg_notify($1, $2)`, s.notifyChannel, notifyPayload(driver.SourceDAG, kind)); err != nil {
			return fmt.Errorf("azyncpgx: dag notify: %w", err)
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
