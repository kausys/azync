package azyncpgx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// pgUniqueViolation is the SQLSTATE PostgreSQL reports for a unique
// constraint or index violation.
const pgUniqueViolation = "23505"

// The workflow-as-code capability of the pgx Store: the SQL implementation of
// driver.WorkflowStore (see docs/workflow-v1-spec.md). It operates the four
// azync_workflow_* tables from migration 00003 and is distinct from
// dag_store.go's static-DAG capability, sharing only azync_jobs (via
// source='workflow' and run_id, which the workflow runtime owns directly
// through the plain Store methods).

// --- StartWorkflow / GetWorkflowExecution -----------------------------------

const insertWorkflowSQL = `
INSERT INTO azync_workflow_executions
	(id, name, version, business_idempotency_key, task_queue, input, meta)
VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6::jsonb, $7::jsonb)
ON CONFLICT (name, business_idempotency_key)
	WHERE business_idempotency_key IS NOT NULL AND state IN ('running', 'suspended')
DO NOTHING
RETURNING id`

const selectLiveWorkflowSQL = `
SELECT id FROM azync_workflow_executions
WHERE name = $1 AND business_idempotency_key = $2 AND state IN ('running', 'suspended')`

// workflowTaskKindFmt mirrors workflow/worker.go's unexported
// workflowTaskKind: the fetch partition a workflow's tasks route through.
// Duplicated here (rather than importing the workflow package, which this
// driver-adjacent package structurally cannot depend on) as a stable
// wire-format constant — like the 'WorkflowStarted'/'SignalReceived'
// event-type literals below, changing it would break every already
// -scheduled task job.
func workflowTaskKindFmt(name string) string { return "$wf:" + name }

// isTerminalWorkflowStateStr reports whether a raw workflow state column
// value is one of the three terminal states.
func isTerminalWorkflowStateStr(state string) bool {
	switch driver.WorkflowState(state) {
	case driver.WorkflowSucceeded, driver.WorkflowFailed, driver.WorkflowCancelled:
		return true
	default:
		return false
	}
}

const insertFirstHistorySQL = `
INSERT INTO azync_workflow_history (workflow_id, event_seq, event_type, payload)
VALUES ($1, 1, 'WorkflowStarted', $2::jsonb)`

// StartWorkflow atomically inserts one workflow-as-code execution header,
// deduplicating by (Name, BusinessIdempotencyKey) against live (running or
// suspended) executions, and — for a newly inserted execution — records the
// WorkflowStarted history event and schedules the first workflow-task job in
// the same transaction: a caller can never observe a newly started execution
// with no history or task, closing the crash window between the three
// separate calls Client.Start used to make.
func (s *Store) StartWorkflow(ctx context.Context, p driver.WorkflowStartParams) (bool, uuid.UUID, error) {
	taskQueue := p.TaskQueue
	if taskQueue == "" {
		taskQueue = "default"
	}
	metaJSON, err := json.Marshal(orEmptyMeta(p.Meta))
	if err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: marshal workflow meta: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: start workflow begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var id pgtype.UUID
	err = tx.QueryRow(ctx, insertWorkflowSQL,
		p.ID, p.Name, p.Version, p.BusinessIdempotencyKey, taskQueue,
		nullableRawJSON(p.Input), string(metaJSON),
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		// DO NOTHING fired: a live execution holds (Name, BusinessIdempotencyKey).
		// This only happens for a non-empty key, since the partial index excludes
		// NULL keys, so the SELECT below always finds exactly one live row.
		var existing pgtype.UUID
		if err := s.pool.QueryRow(ctx, selectLiveWorkflowSQL, p.Name, p.BusinessIdempotencyKey).Scan(&existing); err != nil {
			return false, uuid.Nil, fmt.Errorf("azyncpgx: resolve deduplicated workflow: %w", err)
		}
		return false, toUUID(existing), nil
	}
	if err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: insert workflow: %w", err)
	}

	if _, err := tx.Exec(ctx, insertFirstHistorySQL, p.ID, nullableRawJSON(p.Input)); err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: record workflow started: %w", err)
	}
	kind := workflowTaskKindFmt(p.Name)
	if _, err := tx.Exec(ctx, scheduleTaskSQL, uuid.New(), kind, p.ID, nullableTime(time.Time{})); err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: schedule first workflow task: %w", err)
	}
	if err := s.bumpStat(ctx, tx, driver.SourceWorkflow, kind, statEnqueued, 1); err != nil {
		return false, uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: start workflow commit: %w", err)
	}
	s.notifyAfterCommit(driver.SourceWorkflow, kind) //nolint:contextcheck // deliberately independent of ctx, see notifyAfterCommit doc
	return true, uuid.Nil, nil
}

const workflowExecutionColumns = `
	id, name, version, state, COALESCE(business_idempotency_key, ''), task_queue,
	input::text, result::text, COALESCE(failure_reason, ''), meta::text,
	created_at, updated_at, completed_at`

const getWorkflowExecutionSQL = `SELECT ` + workflowExecutionColumns + ` FROM azync_workflow_executions WHERE id = $1`

// GetWorkflowExecution returns one execution header by id, or a not-found
// error when it does not exist.
func (s *Store) GetWorkflowExecution(ctx context.Context, id uuid.UUID) (driver.WorkflowExecutionView, error) {
	row := s.pool.QueryRow(ctx, getWorkflowExecutionSQL, id)
	view, err := scanWorkflowExecution(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.WorkflowExecutionView{}, driver.NewNotFound("get workflow execution")
	}
	if err != nil {
		return driver.WorkflowExecutionView{}, err
	}
	return view, nil
}

// scanWorkflowExecution scans one row shaped by workflowExecutionColumns.
func scanWorkflowExecution(row pgx.Row) (driver.WorkflowExecutionView, error) {
	var (
		view        driver.WorkflowExecutionView
		id          pgtype.UUID
		state       string
		input       *string
		result      *string
		meta        string
		completedAt pgtype.Timestamptz
	)
	if err := row.Scan(
		&id, &view.Name, &view.Version, &state, &view.BusinessIdempotencyKey, &view.TaskQueue,
		&input, &result, &view.FailureReason, &meta,
		&view.CreatedAt, &view.UpdatedAt, &completedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return driver.WorkflowExecutionView{}, err
		}
		return driver.WorkflowExecutionView{}, fmt.Errorf("azyncpgx: scan workflow execution: %w", err)
	}
	view.ID = toUUID(id)
	view.State = driver.WorkflowState(state)
	view.CompletedAt = completedAt.Time
	if input != nil {
		view.Input = json.RawMessage(*input)
	}
	if result != nil {
		view.Result = json.RawMessage(*result)
	}
	m, err := decodeMeta(meta)
	if err != nil {
		return driver.WorkflowExecutionView{}, fmt.Errorf("azyncpgx: decode workflow meta for %s: %w", view.ID, err)
	}
	view.Meta = m
	return view, nil
}

// --- AppendHistory / ListHistory ---------------------------------------------

// appendHistorySQL locks the execution row so the next sequence number is
// resolved and inserted atomically: two concurrent appenders serialize on the
// row lock instead of racing MAX(event_seq)+1 into a duplicate.
const lockWorkflowSQL = `SELECT 1 FROM azync_workflow_executions WHERE id = $1 FOR UPDATE`

const nextHistorySeqSQL = `SELECT COALESCE(MAX(event_seq), 0) + 1 FROM azync_workflow_history WHERE workflow_id = $1`

// insertHistorySQL's ON CONFLICT targets azync_workflow_history_exec_key_idx
// (migration 00007) by matching its definition exactly. That index's WHERE
// clause only matches OperationCompleted/OperationFailed rows carrying an
// execution_key, so for every other event type this is always a plain
// insert — DO NOTHING never applies outside that one case. It exists to
// close the crash window between AppendHistory and the operation job's Ack
// (processOperationJob): retrying the append after such a crash must not
// durably record the operation's outcome twice.
const insertHistorySQL = `
INSERT INTO azync_workflow_history (workflow_id, event_seq, event_type, payload)
VALUES ($1, $2, $3, $4::jsonb)
ON CONFLICT (workflow_id, event_type, (payload ->> 'execution_key'))
	WHERE payload ->> 'execution_key' IS NOT NULL
	  AND event_type IN ('OperationCompleted', 'OperationFailed')
DO NOTHING`

const findHistoryByExecKeySQL = `
SELECT event_seq FROM azync_workflow_history
WHERE workflow_id = $1 AND event_type = $2 AND payload ->> 'execution_key' = $3`

// executionKeyFromPayload extracts the execution_key field from a history
// payload without depending on operation.go's private payload struct types
// (this package has no import of the workflow package to reuse them).
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

// AppendHistory appends one durable history record with the next monotonic
// sequence number for the workflow. For an OperationCompleted/OperationFailed
// payload carrying an execution_key already recorded, it is idempotent: it
// returns the existing record's seq instead of appending a duplicate (see
// insertHistorySQL).
func (s *Store) AppendHistory(ctx context.Context, workflowID uuid.UUID, typ string, payload json.RawMessage) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: append history begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var exists int
	err = tx.QueryRow(ctx, lockWorkflowSQL, workflowID).Scan(&exists)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, driver.NewNotFound("append history")
	}
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: lock workflow for history append: %w", err)
	}

	var seq int64
	if err := tx.QueryRow(ctx, nextHistorySeqSQL, workflowID).Scan(&seq); err != nil {
		return 0, fmt.Errorf("azyncpgx: next history seq: %w", err)
	}
	tag, err := tx.Exec(ctx, insertHistorySQL, workflowID, seq, typ, nullableRawJSON(payload))
	if err != nil {
		return 0, fmt.Errorf("azyncpgx: insert history: %w", err)
	}
	if tag.RowsAffected() == 0 {
		execKey, ok := executionKeyFromPayload(payload)
		if !ok {
			return 0, fmt.Errorf("azyncpgx: insert history: conflict on %s with no execution_key in payload", typ)
		}
		if err := tx.QueryRow(ctx, findHistoryByExecKeySQL, workflowID, typ, execKey).Scan(&seq); err != nil {
			return 0, fmt.Errorf("azyncpgx: resolve deduplicated history: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("azyncpgx: append history commit: %w", err)
	}
	return seq, nil
}

const listHistorySQL = `
SELECT workflow_id, event_seq, event_type, payload::text, created_at
FROM azync_workflow_history
WHERE workflow_id = $1
ORDER BY event_seq`

// ListHistory returns the workflow's history events in sequence order.
func (s *Store) ListHistory(ctx context.Context, workflowID uuid.UUID) ([]driver.HistoryEvent, error) {
	if err := s.requireWorkflow(ctx, s.pool, "list history", workflowID); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, listHistorySQL, workflowID)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: list history: %w", err)
	}
	defer rows.Close()

	var out []driver.HistoryEvent
	for rows.Next() {
		var (
			ev      driver.HistoryEvent
			id      pgtype.UUID
			payload *string
		)
		if err := rows.Scan(&id, &ev.Seq, &ev.Type, &payload, &ev.CreatedAt); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan history event: %w", err)
		}
		ev.WorkflowID = toUUID(id)
		if payload != nil {
			ev.Payload = json.RawMessage(*payload)
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate history: %w", err)
	}
	return out, nil
}

// requireWorkflow maps a missing execution to the contract's not-found. q is
// the pool for a standalone check, or an open tx so the check runs on the
// same snapshot and connection as the statements that follow it.
func (s *Store) requireWorkflow(ctx context.Context, q querier, op string, id uuid.UUID) error {
	var exists bool
	if err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM azync_workflow_executions WHERE id = $1)`, id).Scan(&exists); err != nil {
		return fmt.Errorf("azyncpgx: %s: %w", op, err)
	}
	if !exists {
		return driver.NewNotFound(op)
	}
	return nil
}

// --- SignalWorkflow ----------------------------------------------------------

const insertSignalSQL = `
INSERT INTO azync_workflow_signals (id, workflow_id, name, message_id, payload)
VALUES ($1, $2, $3, NULLIF($4, ''), $5::jsonb)
ON CONFLICT (workflow_id, name, message_id) WHERE message_id IS NOT NULL DO NOTHING
RETURNING id`

// SignalWorkflow appends an early signal to the inbox, deduplicating by
// (WorkflowID, Name, MessageID) when MessageID is set; an empty MessageID
// disables dedupe, so every such Signal is a distinct message.
// signalHistoryPayload mirrors workflow/signal.go's unexported
// signalEventPayload wire format, for the same reason as workflowTaskKindFmt.
//
//nolint:tagliatelle // durable wire format, must match workflow/signal.go
type signalHistoryPayload struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

const selectWorkflowNameStateForUpdateSQL = `SELECT name, state FROM azync_workflow_executions WHERE id = $1 FOR UPDATE`

// SignalWorkflow atomically appends the delivery to the inbox (deduped by
// MessageID), the SignalReceived history record, and — unless the execution
// is already terminal — a wake workflow-task job, all in one transaction: a
// newly delivered signal is never left recorded with no live task able to
// act on it, closing the crash window between the separate calls Client.Signal
// used to make. The row lock (FOR UPDATE) also serializes the history
// sequence number against a concurrent AppendHistory (e.g. a workflow-task
// replay appending its own event at the same moment).
func (s *Store) SignalWorkflow(ctx context.Context, p driver.SignalParams) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("azyncpgx: signal workflow begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var name, state string
	err = tx.QueryRow(ctx, selectWorkflowNameStateForUpdateSQL, p.WorkflowID).Scan(&name, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, driver.NewNotFound("signal workflow")
	}
	if err != nil {
		return false, fmt.Errorf("azyncpgx: signal workflow lookup: %w", err)
	}

	var id pgtype.UUID
	err = tx.QueryRow(ctx, insertSignalSQL, uuid.New(), p.WorkflowID, p.Name, p.MessageID, nullableRawJSON(p.Payload)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // deduplicated by MessageID; nothing else to do
	}
	if err != nil {
		return false, fmt.Errorf("azyncpgx: signal workflow: %w", err)
	}

	var seq int64
	if err := tx.QueryRow(ctx, nextHistorySeqSQL, p.WorkflowID).Scan(&seq); err != nil {
		return false, fmt.Errorf("azyncpgx: signal workflow next seq: %w", err)
	}
	sigPayload, err := json.Marshal(signalHistoryPayload{Name: p.Name, Payload: p.Payload})
	if err != nil {
		return false, fmt.Errorf("azyncpgx: marshal signal payload: %w", err)
	}
	if _, err := tx.Exec(ctx, insertHistorySQL, p.WorkflowID, seq, "SignalReceived", nullableRawJSON(sigPayload)); err != nil {
		return false, fmt.Errorf("azyncpgx: signal workflow record: %w", err)
	}

	var wakeKind string
	if !isTerminalWorkflowStateStr(state) {
		wakeKind = workflowTaskKindFmt(name)
		if _, err := tx.Exec(ctx, scheduleTaskSQL, uuid.New(), wakeKind, p.WorkflowID, nullableTime(time.Time{})); err != nil {
			return false, fmt.Errorf("azyncpgx: schedule signal wake task: %w", err)
		}
		if err := s.bumpStat(ctx, tx, driver.SourceWorkflow, wakeKind, statEnqueued, 1); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("azyncpgx: signal workflow commit: %w", err)
	}
	if wakeKind != "" {
		s.notifyAfterCommit(driver.SourceWorkflow, wakeKind) //nolint:contextcheck // deliberately independent of ctx, see notifyAfterCommit doc
	}
	return true, nil
}

// --- terminal transitions ----------------------------------------------------

const completeWorkflowSQL = `
UPDATE azync_workflow_executions
SET state = 'succeeded', result = $2::jsonb, completed_at = now(), updated_at = now()
WHERE id = $1
RETURNING id`

// CompleteWorkflow settles a workflow as succeeded, persisting the result.
func (s *Store) CompleteWorkflow(ctx context.Context, id uuid.UUID, result json.RawMessage) error {
	return s.settleWorkflow(ctx, completeWorkflowSQL, "complete workflow", id, nullableRawJSON(result))
}

const failWorkflowSQL = `
UPDATE azync_workflow_executions
SET state = 'failed', failure_reason = $2, completed_at = now(), updated_at = now()
WHERE id = $1
RETURNING id`

// FailWorkflow settles a workflow as failed, recording the reason.
func (s *Store) FailWorkflow(ctx context.Context, id uuid.UUID, reason string) error {
	return s.settleWorkflow(ctx, failWorkflowSQL, "fail workflow", id, reason)
}

const cancelWorkflowExecutionSQL = `
UPDATE azync_workflow_executions
SET state = 'cancelled', completed_at = now(), updated_at = now()
WHERE id = $1 AND state NOT IN ('succeeded', 'failed', 'cancelled')
RETURNING id`

// CancelWorkflowExecution settles a non-terminal workflow as cancelled.
func (s *Store) CancelWorkflowExecution(ctx context.Context, id uuid.UUID) error {
	return s.settleWorkflow(ctx, cancelWorkflowExecutionSQL, "cancel workflow execution", id)
}

const suspendWorkflowSQL = `
UPDATE azync_workflow_executions
SET state = 'suspended', failure_reason = $2, updated_at = now()
WHERE id = $1 AND state NOT IN ('succeeded', 'failed', 'cancelled')
RETURNING id`

// SuspendWorkflow parks a running workflow for a manual decision.
func (s *Store) SuspendWorkflow(ctx context.Context, id uuid.UUID, reason string) error {
	return s.settleWorkflow(ctx, suspendWorkflowSQL, "suspend workflow", id, reason)
}

// settleWorkflow runs one settling UPDATE ... RETURNING id and maps a
// no-op (zero rows, whether the id is missing or the row failed the
// statement's own state guard) to the contract's not-found.
func (s *Store) settleWorkflow(ctx context.Context, sql, op string, id uuid.UUID, args ...any) error {
	var scanned pgtype.UUID
	err := s.pool.QueryRow(ctx, sql, append([]any{id}, args...)...).Scan(&scanned)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.NewNotFound(op)
	}
	if err != nil {
		return fmt.Errorf("azyncpgx: %s: %w", op, err)
	}
	return nil
}

const resumeWorkflowSQL = `
UPDATE azync_workflow_executions
SET state = 'running', failure_reason = NULL, updated_at = now()
WHERE id = $1 AND state = 'suspended'
RETURNING id`

// ResumeWorkflow moves a suspended execution back to running.
func (s *Store) ResumeWorkflow(ctx context.Context, id uuid.UUID) error {
	return s.settleWorkflow(ctx, resumeWorkflowSQL, "resume workflow", id)
}

const scheduleOperationSQL = `
INSERT INTO azync_jobs
	(id, source, kind, state, payload, meta, run_at, max_attempts, max_attempts_explicit, enqueued_at, run_id)
SELECT $1, 'workflow', $2,
	CASE WHEN r.run_at > now() THEN 'scheduled' ELSE 'pending' END,
	$3::jsonb, $4::jsonb, r.run_at, $5, true, now(), $6
FROM (SELECT COALESCE($7::timestamptz, now()) AS run_at) r
RETURNING id`

const findOperationByExecKeySQL = `
SELECT id FROM azync_jobs
WHERE source = 'workflow' AND run_id = $1
  AND meta->>'execution_key' = $2
  AND state IN ('pending', 'scheduled', 'active', 'uncertain')
LIMIT 1`

// ScheduleOperation inserts one Operation task job, deduping by ExecutionKey.
func (s *Store) ScheduleOperation(ctx context.Context, p driver.ScheduleOperationParams) (uuid.UUID, error) {
	if err := s.requireWorkflow(ctx, s.pool, "schedule operation", p.WorkflowID); err != nil {
		return uuid.Nil, err
	}
	// This pre-check runs before any transaction starts, so it is a benign
	// TOCTOU against a concurrent inserter of the same execution key: the
	// unique index azync_jobs_workflow_exec_key_idx (migration 00004) is the
	// real dedupe guarantee, and the conflict fallback below resolves the
	// race by returning whichever row actually won.
	if p.ExecutionKey != "" {
		var existing pgtype.UUID
		err := s.pool.QueryRow(ctx, findOperationByExecKeySQL, p.WorkflowID, p.ExecutionKey).Scan(&existing)
		if err == nil {
			return toUUID(existing), nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, fmt.Errorf("azyncpgx: lookup operation by execution key: %w", err)
		}
	}

	meta := orEmptyMeta(p.Meta)
	if p.ExecutionKey != "" {
		if meta == nil {
			meta = map[string]string{}
		}
		meta["execution_key"] = p.ExecutionKey
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return uuid.Nil, fmt.Errorf("azyncpgx: marshal operation meta: %w", err)
	}
	maxAttempts := p.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 25
	}
	id := uuid.New()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("azyncpgx: schedule operation begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var inserted pgtype.UUID
	err = tx.QueryRow(ctx, scheduleOperationSQL,
		id, p.Kind, nullableRawJSON(p.Payload), string(metaJSON),
		maxAttempts, p.WorkflowID, nullableTime(p.RunAt),
	).Scan(&inserted)
	if err != nil {
		// Unique index race: another inserter won — return theirs. Scoped to
		// an actual unique-violation SQLSTATE so an unrelated failure (a
		// dropped connection, a statement timeout) is never silently
		// reinterpreted as "someone else already inserted it".
		var pgErr *pgconn.PgError
		if p.ExecutionKey != "" && errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			var existing pgtype.UUID
			if lookupErr := s.pool.QueryRow(ctx, findOperationByExecKeySQL, p.WorkflowID, p.ExecutionKey).Scan(&existing); lookupErr == nil {
				return toUUID(existing), nil
			}
		}
		return uuid.Nil, fmt.Errorf("azyncpgx: schedule operation: %w", err)
	}
	if err := s.bumpStat(ctx, tx, driver.SourceWorkflow, p.Kind, statEnqueued, 1); err != nil {
		return uuid.Nil, err
	}
	pending := p.RunAt.IsZero() || !p.RunAt.After(time.Now())
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("azyncpgx: schedule operation commit: %w", err)
	}
	if pending {
		s.notifyAfterCommit(driver.SourceWorkflow, p.Kind) //nolint:contextcheck // deliberately independent of ctx, see notifyAfterCommit doc
	}
	return toUUID(inserted), nil
}

const markUncertainSQL = `
WITH upd AS (
	UPDATE azync_jobs SET
		state = 'uncertain', lease_until = NULL, lease_token = NULL,
		last_error = $3, failed_at = now(), updated_at = now()
	WHERE id = $1 AND state = 'active' AND lease_token = $2 AND source = 'workflow'
	RETURNING id, run_id
)
UPDATE azync_workflow_executions e
SET state = 'suspended', failure_reason = $3, updated_at = now()
FROM upd
WHERE e.id = upd.run_id AND e.state IN ('running', 'suspended')
RETURNING upd.id, upd.run_id`

// MarkUncertain moves an active Operation to uncertain and suspends the run.
func (s *Store) MarkUncertain(ctx context.Context, operationJobID, leaseToken uuid.UUID, reason string) error {
	var jobID, runID pgtype.UUID
	err := s.pool.QueryRow(ctx, markUncertainSQL, operationJobID, leaseToken, reason).Scan(&jobID, &runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return driver.NewNotFound("mark uncertain")
	}
	if err != nil {
		return fmt.Errorf("azyncpgx: mark uncertain: %w", err)
	}
	return nil
}

// resolveUncertainSettleSQL is templated per decision (see
// resolveUncertainSettle below) but always shares the same guard and
// RETURNING clause: WHERE id = $1 AND source = 'workflow' AND state =
// 'uncertain' fences the settlement to a row that is still uncertain, in one
// statement, so a job that was re-leased back to active in between MarkUncertain
// and ResolveUncertain (another worker's reap, or a concurrent resolve) is
// never clobbered — zero rows updated maps to not-found instead.
const resolveUncertainSettleTail = `
WHERE id = $1 AND source = 'workflow' AND state = 'uncertain'
RETURNING run_id, kind, COALESCE(meta->>'workflow_name', '')`

var resolveUncertainSettleSQL = map[driver.UncertainDecision]string{
	driver.UncertainComplete: `
UPDATE azync_jobs SET state = 'succeeded', completed_at = now(), updated_at = now(),
	result = $2::jsonb, last_error = NULL` + resolveUncertainSettleTail,
	driver.UncertainFail: `
UPDATE azync_jobs SET state = 'dead', failed_at = now(), updated_at = now(),
	last_error = 'uncertain: fail'` + resolveUncertainSettleTail,
	driver.UncertainRetry: `
UPDATE azync_jobs SET state = 'pending', run_at = now(), updated_at = now(),
	last_error = NULL, failed_at = NULL` + resolveUncertainSettleTail,
}

// ResolveUncertain applies complete/fail/retry to an uncertain Operation.
// History append is the caller's responsibility (so payloads stay typed).
func (s *Store) ResolveUncertain(ctx context.Context, operationJobID uuid.UUID, decision string, result json.RawMessage) (uuid.UUID, string, error) {
	sql, ok := resolveUncertainSettleSQL[driver.UncertainDecision(decision)]
	if !ok {
		return uuid.Nil, "", fmt.Errorf("azyncpgx: unknown uncertain decision %q", decision)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("azyncpgx: resolve uncertain begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	args := []any{operationJobID}
	if driver.UncertainDecision(decision) == driver.UncertainComplete {
		args = append(args, nullableRawJSON(result))
	}

	var (
		runID  pgtype.UUID
		kind   string
		wfName string
	)
	err = tx.QueryRow(ctx, sql, args...).Scan(&runID, &kind, &wfName)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, "", driver.NewNotFound("resolve uncertain")
	}
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("azyncpgx: resolve uncertain settle job: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE azync_workflow_executions
		SET state = 'running', failure_reason = NULL, updated_at = now()
		WHERE id = $1 AND state = 'suspended'`, toUUID(runID))
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("azyncpgx: resolve uncertain resume: %w", err)
	}

	retry := driver.UncertainDecision(decision) == driver.UncertainRetry
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, "", fmt.Errorf("azyncpgx: resolve uncertain commit: %w", err)
	}
	if retry {
		s.notifyAfterCommit(driver.SourceWorkflow, kind) //nolint:contextcheck // deliberately independent of ctx, see notifyAfterCommit doc
	}
	return toUUID(runID), wfName, nil
}

// --- ScheduleTask ------------------------------------------------------------

// scheduleTaskSQL resolves run_at and the pending/scheduled split against the
// backend clock, exactly like enqueueInsertSQL for queue jobs.
const scheduleTaskSQL = `
INSERT INTO azync_jobs
	(id, source, kind, state, run_at, max_attempts, max_attempts_explicit, meta, enqueued_at, run_id)
SELECT $1, 'workflow', $2,
	CASE WHEN r.run_at > now() THEN 'scheduled' ELSE 'pending' END,
	r.run_at, 0, false, '{}'::jsonb, now(), $3
FROM (SELECT COALESCE($4::timestamptz, now()) AS run_at) r`

// ScheduleTask durably inserts one workflow-task job (Source SourceWorkflow,
// RunID = workflowID), born pending when runAt is due, scheduled otherwise,
// and signals workers for an immediately-runnable job.
func (s *Store) ScheduleTask(ctx context.Context, workflowID uuid.UUID, kind string, runAt time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("azyncpgx: schedule workflow task begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := s.requireWorkflow(ctx, tx, "schedule workflow task", workflowID); err != nil {
		return err
	}

	pending := runAt.IsZero() || !runAt.After(time.Now())
	if _, err := tx.Exec(ctx, scheduleTaskSQL, uuid.New(), kind, workflowID, nullableTime(runAt)); err != nil {
		return fmt.Errorf("azyncpgx: schedule workflow task: %w", err)
	}
	if err := s.bumpStat(ctx, tx, driver.SourceWorkflow, kind, statEnqueued, 1); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("azyncpgx: schedule workflow task commit: %w", err)
	}
	if pending {
		s.notifyAfterCommit(driver.SourceWorkflow, kind) //nolint:contextcheck // deliberately independent of ctx, see notifyAfterCommit doc
	}
	return nil
}

const vacuumWorkflowsSQL = `
DELETE FROM azync_workflow_executions
WHERE state IN ('succeeded', 'failed', 'cancelled')
	AND completed_at IS NOT NULL
	AND completed_at < now() - make_interval(secs => $1)`

// VacuumWorkflows deletes terminal workflow-as-code executions completed
// before retention ago, cascading (via FKs) to history, signals, timers and
// jobs linked by run_id. A retention <= 0 removes nothing.
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

// listStalledWorkflowsSQL finds running executions whose last update is
// older than the caller's grace window and that have no non-terminal
// source=workflow job pointing at them. See driver.WorkflowStore's doc
// comment for why this is a safety net, not the primary mechanism.
const listStalledWorkflowsSQL = `
SELECT e.id, e.name FROM azync_workflow_executions e
WHERE e.state = 'running'
  AND e.updated_at < now() - make_interval(secs => $1)
  AND NOT EXISTS (
    SELECT 1 FROM azync_jobs j
    WHERE j.run_id = e.id AND j.source = 'workflow'
      AND j.state IN ('pending', 'scheduled', 'active', 'uncertain')
  )
ORDER BY e.updated_at
LIMIT $2`

// ListStalledWorkflows returns up to limit running executions with no live
// task, updated more than olderThan ago.
func (s *Store) ListStalledWorkflows(ctx context.Context, olderThan time.Duration, limit int) ([]driver.StalledWorkflow, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, listStalledWorkflowsSQL, olderThan.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("azyncpgx: list stalled workflows: %w", err)
	}
	defer rows.Close()
	var out []driver.StalledWorkflow
	for rows.Next() {
		var id pgtype.UUID
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("azyncpgx: scan stalled workflow: %w", err)
		}
		out = append(out, driver.StalledWorkflow{ID: toUUID(id), Name: name})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("azyncpgx: iterate stalled workflows: %w", err)
	}
	return out, nil
}
