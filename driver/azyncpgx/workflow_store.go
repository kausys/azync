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
	"github.com/jackc/pgx/v5/pgtype"
)

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

// StartWorkflow atomically inserts one workflow-as-code execution header,
// deduplicating by (Name, BusinessIdempotencyKey) against live (running or
// suspended) executions.
func (s *Store) StartWorkflow(ctx context.Context, p driver.WorkflowStartParams) (bool, uuid.UUID, error) {
	taskQueue := p.TaskQueue
	if taskQueue == "" {
		taskQueue = "default"
	}
	metaJSON, err := json.Marshal(orEmptyMeta(p.Meta))
	if err != nil {
		return false, uuid.Nil, fmt.Errorf("azyncpgx: marshal workflow meta: %w", err)
	}

	var id pgtype.UUID
	err = s.pool.QueryRow(ctx, insertWorkflowSQL,
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

const insertHistorySQL = `
INSERT INTO azync_workflow_history (workflow_id, event_seq, event_type, payload)
VALUES ($1, $2, $3, $4::jsonb)`

// AppendHistory appends one durable history record with the next monotonic
// sequence number for the workflow.
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
	if _, err := tx.Exec(ctx, insertHistorySQL, workflowID, seq, typ, nullableRawJSON(payload)); err != nil {
		return 0, fmt.Errorf("azyncpgx: insert history: %w", err)
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
	if err := s.requireWorkflow(ctx, "list history", workflowID); err != nil {
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

// requireWorkflow maps a missing execution to the contract's not-found.
func (s *Store) requireWorkflow(ctx context.Context, op string, id uuid.UUID) error {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM azync_workflow_executions WHERE id = $1)`, id).Scan(&exists); err != nil {
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
func (s *Store) SignalWorkflow(ctx context.Context, p driver.SignalParams) (bool, error) {
	if err := s.requireWorkflow(ctx, "signal workflow", p.WorkflowID); err != nil {
		return false, err
	}
	var id pgtype.UUID
	err := s.pool.QueryRow(ctx, insertSignalSQL, uuid.New(), p.WorkflowID, p.Name, p.MessageID, nullableRawJSON(p.Payload)).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // deduplicated
	}
	if err != nil {
		return false, fmt.Errorf("azyncpgx: signal workflow: %w", err)
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
	if err := s.requireWorkflow(ctx, "schedule operation", p.WorkflowID); err != nil {
		return uuid.Nil, err
	}
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
		// Unique index race: another inserter won — return theirs.
		if p.ExecutionKey != "" {
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
	if pending {
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, s.notifyChannel, notifyPayload(driver.SourceWorkflow, p.Kind)); err != nil {
			return uuid.Nil, fmt.Errorf("azyncpgx: schedule operation notify: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("azyncpgx: schedule operation commit: %w", err)
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

const resolveUncertainLoadSQL = `
SELECT id, run_id, COALESCE(meta->>'workflow_name', ''), state
FROM azync_jobs
WHERE id = $1 AND source = 'workflow'`

// ResolveUncertain applies complete/fail/retry to an uncertain Operation.
// History append is the caller's responsibility (so payloads stay typed).
func (s *Store) ResolveUncertain(ctx context.Context, operationJobID uuid.UUID, decision string, result json.RawMessage) (uuid.UUID, string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("azyncpgx: resolve uncertain begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		id, runID pgtype.UUID
		wfName    string
		state     string
	)
	err = tx.QueryRow(ctx, resolveUncertainLoadSQL, operationJobID).Scan(&id, &runID, &wfName, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, "", driver.NewNotFound("resolve uncertain")
	}
	if err != nil {
		return uuid.Nil, "", fmt.Errorf("azyncpgx: resolve uncertain load: %w", err)
	}
	if state != string(driver.StateUncertain) {
		return uuid.Nil, "", driver.NewNotFound("resolve uncertain")
	}

	switch driver.UncertainDecision(decision) {
	case driver.UncertainComplete:
		_, err = tx.Exec(ctx, `
			UPDATE azync_jobs SET state = 'succeeded', completed_at = now(), updated_at = now(),
				result = $2::jsonb, last_error = NULL
			WHERE id = $1`, operationJobID, nullableRawJSON(result))
	case driver.UncertainFail:
		_, err = tx.Exec(ctx, `
			UPDATE azync_jobs SET state = 'dead', failed_at = now(), updated_at = now(),
				last_error = 'uncertain: fail'
			WHERE id = $1`, operationJobID)
	case driver.UncertainRetry:
		_, err = tx.Exec(ctx, `
			UPDATE azync_jobs SET state = 'pending', run_at = now(), updated_at = now(),
				last_error = NULL, failed_at = NULL
			WHERE id = $1`, operationJobID)
	default:
		return uuid.Nil, "", fmt.Errorf("azyncpgx: unknown uncertain decision %q", decision)
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

	if driver.UncertainDecision(decision) == driver.UncertainRetry {
		var kind string
		if err := tx.QueryRow(ctx, `SELECT kind FROM azync_jobs WHERE id = $1`, operationJobID).Scan(&kind); err != nil {
			return uuid.Nil, "", err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, s.notifyChannel, notifyPayload(driver.SourceWorkflow, kind)); err != nil {
			return uuid.Nil, "", fmt.Errorf("azyncpgx: resolve uncertain notify: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, "", fmt.Errorf("azyncpgx: resolve uncertain commit: %w", err)
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

	if err := s.requireWorkflow(ctx, "schedule workflow task", workflowID); err != nil {
		return err
	}

	pending := runAt.IsZero() || !runAt.After(time.Now())
	if _, err := tx.Exec(ctx, scheduleTaskSQL, uuid.New(), kind, workflowID, nullableTime(runAt)); err != nil {
		return fmt.Errorf("azyncpgx: schedule workflow task: %w", err)
	}
	if err := s.bumpStat(ctx, tx, driver.SourceWorkflow, kind, statEnqueued, 1); err != nil {
		return err
	}
	if pending {
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, s.notifyChannel, notifyPayload(driver.SourceWorkflow, kind)); err != nil {
			return fmt.Errorf("azyncpgx: schedule workflow task notify: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("azyncpgx: schedule workflow task commit: %w", err)
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
