-- +goose NO TRANSACTION
-- +goose Up
-- A crash between AppendHistory and the operation job's Ack (see
-- processOperationJob) could previously append a second OperationCompleted /
-- OperationFailed record for the same execution_key on replay, and the next
-- replay's cursor would then misattribute that stray event's outcome to a
-- later, unrelated ExecuteOperation call. Clear any duplicates that window
-- already produced, keeping the earliest (lowest event_seq) copy, before the
-- unique index below would otherwise fail to build against them.
DELETE FROM azync_workflow_history a
USING azync_workflow_history b
WHERE a.workflow_id = b.workflow_id
  AND a.event_type = b.event_type
  AND a.event_type IN ('OperationCompleted', 'OperationFailed')
  AND a.payload ->> 'execution_key' IS NOT NULL
  AND a.payload ->> 'execution_key' = b.payload ->> 'execution_key'
  AND a.event_seq > b.event_seq;

-- CONCURRENTLY + NO TRANSACTION (idempotent via IF NOT EXISTS) so building
-- this on a populated azync_workflow_history never blocks concurrent
-- AppendHistory calls (each holds a row lock on azync_workflow_executions
-- for the duration of its own append).
--
-- Scoped to OperationScheduled's result events specifically: 00004 gives
-- ScheduleOperation its own dedupe index (also on execution_key) for the job
-- row, and OperationScheduled legitimately shares its key with its eventual
-- result, so scoping this one to the two result types keeps the two
-- constraints from ever conflicting.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS azync_workflow_history_exec_key_idx
    ON azync_workflow_history (workflow_id, event_type, (payload ->> 'execution_key'))
    WHERE payload ->> 'execution_key' IS NOT NULL
      AND event_type IN ('OperationCompleted', 'OperationFailed');

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS azync_workflow_history_exec_key_idx;
