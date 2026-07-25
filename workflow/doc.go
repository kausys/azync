// Package workflow is the workflow-as-code (WAC) runtime: workflows and
// Operations defined as ordinary Go functions, executed by deterministic
// replay over an append-only history. It is a sibling of package dag (the
// static-DAG runtime) over the same [azync.Core]; the two never import each
// other (see the repository's boundary test).
//
// User guide: workflow.md at the repository root.
//
//   - Client.Start creates a workflow execution, deduplicated by
//     (name, BusinessIdempotencyKey) against live executions.
//   - Workflow and Operation functions register by (name, version) with
//     RegisterWorkflow and RegisterOperation.
//   - Worker replays a workflow function against its history on every
//     workflow-task job. ExecuteOperation schedules a leased Operation job
//     (source=workflow, kind "$op:name@version") and parks until the
//     Operation executor records OperationCompleted / OperationFailed and
//     wakes a fresh workflow-task. Sleep and WaitSignal return Futures;
//     Select parks when none are ready.
//   - Operation handlers may return ErrUncertain (or hit StartToClose
//     timeout) to enter StateUncertain and suspend the run; Manager.ResolveUncertain
//     applies complete / fail / retry.
//   - Client.Signal appends the signal to the inbox (deduped by MessageID),
//     records SignalReceived in history, and wakes a workflow-task job.
//
// Terminal executions are vacuumed after WithRetention (default 30 days;
// 0 = forever). Side effects are at-least-once — make Operations idempotent
// via ExecutionKey / business keys. Operation handlers receive lease heartbeats
// (ExtendLease every LeaseTTL/2) while they run.
package workflow
