// Package workflow provides durable DAG workflows over an azync Core: a
// static task graph declared up front, executed by ordinary job machinery,
// with durable timers, signals, task results, compensation and a per-workflow
// failure policy. There is no workflow-as-code or replay: the DAG is data, and
// each task handler is a plain function that runs at-least-once.
//
// # Model
//
// A workflow is declared with Define and the builder methods:
//
//	def := workflow.Define("user-onboarding",
//		workflow.OnFailure(workflow.Suspend)).
//		Task("create", CreateAccount{Email: email},
//			workflow.Compensate(DeleteAccount{Email: email})).
//		Sleep("cooldown", 24*time.Hour, workflow.After("create")).
//		WaitSignal("approved", workflow.After("cooldown")).
//		Task("activate", ActivateAccount{Email: email}, workflow.After("approved"))
//
// Client.Run validates the definition (unique keys, existing dependencies, no
// cycles, no reserved prefixes) and inserts the whole graph atomically. Tasks
// without dependencies are immediately runnable; the rest start blocked and
// the scheduler promotes each one when everything it declared with After has
// succeeded. Fan-out and fan-in are just edges: several tasks can share a
// dependency, and one task can wait on several.
//
// # Primitives
//
// Results: a handler registered with Register returns (R, error); the R value
// is persisted atomically with the task's completion and any downstream task
// reads it with ResultOf[R](ctx, key). Tasks without output return None.
//
// Timers: Sleep parks the workflow branch for a duration, durably — no worker
// holds anything while it waits. A signal named after the sleep's key wakes it
// early.
//
// Signals: WaitSignal parks the branch until Client.Signal(id, name, payload)
// delivers; the payload becomes the task's result. Signal returns an error
// wrapping ErrNoSignalMatched when nothing was waiting.
//
// Polling-wait: a handler that finds its external condition not yet met
// returns NotReady(d); the task re-checks after d without consuming its retry
// budget — indefinitely, until it succeeds or fails with a real error.
//
// Compensation: a task may declare Compensate(args). When the workflow
// compensates, one "comp:<key>" task per succeeded task that declared one runs
// in reverse completion order (a saga).
//
// # Failure policy
//
// Each workflow declares at Define time how a dead task (aborted or out of
// retries) is handled. Cancel — the default — cancels the remaining tasks,
// runs the compensation chain and settles the workflow failed. Suspend parks
// the workflow for a manual decision through the Manager: Retry (reset dead
// tasks with a fresh budget and resume), Compensate or Cancel. A dead task
// whose dependents all declared IgnoreDeadDeps does not trigger the policy —
// the tolerant branch keeps running — but a workflow that finishes with any
// dead task settles failed, never succeeded.
//
// A workflow moves through running -> succeeded | failed | cancelled, with
// suspended (parked for an operator) and compensating (saga in flight)
// alongside. Terminal workflows are removed by the vacuum after the configured
// retention (WithWorkflowRetention, default 30 days; 0 retains forever). A
// succeeded task's row and result live for as long as its workflow does,
// regardless of the completed-job retention (azync.WithCompletedRetention,
// which is why this package has no variant of it): task jobs are exempt from
// that sweep and are only ever removed as part of their own workflow's vacuum,
// so a task
// parked behind a long Sleep or WaitSignal never loses the result ResultOf and
// CompleteWorkflows depend on.
//
// # Execution
//
// Compose a Runtime over a shared Core with New, or standalone with Open; the
// driver must implement the workflow capability (driver.WorkflowStore).
// Register handlers with Register / RegisterKind before Worker.Start. Handlers
// receive the decoded task arguments; task metadata travels on ctx (WorkflowID,
// TaskKey, Attempt, ...). Execution is at-least-once — idempotency of external
// effects belongs to the handler — and every scheduler operation is set-based
// and idempotent, so any number of worker instances can run concurrently
// without leader election.
//
// Run combined with WithIdempotencyKey is also the fan-in barrier across
// workflows: any number of concurrent Run calls with the same (name, key)
// yield exactly one live execution (see Client.Run).
package workflow
