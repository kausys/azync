# workflow

Workflow-as-code on a shared [`azync.Core`](README.md): ordinary Go functions re-executed by **deterministic replay** over append-only history. Sibling of [`dag`](dag.md) — same Core, no shared imports.

Side effects belong only in **Operations** (leased jobs). Workflow code must be deterministic: use `ctx.Now()`, never `time.Now()`, and do not do I/O in the workflow function.

Package notes: [`workflow/README.md`](workflow/README.md) · Example: [`examples/workflow-kyc`](examples/workflow-kyc)

Requires `driver.WorkflowStore` (azyncpgx).

## Compose

```go
wf, err := workflow.New(core)
// or: workflow.Open(dsn)
```

Migrate the Core. Register every `(name, version)` for workflows and Operations **before** `Worker.Start`.

**Changing a workflow's or Operation's code always means registering it under a new version.** Replay re-executes the function against durable history, so editing the code registered under a version already in flight is a determinism violation: an in-progress execution's next replay pass may see a call sequence that no longer matches what its history recorded, and the workflow-task job retries with backoff, then suspends the execution once its retry budget is exhausted (alertable via `Manager.Get`, recoverable with `ResumeWorkflow` once the code is fixed) — never silently wrong, but not obviously connected to "I edited the handler" either. `RegisterWorkflow` / `RegisterOperation` panic immediately if called twice for the same `(name, version)`, so a copy-paste duplicate registration or a forgotten version bump fails at startup instead of at replay time.

## Register and start

```go
workflow.RegisterWorkflow(wf.Worker(), "kyc", "1",
    func(ctx workflow.Context, in KYCInput) (KYCResult, error) {
        // deterministic orchestration only
        return KYCResult{}, nil
    })

workflow.RegisterOperation(wf.Worker(), "check-status", "1",
    func(ctx context.Context, in CheckIn) (CheckOut, error) {
        // real I/O here — make it idempotent
        key := workflow.ExecutionKey(ctx) // "{workflowID}:{eventSeq}"
        _ = key
        return CheckOut{}, nil
    })

res, err := wf.Client().Start(ctx, "kyc", "1", input,
    workflow.WithBusinessIdempotencyKey(applicationID))
// res.WorkflowID, res.Deduplicated

go wf.Worker().Start(ctx)
```

A second `Start` with the same `(name, business key)` against a **live** execution returns the existing id with `Deduplicated=true`. Other start options: `WithWorkflowID`, `WithTaskQueue`, `WithStartMeta`.

### Worker modes

`WithWorkerMode`:

| Mode | Role |
|------|------|
| `WorkerModeCombined` (default) | Workflow tasks + Operations in one process |
| `WorkerModeWorkflowOnly` | Only replay / park |
| `WorkerModeOperationOnly` | Only Operation executors |

## Primitives

| API | Role |
|-----|------|
| `ExecuteOperation` | Schedule a leased Operation; returns a `Future` |
| `Sleep` / `WaitSignal` | Futures — they do **not** park by themselves |
| `Select` | **Only** way to park; first ready future wins (registration order breaks ties) |
| `Future.Get` / `Ready` / `Err` | Read the outcome after Select (or when already complete) |

```go
var out CheckOut
fut := workflow.ExecuteOperation(ctx, "check-status", "1", in)
if err := fut.Get(&out); err != nil {
    return KYCResult{}, err
}

idx, err := workflow.Select(ctx,
    workflow.WaitSignal(ctx, "approved"),
    workflow.Sleep(ctx, 24*time.Hour),
)
switch idx {
case 0:
    // approved
case 1:
    // deadline
}
```

### Polling (external status)

Do **not** use `dag.NotReady`. Pattern:

```text
ExecuteOperation(query) → decide in workflow code → Select(Sleep) → ExecuteOperation(query) → …
```

### Clock

`workflow.Context` exposes `Now()` — the backend clock for this replay pass. Using wall `time.Now` in workflow code breaks determinism.

Do not wrap or reconstruct `workflow.Context`; primitives expect the runtime’s value.

## Signals

```go
delivered, err := wf.Client().Signal(ctx, workflowID, "approved", payload,
    workflow.WithMessageID(idempotentMsgID))
```

Early signals are buffered. Duplicate `MessageID` → `delivered=false`, `err=nil`.

## Uncertain Operations

If an Operation handler returns `workflow.ErrUncertain`, or hits StartToClose timeout (`WithOperationTimeout`, default 5m), the job enters `uncertain` and the execution is **suspended** until an operator resolves it:

```go
err := wf.Manager().ResolveUncertain(ctx, operationJobID, "complete", resultJSON)
// decisions: "complete" | "fail" | "retry"
```

While an Operation runs, the runtime renews the lease every `LeaseTTL/2`. Losing the lease cancels the handler; settlement is fenced.

**At-least-once:** Operations may run more than once. Use `ExecutionKey(ctx)` or your own business keys.

## Retention and admin

- `workflow.WithRetention(d)` — vacuum terminal executions (default **30 days**; `0` = forever)
- `Manager.Get` / `Cancel` / `ResolveUncertain`

MVP Manager has no List/Retry/Compensate (those are dag concerns).

## Guarantees (short)

| Topic | Behavior |
|-------|----------|
| Workflow code | Deterministic replay; no I/O |
| Operations | At-least-once, leased, heartbeaten |
| Ambiguous side effect | `uncertain` + suspend → `ResolveUncertain` |
| Business start key | Dedupes live executions only |
| Replay/determinism error | Retries with backoff, then suspends (no version bump) |

## vs dag

| | `workflow` | `dag` |
|--|------------|-------|
| Model | Go + history | Declared graph |
| Effects | Operations only | Task handlers |
| Park | `Select` over Futures | Sleep / WaitSignal nodes |
| Saga / compensate | Not in MVP | First-class |
| Tx create | — | `TxRunner` |

## See also

[dag.md](dag.md) · [queue.md](queue.md) · [event.md](event.md) · [README](README.md)
