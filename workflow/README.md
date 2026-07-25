# workflow (package)

Import: `github.com/kausys/azync/workflow`

User guide: [../workflow.md](../workflow.md).

## Role

Workflow-as-code runtime: deterministic replay of registered Go functions over append-only history. Effects only via leased Operations.

## Source layout

| Path | Responsibility |
|------|----------------|
| `workflow.go`, `open.go` | `New` / `Open`, composition |
| `client.go`, `manager.go` | Start/Signal; admin + `ResolveUncertain` |
| `worker.go`, `register.go` | Workflow + Operation registration, run loop |
| `operation.go` | Leased Operation execution, heartbeats, settlement |
| `future.go`, `selector.go`, `sleep.go`, `signal.go` | Futures / Select / timers / signals |
| `context.go`, `options.go`, `retention.go` | Replay clock, knobs, vacuum ticker |
| [`kernel/`](kernel/) | Pure in-memory history cursor + replay (no I/O) |

### `workflow/kernel`

Internal engine: append-only event log, command cursor, replay/park decisions. No driver, no network, no SQL.

- Runtime (`workflow`) loads history from `WorkflowStore`, feeds `kernel`, persists new events, schedules jobs.
- Apps should import `workflow`, not `kernel`, unless building tools/tests against the pure engine.
- Name is **kernel** (core of the WAC runtime), not a separate product.

## Driver surface

Requires `driver.WorkflowStore` (+ Core). Migrations: `00003_workflows.sql`, `00004_operation_uncertain.sql`. Jobs with `run_id` / `source=workflow`.

## Public surface (summary)

- `RegisterWorkflow` / `RegisterOperation`
- `Client.Start`, `Client.Signal`
- `ExecuteOperation`, `Sleep`, `WaitSignal`, `Select`
- `Manager.Get` / `Cancel` / `ResolveUncertain`
- `WithRetention`, `WithBusinessIdempotencyKey`, worker modes, lease/timeouts

## Boundaries

- No import of `dag` (sibling on Core only).
- Non-determinism in workflow code is a bug; I/O belongs in Operations.
- Ambiguous Operation settlement → job `uncertain` + execution `suspended` until admin resolve.
- Terminal executions vacuumed by retention; completed workflow jobs exempt from Core completed-job vacuum while `run_id` is set.

## Tests

`go test ./workflow/...` · `./workflow/kernel/...` · WAC conformance in `driver/drivertest`.
