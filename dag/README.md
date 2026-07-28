# dag (package)

Import: `github.com/kausys/azync/dag`

User guide: [../dag.md](../dag.md) · GoDoc: package docs via `go doc` / pkg.go.dev.

## Role

Static durable DAG runtime: graph is data; task handlers are at-least-once functions. Not workflow-as-code (see `workflow`).

## Source layout

| File / area | Responsibility |
|-------------|----------------|
| `dag.go`, `open.go` | `New` / `Open` |
| `define.go`, `client.go` | Graph definition, `Run` / `Signal` |
| `worker.go`, `register.go` | Task handlers, scheduling |
| `manager.go` | Admin (Get/Tasks/Stats/Definitions/TaskCounts/TaskAttempts/Retry/Compensate/Cancel) |
| `result.go`, `context.go`, `options.go` | `ResultOf`, ctx accessors, retention |

## Driver surface

Requires `driver.DAGStore` (+ Core job store). Tables: `azync_dags`, `azync_dag_deps`; jobs with `dag_id`.

## Public surface (summary)

- `Define` / `Task` / `Sleep` / `WaitSignal` / `Compensate` / `OnFailure` / …
- `Client.Run`, `Client.Signal`
- `Register`, `ResultOf[T]`, `NotReady`
- `WithRetention`, `WithIdempotencyKey`
- `Manager` — inspection (`Tasks` returns each task's `DependsOn`, so the slice
  is the graph), `Stats` / `Definitions` / `TaskCounts` for listings and the
  definition navigator, `TaskAttempts` for the failure trail, plus the operator
  verbs

## Boundaries

- No import of `workflow` (or vice versa).
- Task rows exempt from completed-job vacuum until DAG vacuum (retention).
- Compensations and dead-task policy are graph-level, not replay history.

## Tests

`go test ./dag/...` · DAG conformance in `driver/drivertest`.
