# queue (package)

Import: `github.com/kausys/azync/queue`

User guide: [../queue.md](../queue.md) · GoDoc: package docs via `go doc` / pkg.go.dev.

## Role

Producer / worker / manager for durable jobs with `source=queue` on a shared [`azync.Core`](../README.md).

## Source layout

| File / area | Responsibility |
|-------------|----------------|
| `queue.go`, `open.go` | `New` / `Open`, composition over Core |
| `producer.go`, `tx.go` | Enqueue, transactional outbox |
| `worker.go`, `register.go` | Consume + typed handlers |
| `manager.go`, `cron.go` | Admin API, leader cron |
| `context.go`, `options.go` | Job metadata on `context`, knobs |

## Driver surface

Requires Core + job store. Cron needs `driver.LeaderElector`. No DAG/WorkflowStore.

## Public surface (summary)

- `New` / `Open` → `Producer`, `Worker`, `Manager`
- `Register` / `RegisterCron`
- `TxProducer[T]`
- Enqueue / worker / manager options (see GoDoc)

## Boundaries

- Does not own schema migrations (`Core.Migrate`).
- Does not implement HTTP admin — `Manager` is a library API.
- Sibling packages (`event`, `dag`, `workflow`) share Core; they do not import `queue`.

## Tests

`go test ./queue/...` · driver conformance via `driver/drivertest`.
