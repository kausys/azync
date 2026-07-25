# event (package)

Import: `github.com/kausys/azync/event`

User guide: [../event.md](../event.md) · GoDoc: package docs via `go doc` / pkg.go.dev.

## Role

CQRS event bus: insert-only ledger + fan-out delivery jobs (`source=event`) on a shared Core.

## Source layout

| File / area | Responsibility |
|-------------|----------------|
| `event.go`, `open.go` | `New` / `Open` |
| `publisher.go`, `tx.go` | Publish, transactional publish |
| `worker.go`, `register.go` | Subscriber handlers |
| `manager.go` | Admin + Replay |
| `context.go`, `options.go` | Delivery metadata, knobs |

## Driver surface

Requires Core + event ledger + delivery jobs. No DAG/WorkflowStore.

## Public surface (summary)

- `New` / `Open` → `Publisher`, `Worker`, `Manager`
- `Register` / `RegisterFunc`
- `TxPublisher[T]`
- `Manager.Replay`

## Boundaries

- Deliveries rehydrate from ledger; Replay does not need the original publish payload in-process.
- Prefer `Worker.Ready()` before first publish so new subscriptions are included in fan-out.
- Does not import `queue` / `dag` / `workflow`.

## Tests

`go test ./event/...` · driver conformance via `driver/drivertest`.
