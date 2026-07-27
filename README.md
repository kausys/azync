# azync

[![Tests](https://github.com/kausys/azync/actions/workflows/test.yml/badge.svg)](https://github.com/kausys/azync/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/kausys/azync)](https://goreportcard.com/report/github.com/kausys/azync)

Durable background work for Go over one job table and a pluggable `driver.Store`. First-party PostgreSQL driver: `azyncpgx`.

## Runtimes

| Package | `source` | Model | Guide | Package notes |
|---------|----------|-------|-------|---------------|
| [`queue`](queue/) | `queue` | Typed jobs, cron, transactional outbox | [queue.md](queue.md) | [README](queue/README.md) |
| [`event`](event/) | `event` | CQRS ledger + fan-out deliveries + Replay | [event.md](event.md) | [README](event/README.md) |
| [`dag`](dag/) | `dag` | Static declared graph (no replay) | [dag.md](dag.md) | [README](dag/README.md) |
| [`workflow`](workflow/) | `workflow` | Workflow-as-code (Go + history replay) | [workflow.md](workflow.md) | [README](workflow/README.md) |

All four compose over one `azync.Core` and never import each other.

- **Guides** (`queue.md`, …) — how to use the runtime in an application.
- **Package notes** (`queue/README.md`, …) — technical layout, driver surface, boundaries for maintainers.

## Why azync

- **One table for runnable work.** Queue jobs, event deliveries, DAG tasks, and workflow tasks share `azync_jobs`, partitioned by `source`.
- **Durable event bus.** Insert-only ledger, atomic fan-out, Replay from history.
- **Transactional outbox.** Enlist enqueue / publish / DAG run in your own backend transaction.
- **Driver contract.** Implement `driver.Store` (+ optional capabilities); validate with the conformance suite.
- **Lease fencing + reaper.** At-least-once delivery with real fencing tokens.
- **Two orchestration styles.** Declared graphs ([dag.md](dag.md)) or ordinary Go with deterministic replay ([workflow.md](workflow.md)).

## Install

```sh
go get github.com/kausys/azync@latest
go get github.com/kausys/azync/driver/azyncpgx@latest
```

Go 1.26+, PostgreSQL 13+.

```go
import (
    "github.com/kausys/azync"
    "github.com/kausys/azync/queue"
    "github.com/kausys/azync/event"
    "github.com/kausys/azync/dag"
    "github.com/kausys/azync/workflow"

    _ "github.com/kausys/azync/driver/azyncpgx"
)

core, err := azync.Open(dsn)
if err != nil { /* ... */ }
if err := core.Migrate(ctx); err != nil { /* ... */ } // always explicit

q, _ := queue.New(core)
ev, _ := event.New(core)
d, _ := dag.New(core)       // needs driver.DAGStore
wf, _ := workflow.New(core) // needs driver.WorkflowStore
```

`Open` / `New` never migrate. Prefer one shared Core per process ([`examples/shared-core`](https://github.com/kausys/azync/tree/main/examples/shared-core)); use `queue.Open` / `event.Open` / … when a runtime should own a private Core.

## Guides and examples

| Guide | Example |
|-------|---------|
| [queue.md](queue.md) | [`examples/queue-basic`](https://github.com/kausys/azync/tree/main/examples/queue-basic) |
| [event.md](event.md) | [`examples/event-basic`](https://github.com/kausys/azync/tree/main/examples/event-basic) |
| [dag.md](dag.md) | [`examples/dag-basic`](https://github.com/kausys/azync/tree/main/examples/dag-basic) |
| [workflow.md](workflow.md) | [`examples/workflow-kyc`](https://github.com/kausys/azync/tree/main/examples/workflow-kyc) |

## Roadmap

- **Concurrent Operations in workflow-as-code.** Today `ExecuteOperation`
  always parks — Operations are strictly serial, which is what rules the
  workflow runtime out for fan-out shapes (N verifications in parallel; use
  `dag` for those). The design: an `ExecuteOperationAsync` that records
  `OperationScheduled` *without* parking and returns an unready `Future`;
  `Select` extended to park on Operation futures; and replay matching keyed
  by `execution_key` instead of strict sequence, so out-of-order completions
  replay deterministically. With that, workflow-as-code supports fan-out and
  the dag-vs-workflow choice becomes expressiveness, not capability.
- **Workflow runtime on the shared engine.** The workflow worker bypasses
  `internal/engine` today, so it lacks the consumer spans, metrics and trace
  propagation the other three runtimes get for free. Migrating it is a large
  refactor with an observable payoff.
- **Compensation in workflow-as-code** stays a documented pattern (the
  workflow function can run its own compensating Operations after a failed
  step — it is plain code), not a new primitive; `dag` keeps first-class
  `Compensate` for declarative chains.
- Workflow-as-code hardening (continue-as-new, child workflows, richer archives)
- Scheduled DAGs; first-class parent/child links
- Admin UI over the `Manager` surfaces
- `database/sql` driver and more backends

## License

[MIT](LICENSE)
