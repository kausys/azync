# Using DAG

A **DAG** is a Directed Acyclic Graph: tasks with explicit dependencies, no cycles. You declare the graph up front; azync runs each node as an ordinary at-least-once job when its upstreams are done.

Static durable DAG executions on a shared [`azync.Core`](README.md). The graph is **data**; each task handler is a typed function.

Jobs use `source=dag`. Headers live in `azync_dags`; edges in `azync_dag_deps`; task rows link via `dag_id`.

Package notes: [`dag/README.md`](dag/README.md) · Example: [`examples/dag-basic`](examples/dag-basic)

Requires a driver that implements `driver.DAGStore` (azyncpgx does).

## Compose

```go
d, err := dag.New(core)
// or: dag.Open(dsn)
```

Migrate the Core before running. Register every task kind before `Worker.Start`.

## Declare and run

```go
def := dag.Define("onboard", dag.OnFailure(dag.Suspend)).
    Task("create", CreateAccount{Email: email},
        dag.Compensate(DeleteAccount{Email: email})).
    Sleep("cooldown", 24*time.Hour, dag.After("create")).
    WaitSignal("approved", dag.After("cooldown")).
    Task("activate", Activate{}, dag.After("approved"))

res, err := d.Client().Run(ctx, def, dag.WithIdempotencyKey(key))
// res.ID, res.Deduplicated
```

Register typed handlers (optional result type; use `dag.None` when there is none):

```go
dag.Register(d.Worker(), func(ctx context.Context, c CreateAccount) (Account, error) {
    return Account{ID: "acct_1"}, nil
})

go d.Worker().Start(ctx)
```

Downstream tasks read upstream outputs with:

```go
acct, err := dag.ResultOf[Account](ctx, "create")
```

### Primitives

| Primitive | Role |
|-----------|------|
| `Task` / `Register` | Typed work; optional result for `ResultOf` |
| `Sleep` | Durable timer; early wake via `Client.Signal` on the sleep **key** |
| `WaitSignal` | Park until `Client.Signal`; payload available via `ResultOf` |
| `After(keys...)` | Explicit dependencies |
| `Compensate(args)` | Saga step on the failure/cancel path (`comp:<key>`) |
| `OnFailure(Cancel \| Suspend)` | What happens when a task goes dead |
| `IgnoreDeadDeps()` | Tolerate a dead upstream on a branch |
| `NotReady(d)` | Re-check later **without** consuming retry budget |

Task options also include `MaxRetries(n)`. Kind strings must not use a `$` prefix (reserved).

### Context accessors

`ID`, `TaskKey`, `Attempt`, `MaxAttempts`, `IsRetry`, `Metadata`.

### Handler errors

Same family as queue: plain error → retry; `dag.Abort`, `dag.Retry`, `dag.RetryAfter`, `dag.Reportable`.

## Signals

```go
err := d.Client().Signal(ctx, res.ID, "approved", map[string]string{"by": "ops"})
// ErrNoSignalMatched if nothing is waiting on that name
```

Sleep and WaitSignal both use the node **key** as the signal name.

## Idempotent barrier

`Run` + `WithIdempotencyKey` dedupes live executions on `(definition name, key)`. Concurrent callers (including from inside another DAG task) share one live run and see `Deduplicated=true` — useful as a fan-in barrier.

## Transactional run

```go
txc, err := dag.TxRunner[pgx.Tx](d) // needs driver.TxDAGStore
res, err := txc.RunTx(ctx, tx, def, dag.WithIdempotencyKey(key))
```

## Retention

`dag.WithRetention(d)` — vacuum terminal DAGs after duration (default **30 days**; `0` = forever). Succeeded task rows are exempt from completed-job vacuum until the DAG itself is vacuumed.

## Admin

`d.Manager()`:

| Method | Role |
|--------|------|
| `Get` / `Tasks` / `List` | Inspect |
| `Retry` | Retry a failed/suspended path |
| `Compensate` | Drive compensation |
| `Cancel` | Cancel a live DAG |

States: `running`, `suspended`, `compensating`, `succeeded`, `failed`, `cancelled`.

## vs workflow-as-code

| | `dag` | `workflow` |
|--|-------|------------|
| Control flow | Declared graph (data) | Ordinary Go + history replay |
| Side effects | Task handlers | `Operation` handlers only |
| Waiting | Sleep / WaitSignal nodes | Futures + `Select` |
| Polling | `NotReady(d)` | Operation → decide → `Sleep` → Operation |
| Saga | `Compensate` / Manager | Not in MVP |
| Clock in orchestration | Wall time in handlers is fine | Must use `ctx.Now()` in workflow code |

## See also

[workflow.md](workflow.md) · [queue.md](queue.md) · [event.md](event.md) · [README](README.md)
