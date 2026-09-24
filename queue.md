# queue

Durable typed background jobs on a shared [`azync.Core`](README.md). Rows live in `azync_jobs` with `source=queue`.

Package notes (internals): [`queue/README.md`](queue/README.md) · Example: [`examples/queue-basic`](https://github.com/kausys/azync/tree/main/examples/queue-basic)

## Install

```sh
go get github.com/kausys/azync@latest
go get github.com/kausys/azync/driver/azyncpgx@latest
```

Requirements: Go 1.27+, PostgreSQL 13+.

```go
import (
    "github.com/kausys/azync"
    "github.com/kausys/azync/queue"

    _ "github.com/kausys/azync/driver/azyncpgx" // registers the postgres:// scheme
)
```

`Open` / `New` never migrate. Call migrate once at deploy/startup:

```go
core, err := azync.Open(dsn)
if err != nil { /* ... */ }
if err := core.Migrate(ctx); err != nil { /* ... */ }

q, err := queue.New(core)
// or: q, err := queue.Open(dsn)  // owns a private Core; Close closes it
```

## Produce and consume

Jobs are typed values that implement `Kind()`:

```go
type WelcomeEmail struct {
    To string `json:"to"`
}

func (WelcomeEmail) Kind() string { return "app.email.welcome" }

err = q.Worker().Register(func(ctx context.Context, job WelcomeEmail) error {
    log.Printf("send to %s (attempt %d)", job.To, queue.Attempt(ctx))
    return nil
})
if err != nil { /* duplicate kind or already started */ }

res, err := q.Producer().Enqueue(ctx, WelcomeEmail{To: "ada@example.com"})
// res.ID, res.Deduplicated

go func() {
    if err := q.Worker().Start(ctx); err != nil {
        log.Fatal(err)
    }
}()
```

`Worker.Start` blocks until `ctx` is cancelled. Register every kind before `Start`.

### Enqueue options

| Option | Effect |
|--------|--------|
| `queue.Delay(d)` | Run after duration |
| `queue.At(t)` | Run at time (wins over Delay) |
| `queue.IdempotencyKey(k)` | Dedupe while a live job with key exists |
| `queue.IdempotencyKeyTTL(k, window)` | Dedupe window |
| `queue.CoalesceKey(k)` | Drop while a job of the same kind and key has not started; never while one is running |
| `queue.MaxRetries(n)` | Per-enqueue retry budget |
| `queue.Meta(key, value)` | Opaque metadata on the job |

### Per-kind register options

`WithConcurrency(n)`, `WithMaxRetries(n)`, `WithJobTimeout(d)` (`0` = unlimited). Runtime default job timeout is 5m (`WithDefaultJobTimeout`).

### Handler errors

| Return | Meaning |
|--------|---------|
| `nil` | Success |
| plain `error` | Retry (consumes attempt) |
| `queue.RetryAfter(d)` | Retry after delay |
| `queue.Abort(err)` | Dead letter immediately |
| `queue.Reportable(err)` | Retry + mark reportable for ops |

### Context accessors

`JobID`, `Kind`, `Attempt`, `MaxAttempts`, `IsRetry`, `EnqueuedAt`, `Metadata`.

## Transactional outbox

Enqueue inside a transaction you already opened. Rollback → no job.

```go
import "github.com/jackc/pgx/v5"

producer, err := q.TxProducer[pgx.Tx]() // needs driver.TxStore[pgx.Tx]
if err != nil { /* driver does not support tx enlist */ }

err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    if _, err := tx.Exec(ctx, `insert into orders (id) values ($1)`, orderID); err != nil {
        return err
    }
    _, err := producer.EnqueueTx(ctx, tx, SendReceipt{OrderID: orderID})
    return err
})
```

`tx` must be open against the database azync's tables live in, with the same `search_path` when the store uses `WithSchema`. It does not have to come from the store's own pool.

### Over `database/sql`

An application that reaches Postgres through `database/sql` — directly or through a library built on it — enlists its `*sql.Tx` instead. Build the Core over the store's `SQLTx` view; every runtime on that Core then resolves its transactional client for `*sql.Tx`:

```go
store := azyncpgx.New(pool, azyncpgx.WithSchema("jobs"))
core, err := azync.New(store.SQLTx())
q, err := queue.New(core)

producer, err := q.TxProducer[*sql.Tx]()

tx, err := db.BeginTx(ctx, nil)
// your writes on tx...
_, err = producer.EnqueueTx(ctx, tx, SendReceipt{OrderID: orderID})
err = tx.Commit()
```

The statements enlisted are the same ones the `pgx.Tx` path runs, and the worker wakeup is sent inside the transaction, so it fires only on commit. A Core serves one transaction type: over `SQLTx()` it refuses `pgx.Tx`.

### Through another store

`TxProducer` writes through the runtime's own driver. `TxProducerVia(store)` builds the same client over any `driver.TxStore` you hand it: the runtime still builds the job — id, payload, schedule, keys, metadata, trace context — and `store` decides where it is written. The transaction type is inferred from the store.

```go
producer, err := q.TxProducerVia(store) // any driver.TxStore[TTx]; TTx is inferred from it
```

That is how a write lands in a database the runtime does not operate, such as an outbox beside the application's tables. `event` and `dag` have the same pair: `TxPublisherVia`, `TxRunnerVia`.

## Cron

Schedules recurring enqueues when the driver implements `LeaderElector` (azyncpgx does). Spec: classic 5-field cron or `@hourly` / `@daily` / … No backfill — the leader starts from “now”.

```go
err = q.Worker().RegisterCron("nightly-digest", "0 2 * * *", DigestJob{})
```

Toggle with `queue.WithCron(false)` / `WithCronTick(d)` (default 30s).

## Admin

`q.Manager()` is a library API (no HTTP, no auth — wrap it yourself):

- Inspect: `List`, `Get`, `JobAttempts`, `Stats`, `ListQueues`, …
- Mutate: `Retry`, `RetryAll`, `Pause`, `Resume`, `Archive`, `Delete`, `Purge`, `VacuumDead`
- Dev-only: `NukeAll`

## Delivery guarantees

At-least-once with lease fencing. A worker that loses its lease cannot settle the job. Design handlers to be safe on retry (`Attempt`, idempotency keys, or your own business keys).

## See also

[event.md](event.md) · [dag.md](dag.md) · [workflow.md](workflow.md) · [watch.md](watch.md) · [README](README.md)
