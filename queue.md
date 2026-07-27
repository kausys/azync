# queue

Durable typed background jobs on a shared [`azync.Core`](README.md). Rows live in `azync_jobs` with `source=queue`.

Package notes (internals): [`queue/README.md`](queue/README.md) · Example: [`examples/queue-basic`](https://github.com/kausys/azync/tree/main/examples/queue-basic)

## Install

```sh
go get github.com/kausys/azync@latest
go get github.com/kausys/azync/driver/azyncpgx@latest
```

Requirements: Go 1.26+, PostgreSQL 13+.

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

err = queue.Register(q.Worker(), func(ctx context.Context, job WelcomeEmail) error {
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

producer, err := queue.TxProducer[pgx.Tx](q) // needs driver.TxStore[pgx.Tx]
if err != nil { /* driver does not support tx enlist */ }

err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    if _, err := tx.Exec(ctx, `insert into orders (id) values ($1)`, orderID); err != nil {
        return err
    }
    _, err := producer.EnqueueTx(ctx, tx, SendReceipt{OrderID: orderID})
    return err
})
```

`tx` must come from the same pool the azync store uses. Prefer building the pool yourself, `azyncpgx.New(pool)`, then `azync.New(store)`.

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

[event.md](event.md) · [dag.md](dag.md) · [workflow.md](workflow.md) · [README](README.md)
