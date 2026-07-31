# event

Durable CQRS-style event bus on the same [`azync.Core`](README.md) as queue.

`Publish` appends to an insert-only ledger (`azync_events`) and, in the same transaction, fans out one delivery job per matching subscriber (`source=event`). On dequeue, the payload is rehydrated from the ledger — that is what makes admin Replay possible without keeping the original publish call around.

Package notes: [`event/README.md`](event/README.md) · Examples: [`examples/event-basic`](https://github.com/kausys/azync/tree/main/examples/event-basic), projector in [`examples/shared-core`](https://github.com/kausys/azync/tree/main/examples/shared-core)

## Install / compose

Same install as [queue](queue.md). Blank-import the driver; migrate explicitly.

```go
ev, err := event.New(core)
// or: event.Open(dsn) for a private Core
```

## Subscribe and publish

Unlike [queue](queue.md) (one enqueue → one job), **one publish fans out**: each matching subscriber gets its own delivery job, with its own retries and dead-letter state.

Events implement `EventType()`:

```go
type UserSignedUp struct {
    Email string `json:"email"`
}

func (UserSignedUp) EventType() string { return "app.user.signed_up" }

// Three independent subscribers for the same event type.
_ = event.RegisterFunc(ev.Worker(), "welcome-email",
    func(ctx context.Context, e UserSignedUp) error {
        log.Printf("welcome %s", e.Email)
        return nil
    })
_ = event.RegisterFunc(ev.Worker(), "crm-sync",
    func(ctx context.Context, e UserSignedUp) error {
        log.Printf("crm upsert %s", e.Email)
        return nil
    })
_ = event.RegisterFunc(ev.Worker(), "analytics",
    func(ctx context.Context, e UserSignedUp) error {
        log.Printf("track signup %s (replay %t)", e.Email, event.IsReplay(ctx))
        return nil
    })

go func() {
    if err := ev.Worker().Start(ctx); err != nil {
        log.Fatal(err)
    }
}()
<-ev.Worker().Ready() // wait: Start upserts durable subscriptions first

// One Publish → three delivery jobs (welcome-email, crm-sync, analytics).
id, err := ev.Publisher().Publish(ctx, UserSignedUp{Email: "ada@example.com"})
```

Subscriptions are durable and unique per `(subscriber name, event type)`. Prefer `RegisterFunc` for a single-type handler; use `Worker.Register` + `event.On` when one subscriber binds several types.

**Wait for `Ready()` before the first publish** in the same process, or a brand-new subscription may miss that publish. New subscribers only receive **future** events — backfill with Replay.

### Publish options

| Option | Effect |
|--------|--------|
| `event.WithAggregate(type, id)` | Aggregate coordinates on the ledger row |
| `event.WithVersion(v)` | Event version |
| `event.WithMeta(k, v)` | Opaque metadata (use for tenant, trace ids, etc.) |

### Handler errors

| Return | Meaning |
|--------|---------|
| `nil` | Ack delivery |
| plain `error` | Retry |
| `event.Permanent(err)` | Dead letter (no more retries) |

Deliveries are at-least-once and **not ordered** across subscribers. Deduplicate with `(EventID, SubscriberName)` if your projector needs it.

### Context accessors

`EventID`, `Type`, `OccurredAt`, `SubscriberName`, `Attempt`, `MaxAttempts`, `IsRetry`, `IsReplay`, `AggregateType`, `AggregateID`, `Version`, `Metadata`.

## Transactional publish

Publish inside your own transaction (outbox). Rollback → no ledger row, no deliveries.

```go
import "github.com/jackc/pgx/v5"

publisher, err := event.TxPublisher[pgx.Tx](ev)
if err != nil { /* ... */ }

err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
    // your writes...
    _, err := publisher.PublishTx(ctx, tx, UserSignedUp{Email: email})
    return err
})
```

Same pool rule as queue: `tx` must belong to the store’s pool.

## Replay

Creates **new** delivery jobs from ledger history for a subscriber (does not rewrite the past):

```go
report, err := ev.Manager().Replay(ctx, event.ReplayFilter{
    Subscriber: "welcome-email",
    EventType:  "app.user.signed_up",
    Since:      since,
    Until:      until,
    Limit:      1000,
})
```

Handlers see `event.IsReplay(ctx) == true`.

## Admin

`ev.Manager()` — library only (wrap with your authz):

- Events / deliveries / subscribers: `List`, `Get`, `ListDeliveries`, `ListSubscribers`, `Stats`
- `Retry`, `RetryDead`, `Replay`
- Ledger retention: `Retain(before, limit)`

There is no pause/purge/delete surface like queue’s Manager.

## See also

[queue.md](queue.md) · [dag.md](dag.md) · [workflow.md](workflow.md) · [watch.md](watch.md) · [README](README.md)
