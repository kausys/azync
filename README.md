# azync

[![Tests](https://github.com/kausys/azync/actions/workflows/test.yml/badge.svg)](https://github.com/kausys/azync/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/kausys/azync)](https://goreportcard.com/report/github.com/kausys/azync)

Durable background jobs and a CQRS event bus for Go, unified over a single table. PostgreSQL driver included; pluggable storage backends.

## Why azync

- **One table for jobs and event deliveries.** Queue jobs and event deliveries live in the same `azync_jobs` table, partitioned by source. One schema, one migration, one set of admin operations (list, retry, pause, purge, vacuum) for both.
- **A durable event bus built in, not bolted on.** `Publish` appends to an insert-only ledger and fans out one delivery per subscriber atomically, with replay from history. This is normally something you hand-roll on top of a plain job queue.
- **Transactional enqueue/publish with your own data.** `TxProducer`/`TxPublisher` enlist the insert in your own backend transaction — a real outbox, so a rollback means no job and no event, not a partial write.
- **A driver abstraction, not a PostgreSQL-only library.** `driver.Store` is a public, frozen contract. `azyncpgx` is the first-party PostgreSQL implementation; third-party backends validate themselves against the same conformance suite it is held to.
- **Lease fencing and a reaper, proven by tests.** At-least-once delivery that means what it says: a worker that lost its lease cannot settle a job another worker now owns, and jobs stuck behind a dead worker are reclaimed instead of lost.
- **Durable DAG workflows, same table.** Declare a static task graph — dependencies, durable timers, signals, typed task results, saga compensation and a per-workflow failure policy — run by the same at-least-once machinery as a third `source` alongside queue and event. The DAG is data, not code: no workflow-as-code, no replay.

## Installation

```sh
go get github.com/kausys/azync@latest
go get github.com/kausys/azync/driver/azyncpgx@latest
```

Requirements: Go 1.26+, PostgreSQL 13+.

## Quickstart — jobs

```go
package main

import (
	"context"
	"log"

	"github.com/kausys/azync"
	"github.com/kausys/azync/queue"

	// Blank import registers the "postgres" DSN scheme.
	_ "github.com/kausys/azync/driver/azyncpgx"
)

type WelcomeEmail struct {
	To string `json:"to"`
}

func (WelcomeEmail) Kind() string { return "app.email.welcome" }

func main() {
	ctx := context.Background()

	core, err := azync.Open("postgres://azync:azync@localhost:5432/azync?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer core.Close(ctx)

	// Migrate is always explicit; Open never touches the schema.
	if err := core.Migrate(ctx); err != nil {
		log.Fatal(err)
	}

	q, err := queue.New(core)
	if err != nil {
		log.Fatal(err)
	}

	// The handler receives the decoded job value directly; per-job metadata
	// (attempt, id, enqueue time, ...) travels on ctx via the queue accessors.
	err = queue.Register(q.Worker(), func(ctx context.Context, job WelcomeEmail) error {
		log.Printf("sending welcome email to %s (attempt %d)", job.To, queue.Attempt(ctx))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	if _, err := q.Producer().Enqueue(ctx, WelcomeEmail{To: "ada@example.com"}); err != nil {
		log.Fatal(err)
	}

	// Start blocks until ctx is cancelled, running fetch, execute and
	// maintenance loops.
	if err := q.Worker().Start(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## Quickstart — events

```go
package main

import (
	"context"
	"log"

	"github.com/kausys/azync"
	"github.com/kausys/azync/event"

	// Blank import registers the "postgres" DSN scheme.
	_ "github.com/kausys/azync/driver/azyncpgx"
)

type UserSignedUp struct {
	Email string `json:"email"`
}

func (UserSignedUp) EventType() string { return "app.user.signed_up" }

// welcomeMailer is a subscriber: SubscriberName identifies it, and the On
// binding passed to Register declares the typed event it consumes. The handler
// receives the decoded event; delivery metadata (attempt, id, whether this is a
// retry, ...) travels on ctx and is read through the event accessors.
type welcomeMailer struct{}

func (welcomeMailer) SubscriberName() string { return "welcome-email" }

func (welcomeMailer) send(ctx context.Context, evt UserSignedUp) error {
	log.Printf("welcoming %s (attempt %d, retry %t)",
		evt.Email, event.Attempt(ctx), event.IsRetry(ctx))
	return nil
}

func main() {
	ctx := context.Background()

	core, err := azync.Open("postgres://azync:azync@localhost:5432/azync?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer core.Close(ctx)

	if err := core.Migrate(ctx); err != nil {
		log.Fatal(err)
	}

	ev, err := event.New(core)
	if err != nil {
		log.Fatal(err)
	}

	// Register the subscriber and its typed binding. Start upserts the durable
	// subscription before the worker becomes Ready, so a publish after that fans
	// out a delivery to it. RegisterFunc(ev.Worker(), "name", fn) is the
	// shorthand for a single-type subscriber that skips the interface.
	m := welcomeMailer{}
	if err := ev.Worker().Register(m, event.On(m.send)); err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := ev.Worker().Start(ctx); err != nil {
			log.Fatal(err)
		}
	}()
	<-ev.Worker().Ready()

	if _, err := ev.Publisher().Publish(ctx, UserSignedUp{Email: "ada@example.com"}); err != nil {
		log.Fatal(err)
	}

	select {} // a real service blocks on its signal context instead
}
```

## Workflows

A workflow is a durable DAG of tasks declared up front and run by the same job machinery as queues and events, under a third `source`. It adds the primitives multi-step flows need without a workflow-as-code engine:

- **Typed task results** — a handler returns `(R, error)`; `R` is persisted with the task and any downstream task reads it with `ResultOf[R](ctx, key)`.
- **Durable, interruptible timers** — `Sleep` parks a branch for a duration with no worker held; a signal named after it wakes it early.
- **Signals** — `WaitSignal` parks until `Client.Signal(id, name, payload)` delivers; the payload becomes the task's result.
- **Polling-wait** — a handler that finds its external condition not met returns `NotReady(d)` and re-checks after `d` **without consuming a retry** — the primitive for waiting on a provider with no callback.
- **Saga compensation** — a task may declare `Compensate(args)`; on failure the compensations of the succeeded tasks run in reverse completion order.

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/workflow"

	// Blank import registers the "postgres" DSN scheme.
	_ "github.com/kausys/azync/driver/azyncpgx"
)

type CreateAccount struct {
	Email string `json:"email"`
}

func (CreateAccount) Kind() string { return "onboard.create_account" }

// Account is CreateAccount's durable result, read downstream with ResultOf.
type Account struct {
	ID string `json:"id"`
}

type Activate struct{}

func (Activate) Kind() string { return "onboard.activate" }

func main() {
	ctx := context.Background()

	core, err := azync.Open("postgres://azync:azync@localhost:5432/azync?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer core.Close(ctx)

	if err := core.Migrate(ctx); err != nil {
		log.Fatal(err)
	}

	wf, err := workflow.New(core)
	if err != nil {
		log.Fatal(err)
	}

	// A handler returns (result, error); the result is persisted with the task.
	// A task with no output returns workflow.None.
	if err := workflow.Register(wf.Worker(), func(ctx context.Context, c CreateAccount) (Account, error) {
		return Account{ID: "acct_1"}, nil
	}); err != nil {
		log.Fatal(err)
	}
	if err := workflow.Register(wf.Worker(), func(ctx context.Context, _ Activate) (workflow.None, error) {
		acct, err := workflow.ResultOf[Account](ctx, "create")
		if err != nil {
			return workflow.None{}, err
		}
		log.Printf("activating %s (attempt %d)", acct.ID, workflow.Attempt(ctx))
		return workflow.None{}, nil
	}); err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := wf.Worker().Start(ctx); err != nil {
			log.Fatal(err)
		}
	}()
	<-wf.Worker().Ready()

	// Declare the DAG: create -> cooldown (durable timer) -> approved (signal) ->
	// activate. Run validates it (unique keys, existing deps, no cycles) and
	// inserts the whole graph atomically.
	def := workflow.Define("onboard").
		Task("create", CreateAccount{Email: "ada@example.com"}).
		Sleep("cooldown", 24*time.Hour, workflow.After("create")).
		WaitSignal("approved", workflow.After("cooldown")).
		Task("activate", Activate{}, workflow.After("approved"))

	res, err := wf.Client().Run(ctx, def)
	if err != nil {
		log.Fatal(err)
	}

	// Later — from a webhook, a timer, an operator: a signal named after the
	// Sleep wakes it early; one named after the WaitSignal delivers a payload.
	// Signal returns an error wrapping ErrNoSignalMatched when nothing is waiting.
	_ = wf.Client().Signal(ctx, res.ID, "cooldown", nil)
	_ = wf.Client().Signal(ctx, res.ID, "approved", map[string]string{"by": "ops"})

	select {} // a real service blocks on its signal context instead
}
```

`workflow.Open(dsn, ...)` builds a runtime that owns a private `Core`, mirroring `queue.Open` / `event.Open`.

### Failure policy

Each workflow declares at `Define` time how a dead task (aborted, or out of retries) is handled:

- **`Cancel`** (the default) cancels the remaining tasks, runs the compensation chain of the succeeded tasks that declared one, and settles the workflow `failed`.
- **`Suspend`** parks the workflow as `suspended` and leaves its tasks untouched, so an operator decides through the `Manager`.

```go
def := workflow.Define("payment", workflow.OnFailure(workflow.Suspend)).
	Task("charge", Charge{Amount: 42}, workflow.Compensate(Refund{Amount: 42})).
	Task("ship", Ship{}, workflow.After("charge"))
```

The `Manager` is the pure-library admin surface (no auth, no HTTP — embed it behind your own ops endpoints): `Get`, `Tasks` and `List` inspect; `Retry` resets dead tasks with a fresh budget and resumes (or re-enters compensation), `Compensate` triggers the saga manually, and `Cancel` stops a workflow without compensating.

### Barrier: idempotent fan-in across workflows

`Run` is safe to call from inside a task handler of another workflow. Combined with `WithIdempotencyKey` it is a fan-in barrier: any number of upstream workflows whose last task calls `Run` with the same `(name, key)` collectively start exactly one downstream workflow — the winning `Run` inserts, the rest return the winner's id with `Deduplicated == true`. The task's at-least-once re-execution is absorbed by the same key, so it replaces a distributed lock.

```go
// The last task of each upstream workflow, once its siblings are all done:
res, err := wf.Client().Run(ctx, businessDef, workflow.WithIdempotencyKey(kybID))
if err != nil {
	return workflow.None{}, err
}
// Exactly one caller sees res.Deduplicated == false; that run started the flow.
```

`TxRunner[pgx.Tx](wf)` creates a workflow inside your own transaction (the outbox pattern), exactly like `queue.TxProducer`.

### Retention

Terminal workflows (`succeeded`, `failed`, `cancelled`) are removed by the workflow vacuum after `WithWorkflowRetention` (default 30 days; `0` retains forever), together with their task jobs and dependency edges. A succeeded task's row and result live for **as long as its workflow does**, regardless of `WithCompletedRetention`: workflow task jobs are exempt from that sweep, so a task parked behind a long `Sleep` or `WaitSignal` never loses the result `ResultOf` depends on.

See `examples/workflow-basic` for a full onboarding-style flow: a typed chain, a `NotReady` provider poll, a signal delivered by a simulated webhook, a fan-out, and an idempotent barrier that launches a second workflow.

## Shared core

A single `Core` can back both runtimes at once, sharing one connection pool, one schema and one migrations table:

```go
core, err := azync.Open(dsn)
if err != nil {
	log.Fatal(err)
}
defer core.Close(ctx)

if err := core.Migrate(ctx); err != nil {
	log.Fatal(err)
}

q, err := queue.New(core)
if err != nil {
	log.Fatal(err)
}
ev, err := event.New(core)
if err != nil {
	log.Fatal(err)
}
```

This is the shape a real service usually wants: a projector subscribes to a domain event and enqueues a job in response (a webhook fan-out, a receipt email, a downstream sync), and both the event delivery and the job it triggers are ordinary rows in the same table, run by two workers over the same `Core`. See `examples/shared-core` for the full pattern, including running both `Worker.Start` loops concurrently and reading `Manager` stats from both runtimes.

Use `queue.Open(dsn, ...)` / `event.Open(dsn, ...)` instead of `New` when a runtime should own a private `Core` (its own pool, not shared).

## Transactional enqueue/publish

`TxProducer` / `TxPublisher` enlist an enqueue or publish in a transaction your own code already opened, so the outbox commits atomically with your own writes — a rollback means no job (or no event) was ever created:

```go
import "github.com/jackc/pgx/v5"

producer, err := queue.TxProducer[pgx.Tx](q) // q is *queue.Runtime
if err != nil {
	log.Fatal(err) // the driver does not support TxStore[pgx.Tx]
}

err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `insert into orders (id, total) values ($1, $2)`, orderID, total); err != nil {
		return err // rolled back: no order row, no job
	}
	_, err := producer.EnqueueTx(ctx, tx, SendReceipt{OrderID: orderID})
	return err // rolled back together with the insert above
})
```

The transaction handle type (`pgx.Tx` for `azyncpgx`) is a generic type parameter that appears **only** on `TxProducer[TTx]` / `TxPublisher[TTx]` — every other public API is untyped over the driver. For this to enlist correctly, `tx` must come from the same pool the driver's `Store` operates on: build the pool yourself, hand it to `azyncpgx.New(pool)`, and construct the `Core` with `azync.New(store)` instead of `azync.Open`.

The same pattern applies to events via `event.TxPublisher[TTx]` and `PublishTx`.

## Migrations

`Core.Migrate(ctx)` brings the backend schema up to date. **`Open` and `New` never migrate automatically** — call `Migrate` once, explicitly, from your own startup or deploy step.

`azyncpgx` tracks applied migrations in a version table, `azync_migrations` by default; override it with `azync.WithMigrationsTable("...")` (or `azyncpgx.WithMigrationsTable` if you build the `Store` directly). `azync.WithSchema("...")` isolates every azync table inside a named PostgreSQL schema — `Migrate` creates the schema if it does not exist yet.

Migrate creates:

- `azync_jobs` — the unified job table (queue jobs, event deliveries and workflow tasks, partitioned by `source`).
- `azync_events` — the append-only event ledger.
- `azync_subscribers` — durable subscriber registrations.
- `azync_job_attempts` — per-attempt failure history.
- `azync_idempotency_keys` — time-window dedupe reservations.
- `azync_stats_daily` — sharded daily throughput counters.
- `azync_workflows` — the workflow header (one row per execution: lifecycle state, failure policy, idempotency key).
- `azync_workflow_deps` — the static DAG edges and the compensation chain.
- the migrations version table itself.

**Down-migration limitation.** The workflow migration is safe to roll back only on a schema with no workflow data. Its down step narrows the `azync_jobs` `source` and `state` CHECK constraints back to the queue/event-only sets and drops the workflow tables (`ON DELETE CASCADE`); if any workflow row still exists — a job with `source = 'workflow'` or in a workflow-only state (`blocked`, `waiting`, `cancelled`), or any row in `azync_workflows` — re-adding the narrowed CHECK fails and the rollback aborts. Drain and vacuum workflows before rolling back.

## Architecture

Everything is a job. `azync_jobs` is one table with a `source` discriminator (`queue`, `event` and `workflow`); a queue `Worker`, an event `Worker` and a workflow `Worker` each operate one source in isolation, so a queue never leases an event delivery or a workflow task, and vice versa. Event bodies live separately, in the insert-only `azync_events` ledger: a delivery job carries no payload of its own, it is rehydrated by joining the ledger on dequeue, which is what makes `Manager.Replay` possible without the original `Publish` call.

A job moves through a small state machine:

```
scheduled --(run_at due)--> pending --(leased)--> active --(Ack)--> succeeded
    ^                          ^  ^                  |
    |                          |  +--(lease expired, reap budget left)--
    |                          |                     |
    |                          |                     +--(fail, budget left)--> scheduled
    |                          |                     +--(fail, exhausted / Abort)--> dead
    |                          |                     +--(lease expired, reap budget exhausted)--> dead
    |                          |
    +---------------- pending/scheduled <--(Manager.Resume)-- paused <--(Manager.Pause)--
                               |
                    dead --(Manager.Retry)--> pending
```

Other structural pieces:

- **Lease fencing.** A worker leases a job for a bounded TTL and gets a fresh lease token back; only the current token can `Ack`, `Reschedule`, `Dead`, `Release` or `ExtendLease` that job, so a worker that lost its lease (GC pause, network partition) cannot corrupt a job another worker now owns.
- **Reap count vs. retry budget.** An expired lease increments `ReapCount`, separate from the handler's own `Attempt` / `MaxAttempts` retry budget — a stuck worker cannot silently burn a job's retries just by holding it too long. `MaxReaps` bounds how many times a job survives being reclaimed before it is killed.
- **Sharded daily stats.** Throughput counters are written to `azync_stats_daily` across slots (0–7 in `azyncpgx`) so concurrent enqueues on a hot kind don't serialize on one counter row.
- **Retention.** `CompletedRetention` (default 7 days) bounds how long succeeded jobs are kept before a vacuum trims them; `StatsRetention` (default 35 days) bounds the daily counters. Either set to `0` means keep forever.
- **LISTEN/NOTIFY as acceleration, polling as correctness.** `azyncpgx` keeps one dedicated LISTEN connection to wake fetch loops instantly on `Enqueue`/`Publish`. It is an optimization, not a dependency: the polling loop underneath is what a fetch loop actually relies on for correctness, so a missed notification (a connection blip) is caught on the next poll, and `PollOnly()` (or a driver without `Notifier`) still works, just at polling latency.
- **Set-based workflow scheduler.** The DAG machinery is a handful of set-based, idempotent SQL passes (promote unblocked tasks, complete due timers, apply the failure policy, settle finished workflows) rather than a per-workflow actor. The header carries the lifecycle state — `running`, `suspended`, `compensating`, `succeeded`, `failed`, `cancelled` — and tasks carry `blocked`, `waiting` and `cancelled` on top of the shared job states. Because every pass is idempotent, any number of workflow `Worker`s run the scheduler on their own tick with no leader election.

## Configuration

Settings resolve in layers: a runtime-specific option (`queue.With*` / `event.With*`) overrides a `Core` option (`azync.With*`), which overrides the built-in default. `Core` options that configure infrastructure (`WithSchema`, `WithNotifyChannel`, `WithMigrationsTable`, `PollOnly`) only apply to `azync.Open` — they are rejected by `azync.New`, since the `Store` there is already constructed.

| Setting | Core option | Runtime override | Default |
|---|---|---|---|
| Lease TTL | `WithLeaseTTL` | `queue.WithLeaseTTL`, `event.WithLeaseTTL`, `workflow.WithLeaseTTL` | 30s |
| Default retry budget | `WithDefaultMaxAttempts` | `queue.WithDefaultMaxRetries`, `event.WithDefaultMaxAttempts`, `workflow.WithDefaultMaxRetries` | 25 |
| Shutdown drain | `WithShutdownDrain` | `queue.WithShutdownDrain`, `event.WithShutdownDrain`, `workflow.WithShutdownDrain` | 25s |
| Max concurrency (whole runtime) | `WithMaxConcurrency` | `queue.WithMaxConcurrency`, `event.WithMaxConcurrency`, `workflow.WithMaxConcurrency` | 64 |
| Default per-kind concurrency | `WithDefaultConcurrency` | `queue.WithDefaultConcurrency`, `event.WithDefaultConcurrency`, `workflow.WithDefaultConcurrency` | 4 |
| Fetch batch size | `WithFetchBatchSize` | `queue.WithFetchBatchSize`, `event.WithFetchBatchSize`, `workflow.WithFetchBatchSize` | 10 |
| Fetch poll interval | `WithFetchPollInterval` | `queue.WithFetchPollInterval`, `event.WithFetchPollInterval`, `workflow.WithFetchPollInterval` | 1s |
| Fetch cooldown | `WithFetchCooldown` | `queue.WithFetchCooldown`, `event.WithFetchCooldown`, `workflow.WithFetchCooldown` | 100ms |
| Idle backoff cap | `WithIdleBackoffMax` | `queue.WithIdleBackoffMax`, `event.WithIdleBackoffMax`, `workflow.WithIdleBackoffMax` | 2s |
| Max reaps before death | `WithMaxReaps` | `queue.WithMaxReaps`, `event.WithMaxReaps`, `workflow.WithMaxReaps` | 5 |
| Stats retention | `WithStatsRetention` | `queue.WithStatsRetention`, `event.WithStatsRetention`, `workflow.WithStatsRetention` | 35 days (0 = forever) |
| Completed job retention | `WithCompletedRetention` | `queue.WithCompletedRetention`, `event.WithCompletedRetention` | 7 days (0 = forever) |
| Logger | `WithLogger` | — | `slog.Default()` |
| Job wall-clock timeout (queue only) | — | `queue.WithDefaultJobTimeout` (runtime), `queue.WithJobTimeout` (per kind, via `Register`) | 5m |
| Handler wall-clock timeout (event only) | — | `event.WithHandlerTimeout` | 5m |
| Task wall-clock timeout (workflow only) | — | `workflow.WithDefaultTaskTimeout` (runtime), `workflow.WithTaskTimeout` (per kind, via `Register`) | 5m |
| Terminal workflow retention (workflow only) | — | `workflow.WithWorkflowRetention` | 30 days (0 = forever) |
| Cron enabled (queue only) | — | `queue.WithCron(bool)` | true |
| Cron leader-check tick (queue only) | — | `queue.WithCronTick` | 30s |
| Schema (infra, `Open` only) | `WithSchema` | — | backend default |
| Notify channel (infra, `Open` only) | `WithNotifyChannel` | — | `azync` / `azync_<schema>` |
| Migrations table (infra, `Open` only) | `WithMigrationsTable` | — | `azync_migrations` |
| Poll-only, no LISTEN/NOTIFY (infra, `Open` only) | `PollOnly()` | — | disabled (push enabled) |

`queue.Open` / `event.Open` / `workflow.Open` accept `WithCoreOptions(...)` to forward `azync.Option`s to the private `Core` they build internally; it is rejected by `New`, which composes over an already-built `Core`.

Workflow task jobs are exempt from `WithCompletedRetention` — which is why there is deliberately no `workflow.` variant of that option: a succeeded task's row lives until its workflow is vacuumed (`workflow.WithWorkflowRetention`), never trimmed by the completed-job sweep — see [Workflows › Retention](#retention).

### Bring your own logger

azync logs through the standard library's `*slog.Logger`, so any logging backend with a `slog.Handler` bridge plugs in without adapters. By default (no `WithLogger`) it logs through `slog.Default()`.

With [zap](https://github.com/uber-go/zap), via its official `zapslog` bridge:

```go
import (
	"log/slog"

	"go.uber.org/zap"
	"go.uber.org/zap/exp/zapslog"

	"github.com/kausys/azync"
)

zl, _ := zap.NewProduction()
defer zl.Sync()

core, err := azync.Open(dsn,
	azync.WithLogger(slog.New(zapslog.NewHandler(zl.Core()))))
```

To silence azync entirely:

```go
core, err := azync.Open(dsn,
	azync.WithLogger(slog.New(slog.DiscardHandler)))
```

zerolog (`slog-zerolog`), logrus (`slog-logrus`), and most other structured loggers ship equivalent `slog.Handler` bridges.

## Writing a driver

A driver is any type implementing `driver.Store` — a single interface over the unified job table: enqueue and publish, dequeue/lease, settlement (`Ack`, `Reschedule`, `Dead`, `Release`, `ExtendLease`), maintenance (promotion, reaping, vacuums) and the full admin surface for both queue jobs and event deliveries. Embed `driver.UnimplementedStore` in your driver so that a method added to `Store` in a future azync release does not break your build — the new method reports `driver.ErrNotSupported` until you implement it.

Optional capabilities are discovered by type assertion, so a driver opts into exactly what its backend supports: `driver.Notifier` for push wakeups (falling back to polling otherwise, which is always correct), `driver.LeaderElector` to enable `queue.RegisterCron`, `driver.Migrator` so `Core.Migrate` works, and `driver.TxStore[TTx]` — generic over the backend's transaction handle — to unlock `queue.TxProducer` / `event.TxPublisher`.

Validate a new driver against the public conformance suite instead of hand-writing behavioral tests: wire your backend into `drivertest.RunConformance(t, newStore)` from your own test, and it drives your `Store` through the same black-box coverage (lease fencing, idempotency, promotion, reaping, retention) the first-party PostgreSQL driver is held to, without reaching into your backend's internals.

## Roadmap

- **Workflow-as-code / replay** — an alternative to the declared DAG, where control flow is ordinary Go re-executed deterministically.
- **Scheduled workflows** — cron-triggered workflow runs, the way `queue.RegisterCron` schedules jobs.
- **Child workflows** — first-class parent/child links, beyond today's idempotent `Run`-from-a-handler barrier.
- **An admin UI** over the `Manager` surfaces.
- A **`database/sql`** driver, for backends `pgx` does not cover.
- More storage backends beyond PostgreSQL.

## License

[MIT](LICENSE)
