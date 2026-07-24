# azync examples

Runnable programs demonstrating the public API. This is a separate Go module (`examples/go.mod`) with a permanent `replace` back to the parent tree — it is never published as a dependency.

- **queue-basic** — open a `Core`, compose a queue runtime, register a typed handler, enqueue jobs (including a delayed one and an idempotent one), run the worker.
- **event-basic** — register two subscribers on one event type, publish an event with an aggregate and metadata, run the worker.
- **shared-core** — one `Core` powering both a queue and an event bus at once, wired together by a projector: an event handler that enqueues a job in response.
- **workflow-basic** — a durable DAG shaped like a real onboarding saga: a typed chain whose outputs flow through `ResultOf`, a `NotReady` provider poll, a signal delivered by a simulated webhook, a fan-out, and an idempotent barrier that launches a second workflow. Runs the flow to completion and exits.

## Running

Start PostgreSQL (the repo's compose file):

```sh
./run.sh db-up   # from the repo root
```

Then, from this directory:

```sh
go run ./queue-basic
go run ./event-basic
go run ./shared-core
go run ./workflow-basic
```

Each program migrates the schema on startup. `queue-basic`, `event-basic` and `shared-core` run until interrupted (Ctrl-C); `workflow-basic` drives one workflow to completion and exits.

By default every example connects to `postgres://azync:azync@localhost:5432/azync?sslmode=disable` (the compose default). Point at a different database with `DATABASE_URL`:

```sh
DATABASE_URL="postgres://user:pass@host:5432/db?sslmode=disable" go run ./queue-basic
```
