# azync examples

Runnable programs demonstrating the public API. This is a separate Go module (`examples/go.mod`) with a permanent `replace` back to the parent tree — it is never published as a dependency.

- **queue-basic** — open a `Core`, compose a queue runtime, register a typed handler, enqueue jobs (including a delayed one and an idempotent one), run the worker.
- **event-basic** — register two subscribers on one event type, publish an event with an aggregate and metadata, run the worker.
- **shared-core** — one `Core` powering queue, event, dag, and workflow at once: a projector (event → queue), plus a one-task DAG and a one-Operation workflow to prove coexistence.
- **dag-basic** — a durable DAG shaped like a real onboarding saga: a typed chain whose outputs flow through `ResultOf`, a `NotReady` provider poll, a signal delivered by a simulated webhook, a fan-out, and an idempotent barrier that launches a second DAG. Runs the flow to completion and exits.
- **workflow-kyc** — a workflow-as-code (`package workflow`) KYC onboarding flow: an `Operation` polls a verification provider, a durable `Timer` paces the polling, and a `Select` races a human approval `Signal` against a review deadline `Timer`. Vendor-neutral — no dependency on any specific verification provider's SDK. Runs one workflow to completion and exits.
- **watch-sse** — bridge the `watch` change-hint stream to Server-Sent Events with plain `net/http`: a queue worker generates job traffic, `/stream` fans the hints out, and a dependency-free page renders them live (open http://localhost:8091). Demonstrates the reset-means-refetch consumer rule.

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
go run ./dag-basic
go run ./workflow-kyc
go run ./watch-sse
```

Each program migrates the schema on startup. `queue-basic`, `event-basic`, `shared-core` and `watch-sse` run until interrupted (Ctrl-C); `dag-basic` and `workflow-kyc` each drive one execution to completion and exit.

By default every example connects to `postgres://azync:azync@localhost:5433/azync?sslmode=disable` (the compose default). Point at a different database with `DATABASE_URL`:

```sh
DATABASE_URL="postgres://user:pass@host:5432/db?sslmode=disable" go run ./queue-basic
```
