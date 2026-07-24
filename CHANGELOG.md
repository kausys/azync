# Changelog

## [0.0.3](https://github.com/kausys/azync/compare/v0.0.2...v0.0.3) (2026-07-24)


### Features

* **azyncpgx:** implement the workflow store over postgres ([8188c63](https://github.com/kausys/azync/commit/8188c6327f922c3e3e82c0b8900264a6681167ad))
* **azyncpgx:** index terminal workflows for the retention vacuum ([6651a24](https://github.com/kausys/azync/commit/6651a242dcd136ccdc3e4e26d4120af775f0a1eb))
* **driver:** add workflow contract, snooze primitive and in-memory oracle ([7bcd7f9](https://github.com/kausys/azync/commit/7bcd7f9be83f7b80db17551b1fe021462dca6085))
* **engine:** deliver handler results to an injected acker and add snooze ([ece3df3](https://github.com/kausys/azync/commit/ece3df3b054f5af607998b8278000d89b26dc70d))
* **examples:** add workflow-basic braid-shape example ([f1608ec](https://github.com/kausys/azync/commit/f1608ecd5b95dbeca27cb864d0342972db548189))
* **workflow:** durable DAG workflow runtime over the workflow store ([fb7b38a](https://github.com/kausys/azync/commit/fb7b38a6f9fcc184470e7cbc16e536a4a862fe7d))


### Bug Fixes

* **azyncpgx:** serialize per-workflow compensation under concurrent ticks ([f975f79](https://github.com/kausys/azync/commit/f975f79739f6a5f017b392fb581fa9554e84cebe))
* **azyncpgx:** serialize workflow verbs against concurrent transitions ([ec0422c](https://github.com/kausys/azync/commit/ec0422c2a90e4999e3f5ac1ebbe3ade7ad9a8e36))
* **driver:** align blocked timers, empty policy and task ordering with the contract ([a05bf74](https://github.com/kausys/azync/commit/a05bf749acba4ca67898770b56eff951e3340204))
* **driver:** exempt workflow-owned jobs from completed vacuum ([19498e3](https://github.com/kausys/azync/commit/19498e3cf89bebf8e807f6c465364719cccbb9cc))
* **driver:** resolve ignore-dead-deps policy semantics and completion liveness ([e536782](https://github.com/kausys/azync/commit/e536782c871bb4e745ec4fe927cba69323c611a8))
* re-check dead-task tolerance in workflow completion ([02ea43c](https://github.com/kausys/azync/commit/02ea43cb2df0888a32a4ca669a2237835f8bcdf4))


### Miscellaneous

* prepare release 0.0.3 ([3165b21](https://github.com/kausys/azync/commit/3165b212efb42a97807e356df8b8b7ebf52c477d))

## 0.0.2 (2026-07-23)


### ⚠ BREAKING CHANGES

* handlers now receive the pure domain value; `event.Envelope`, `queue.Job[T]` and `queue.RawJob` are removed, and delivery/job metadata moved to the context accessors
* `event.Subscriber` is now the identity interface for worker registration; the durable catalog record was renamed to `event.Subscription`
* `event.Worker.Subscribe` was replaced by `Register` (interface + typed `On` bindings) and `RegisterFunc`

### Features

* typed event bindings: `event.On(fn)` infers the event type from the handler signature and decodes the payload before invoking it — no casts, no manual unmarshalling
* self-describing subscribers: `Worker.Register` upserts the durable subscription (subscriber × event types) idempotently on `Start`, so `Publisher.Register` is only needed for administrative registration
* context metadata accessors in both runtimes (`Attempt`, `IsRetry`, `Metadata`, `EventID`, `IsReplay`, `JobID`, `Kind`, ...) plus `DeliveryFromContext` / `JobFromContext` and `NewContext` helpers for testing handlers in isolation
* optional `MaxAttempts() int` on a subscriber overrides the runtime default for its durable subscriptions

### Documentation

* "Bring your own logger": wiring zap through its official `zapslog` bridge, silencing azync with `slog.DiscardHandler`, and pointers to zerolog/logrus bridges

## 0.0.1 (2026-07-23)


### Features

* durable background job queue: typed handlers, delayed and idempotent enqueue, and a retry taxonomy (Abort, Retry, RetryAfter, Reportable) on top of a deterministic exponential backoff
* durable CQRS event bus: atomic publish-and-fan-out to registered subscribers, backed by an insert-only ledger, with replay from history
* a single unified job table for queue jobs and event deliveries, sharing one schema, one migration and one admin surface
* PostgreSQL driver (`azyncpgx`) over pgx v5, with LISTEN/NOTIFY push wakeups, goose-based migrations and advisory-lock leader election for cron
* transactional enqueue and publish (`TxProducer`, `TxPublisher`) enlisting writes in the caller's own transaction for a true outbox
* a public driver conformance suite (`drivertest`) so third-party storage backends can validate themselves against the same contract the PostgreSQL driver is held to
* layered runtime configuration: shared `Core` defaults overridable per queue or event runtime, with lease fencing and a reaper proven by the conformance and integration suites
* admin surfaces (`Manager`) for both runtimes: inspection, retry, replay, pause/resume, purge, vacuum and stats
