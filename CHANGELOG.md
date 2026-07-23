# Changelog

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
