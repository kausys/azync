# Changelog

## [0.0.5](https://github.com/kausys/azync/compare/v0.0.4...v0.0.5) (2026-07-26)


### Bug Fixes

* make ListStalledWorkflows' olderThan cutoff inclusive ([765ba0e](https://github.com/kausys/azync/commit/765ba0e4fe2d1420e87e702c01328a6b9e438eab))

## [0.0.4](https://github.com/kausys/azync/compare/v0.0.3...v0.0.4) (2026-07-26)


### ⚠ BREAKING CHANGES

* driver.Store gains DeleteSubscriber and driver.WorkflowStore gains ListStalledWorkflows (custom driver implementations must add them); StartWorkflow/SignalWorkflow now atomically record history and schedule the task; queue.Worker.Start returns an error when crons are registered without a leader elector; Engine/Worker Start return nil instead of ctx.Err() on graceful shutdown.

### Features

* production hardening across driver, engine and all four runtimes ([a382d06](https://github.com/kausys/azync/commit/a382d068d9dbceeeaeba021c333bb1972c70a011))

## [0.0.3](https://github.com/kausys/azync/compare/v0.0.2...v0.0.3) (2026-07-26)


### Bug Fixes

* move tenant and trace into metadata ([03bef2d](https://github.com/kausys/azync/commit/03bef2de57d8c60991aa77a3fef1cda311b43553))

## [0.0.2](https://github.com/kausys/azync/compare/v0.0.1...v0.0.2) (2026-07-25)


### Bug Fixes

* clear CI lint and examples tidy drift ([c5aa92d](https://github.com/kausys/azync/commit/c5aa92d2158a9d5aa3ca883948796271531d63f4))
* **examples:** satisfy gci and exhaustive in shared-core ([8c1fdb4](https://github.com/kausys/azync/commit/8c1fdb43ddd47eb69028031204ceff36a17aaf39))

## [0.0.1](https://github.com/kausys/azync/compare/v0.0.0...v0.0.1) (2026-07-25)

### Features

* Initial release of azync: durable jobs over one `azync_jobs` table and a pluggable `driver.Store`
* `queue` — typed background jobs, cron, transactional outbox
* `event` — CQRS ledger, fan-out deliveries, Replay
* `dag` — static declared DAG executions
* `workflow` — workflow-as-code with deterministic replay, leased Operations, retention vacuum
* First-party PostgreSQL driver `azyncpgx` (migrations, LISTEN/NOTIFY, leader election)
