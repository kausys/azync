# Changelog

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
