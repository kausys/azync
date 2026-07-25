# Changelog

## [0.0.1](https://github.com/kausys/azync/compare/v0.0.0...v0.0.1) (2026-07-25)

### Features

* Initial release of azync: durable jobs over one `azync_jobs` table and a pluggable `driver.Store`
* `queue` — typed background jobs, cron, transactional outbox
* `event` — CQRS ledger, fan-out deliveries, Replay
* `dag` — static declared DAG executions
* `workflow` — workflow-as-code with deterministic replay, leased Operations, retention vacuum
* First-party PostgreSQL driver `azyncpgx` (migrations, LISTEN/NOTIFY, leader election)
