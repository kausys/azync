# Changelog

## [0.0.8](https://github.com/kausys/azync/compare/v0.0.7...v0.0.8) (2026-07-31)


### Features

* export watch.ErrPollOnly for poll-only detection ([77765c7](https://github.com/kausys/azync/commit/77765c7adcc8c80495d6fadbdd548aaee8ec99ae))
* near-real-time change notifications (watch + driver.ChangeNotifier) ([3545c71](https://github.com/kausys/azync/commit/3545c71201c9ee0d24aa53bafa0bb08a4cff8b46))


### Bug Fixes

* deliver the opening change reset only once the stream is live ([3778760](https://github.com/kausys/azync/commit/377876036f46e92ddcf378217b4010001268ad1b))

## [0.0.7](https://github.com/kausys/azync/compare/v0.0.6...v0.0.7) (2026-07-28)


### ⚠ BREAKING CHANGES

* driver.DAGStore gains DAGDeps, DAGNameStateCounts and DAGTaskCounts (custom driver implementations must add them).

### Features

* enumerate DAG definitions with their run counts ([5ec244e](https://github.com/kausys/azync/commit/5ec244ef0b224e5aa6ba7fe27cae0c1df6147b61))
* expose the DAG graph, task timing and admin counts ([5f327d5](https://github.com/kausys/azync/commit/5f327d52f3d2777b10b4985f80024868729da79f))


### Miscellaneous

* pin this cycle to 0.0.7 ([297a908](https://github.com/kausys/azync/commit/297a90899c55d720d969cc6d67d70b33b56dcfa2))
* pin this cycle to 0.0.7 and document the footer that does it ([bdc17d9](https://github.com/kausys/azync/commit/bdc17d941d33584f61fb4bfcaa1fcd833589f035))

## [0.0.6](https://github.com/kausys/azync/compare/v0.0.5...v0.0.6) (2026-07-27)


### ⚠ BREAKING CHANGES

* driver.Store.Snooze returns (deadlined, error) and takes a deadline reason; DAGStore.Signal takes DAGSignalParams and returns (delivered, deduplicated, error); DAGStore gains DeliverBufferedSignals, PauseDAG and FindDAGByKey; driver.Store gains Skip and RunNow; driver.TaskResults returns map[string]TaskResult (carries Skipped); dag.ErrNoSignalMatched is removed (a buffered signal is not an error); JobState gains 'skipped' and DAGState gains 'paused'; RetryDAG also releases paused tasks and clears stamped snooze deadlines.

### Features

* bounded NotReady, durable DAG signals and operator verbs from the first production consumer ([8fa7382](https://github.com/kausys/azync/commit/8fa73827b6851fb2cdcc721836b29e944144a949))


### Bug Fixes

* harden 00009 against interrupted builds and pin rolling-deploy arbiter inference ([a369441](https://github.com/kausys/azync/commit/a369441d8dccca0255ec94d39dc9f731642fa9a1))

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
