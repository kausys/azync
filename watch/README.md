# watch (package)

Import: `github.com/kausys/azync/watch`

User guide: [../watch.md](../watch.md) · GoDoc: package docs via `go doc` / pkg.go.dev.

## Role

Change-hint observer over the Core: streams best-effort, at-most-once row-change hints (jobs, DAG headers, ledger events) to external consumers — ops UIs, SSE bridges — that would otherwise poll. It runs no jobs, so unlike the four job-running runtimes it does not embed `azync.Defaults`; its only knob is the per-subscription buffer.

## Source layout

| File / area | Responsibility |
|-------------|----------------|
| `watch.go` | `New` / `Open`, `Watcher.Watch`, `Filter`, options |
| `doc.go` | Package model: hints, resets, bulk |

## Driver surface

Requires the optional `driver.ChangeNotifier` capability (azyncpgx: migration 00011's triggers on `azync_jobs` / `azync_dags` / `azync_events` NOTIFYing the fixed `azync_changes` channel; a second, lazily-opened LISTEN connection).

## Public surface (summary)

- `New(core)` / `Open(dsn)` — capability-asserting composition
- `Watcher.Watch(ctx, Filter)` — filtered subscription; first delivery is always an `EntityReset`
- `Filter{Entities, Sources, Kinds, DAGID}` — resets always pass; bulk hints pass the kind/DAG bounds
- `Change` / `Entity` / `Source` — aliases of the driver types
- `WithBuffer`, `WithCoreOptions`

## Boundaries

- No import of `queue` / `event` / `dag` / `workflow` (or vice versa); composes only through the Core.
- Hints carry identifiers, kind/name, state and a timestamp — never payloads, results or meta (PII rule; full rows are read through the Managers behind the caller's authz).
- Not a durable feed: no replay, no cross-entity ordering; every possible gap is announced as an in-band reset.

## Tests

`go test ./watch/...` · ChangeNotifier conformance in `driver/drivertest`.
