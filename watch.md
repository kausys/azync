# Using watch

Near-real-time **change hints** over the same [`azync.Core`](README.md) as the other runtimes: every job insert and state transition, every DAG header transition, and every event-ledger append announces itself, so an ops UI updates without polling and without reloading.

Package notes: [`watch/README.md`](watch/README.md) · Example: [`examples/watch-sse`](https://github.com/kausys/azync/tree/main/examples/watch-sse)

Requires a driver that implements `driver.ChangeNotifier` (azyncpgx does, from migration 00011).

## Compose

```go
w, err := watch.New(core)
// or: watch.Open(dsn) for a private Core
```

## Subscribe

```go
ch, err := w.Watch(ctx, watch.Filter{
    Sources: []watch.Source{driver.SourceDAG}, // empty = everything
})
if err != nil { /* poll-only driver, or the driver lacks the capability */ }

for c := range ch {
    switch {
    case c.Entity == watch.EntityReset:
        refetchEverything() // the one rule — see below
    case c.Bulk:
        refetchPartition(c.Entity, c.Source) // a big statement; per-row hints were coalesced
    default:
        invalidate(c) // c.ID / c.DAGID / c.Kind / c.TaskKey / c.State
    }
}
```

## The contract: hints, not data

A `Change` carries **identifiers, kind/name, state and a timestamp — never payloads, results or meta** (they routinely hold PII; read full rows through the Managers behind your own authz). Delivery is **best-effort and at-most-once**: there is no replay and no cross-entity ordering. This is an invalidation stream, not an event log — the authoritative state always lives behind the Manager surfaces.

Every gap a subscription can suffer is announced **in-band** as an `EntityReset` change:

| Reset arrives when | Why |
|--------------------|-----|
| The subscription opens | LISTEN may not be established yet; "refetch first" is the honest opening |
| The backend connection re-establishes | Notifications during the outage are lost |
| The subscription's buffer overflowed | Dropped hints collapse into one reset instead of a silent gap |

That yields a single consumer rule: **on a reset — and you always receive one first — refetch everything you care about.**

A statement that changes more rows than the driver's per-statement cap (50 in azyncpgx) is coalesced into one `Bulk` hint per partition carrying `Count`; treat it as "refetch the partition".

## Filtering

`Filter` bounds what a subscription receives; a zero field means "no bound".

| Field | Admits |
|-------|--------|
| `Entities` | Only these entity kinds (`EntityJob`, `EntityDAG`, `EntityEvent`) |
| `Sources` | Only job changes of these runtimes; never excludes DAG-header or ledger changes |
| `Kinds` | Only these job kinds / event types / DAG definition names |
| `DAGID` | Only that DAG's header change and its task changes — the "run drawer" filter |

Resets always pass a filter. Bulk hints pass `Kinds` and `DAGID` (they carry neither) but still honor `Entities` and `Sources`.

## Bridging to a frontend (SSE)

`Watch` composes directly with an SSE handler — see [`examples/watch-sse`](https://github.com/kausys/azync/tree/main/examples/watch-sse) for the full program:

```go
func stream(w *watch.Watcher) http.HandlerFunc {
    return func(rw http.ResponseWriter, r *http.Request) {
        ch, err := w.Watch(r.Context(), watch.Filter{})
        if err != nil { http.Error(rw, "stream unavailable", http.StatusServiceUnavailable); return }
        rw.Header().Set("Content-Type", "text/event-stream")
        rw.Header().Set("Cache-Control", "no-cache")
        rw.Header().Set("X-Accel-Buffering", "no")
        fl := rw.(http.Flusher)
        for c := range ch {
            payload, _ := json.Marshal(c)
            fmt.Fprintf(rw, "event: change\ndata: %s\n\n", payload)
            fl.Flush()
        }
    }
}
```

The browser side maps hints to cache invalidations (e.g. TanStack Query's `invalidateQueries`) and treats a reset as invalidate-all. `EventSource` reconnects on its own, and the fresh subscription's opening reset makes the client refetch — exactly what a reconnect requires.

**Consume or disconnect.** Postgres queues notifications for every listening session in one shared (8GB) queue; a session that LISTENs on `azync_changes` and stops reading (a paused `psql`, a wedged pod) eventually stalls that queue for the whole database. The azyncpgx listener always consumes promptly — but any raw LISTEN you open yourself must too.

## Ops knobs

- `watch.WithBuffer(n)` — per-subscription buffer (default 256) before hints collapse into a reset.
- The change stream rides its own dedicated LISTEN connection, opened lazily on the first `Watch` — workers that never watch never open it.
- `azync.PollOnly()` disables push entirely; `Watch` then returns a clear error.

## See also

[queue.md](queue.md) · [event.md](event.md) · [dag.md](dag.md) · [workflow.md](workflow.md) · [README](README.md)
