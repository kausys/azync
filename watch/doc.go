// Package watch streams row-change hints from the backend so external
// observers — ops UIs, SSE/WebSocket bridges, cache invalidators — can react
// to state transitions without polling.
//
// # Model
//
// The stream carries hints, not data: each [Change] names the entity that
// changed (a job, a DAG header, a ledger event), its identifiers and its new
// state — never payloads, results or metadata, which routinely carry PII.
// Delivery is best-effort and at-most-once. Every gap a subscription can
// suffer — its own startup, a backend reconnect, a full buffer — is announced
// in-band as an [EntityReset] change, and the first delivery on every
// subscription is one. That yields a single consumer rule:
//
//	On a reset — and you always receive one first — refetch everything you
//	care about through the Manager surfaces.
//
// A statement that changes more rows than the backend's per-statement cap is
// coalesced into one Bulk hint per partition; treat it as "refetch this
// partition". The stream is not a durable feed: there is no replay, no
// ordering guarantee across entities, and the authoritative state always
// lives behind the Managers.
//
// # Filtering
//
// [Watcher.Watch] takes a [Filter] bounding entities, job sources, kinds and
// a single DAG. Resets always pass a filter; bulk hints pass the kind and
// DAG bounds (they carry neither).
//
// Compose a Watcher over a shared Core with New, or standalone with Open;
// the driver must implement the change-notification capability
// (driver.ChangeNotifier).
package watch
