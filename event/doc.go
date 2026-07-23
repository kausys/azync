// Package event is a durable CQRS event bus over an azync Core.
//
// Publish atomically appends an event to the ledger and fans out one delivery
// job per subscriber registered for that event type at that moment (the
// matching snapshot is taken inside the backend's Publish, in one transaction;
// callers must not pre-select subscribers). Deliveries are ordinary jobs of the
// event source: a Worker leases, executes and settles them on the shared
// engine, one job kind per subscriber. Delivery is at-least-once and
// deliberately unordered, so handlers must deduplicate their effects by
// (event id, subscriber). Aggregate id and version travel in the envelope for
// consumers that need to reject stale work.
//
// A subscriber is registered on the Worker with Register (an implementer of the
// Subscriber interface plus one or more typed On bindings) or the RegisterFunc
// shorthand. Handlers receive the decoded domain event directly; all delivery
// metadata — the subscriber name, the attempt, the ledger identifiers and the
// publish-time annotations — travels on the context and is read through the
// package accessors (Attempt, IsRetry, EventID, Metadata, ...). Start upserts
// each subscriber's durable subscriptions before the engine runs, so a
// subscription is born on the first Start: events published earlier created no
// deliveries for it, and Manager.Replay is the way to feed a new subscriber
// historical events (flagged Replay, without overwriting history).
//
// The delivery error taxonomy is deliberately minimal: a plain handler error
// retries with the engine backoff, and Permanent dead-letters immediately —
// there is no RetryAfter or Reportable. A payload that fails to decode is
// treated as Permanent, since it will never decode on retry. A handler panic is
// recovered and settles as an ordinary failure, never crashing the worker
// process. Because the Worker registers one engine kind per subscriber, an event
// whose subscriber has no live registration is simply never leased; the
// "missing handler" case disappears by design.
//
// Compose a Runtime over a shared Core with New, or standalone with Open;
// neither migrates automatically (call Runtime.Migrate first). The Manager
// exposes the admin surface: stats, retry, replay, retention and the event and
// delivery listings.
package event
