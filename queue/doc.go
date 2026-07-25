// Package queue provides durable background jobs over an azync Core.
//
// Jobs are persisted rows a Producer enqueues and a Worker leases, executes
// and settles. A job moves through the states pending -> active ->
// succeeded, with scheduled (a future run_at or a retry backoff), paused (an
// operator hold) and dead (aborted or out of retries) alongside; succeeded
// rows are retained as history until the completed-retention vacuum trims
// them.
//
// Delivery is at-least-once: a worker leases a job for a bounded TTL, renews
// the lease at half-life while the handler runs, and settles with the lease
// token as a fencing credential — a worker that lost its lease cannot settle
// a job now owned by another. Expired leases are reclaimed by the reaper;
// poison jobs die after too many reaps. Failed handlers reschedule on a
// deterministic exponential backoff, or per their error's taxonomy (Abort,
// Retry, RetryAfter, Reportable). A handler panic is recovered and settles as
// an ordinary failure — retried, then dead-lettered — never crashing the
// worker process.
//
// Compose a Runtime over a shared Core with New, or standalone with Open.
// Register handlers with Register / RegisterKind before Worker.Start, and
// periodic jobs with RegisterCron (leader-elected, deduplicated per
// occurrence, no backfill). The Manager exposes the admin surface:
// inspection, retry, archive, pause/resume, purge and vacuums.
package queue
