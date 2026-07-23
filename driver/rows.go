// Package driver defines the backend-agnostic contract that azync storage
// drivers implement. It is the frozen public surface third-party drivers build
// against: a single [Store] interface over one unified job table plus optional
// capability interfaces discovered by type assertion.
//
// The contract carries no SQL, no locking primitives and no clock: durations
// are [time.Duration] values that each backend resolves against its own clock,
// and identifiers are opaque. A driver that implements only [Store] is fully
// functional through polling; the optional capabilities ([Notifier],
// [LeaderElector], [Migrator], [TxStore], [WorkflowStore], [TxWorkflowStore])
// unlock push wakeups, leader-elected cron, migrations, transactional
// enqueues and DAG workflows respectively.
package driver

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Source is the discriminator that partitions the unified job table. Every job
// belongs to exactly one source; runtimes operate one source in isolation
// (a queue never leases an event delivery and vice versa).
type Source string

const (
	// SourceQueue tags durable background jobs produced by Enqueue.
	SourceQueue Source = "queue"
	// SourceEvent tags event deliveries fanned out by Publish; one such job per
	// matching subscriber, rehydrated from the event ledger on dequeue.
	SourceEvent Source = "event"
	// SourceWorkflow tags DAG tasks created through the WorkflowStore
	// capability; they share the unified job table and settlement machinery.
	SourceWorkflow Source = "workflow"
)

// JobState is the persisted lifecycle state of a job.
type JobState string

const (
	// StatePending marks a job ready to be leased once its run_at is due.
	StatePending JobState = "pending"
	// StateScheduled marks a job whose run_at is in the future; PromoteDue moves
	// it to pending when due.
	StateScheduled JobState = "scheduled"
	// StateActive marks a job currently leased by a worker.
	StateActive JobState = "active"
	// StateDead marks a job that aborted or exhausted its retry budget; it is
	// retained for inspection and manual retry.
	StateDead JobState = "dead"
	// StatePaused marks a job an operator held out of the ready set.
	StatePaused JobState = "paused"
	// StateSucceeded is a terminal history state: completed jobs are retained
	// (not deleted) until VacuumCompleted trims them, so the ops UI can show a
	// success history.
	StateSucceeded JobState = "succeeded"
	// StateBlocked marks a workflow task whose dependencies are not all
	// satisfied yet; PromoteUnblocked releases it (SourceWorkflow only).
	StateBlocked JobState = "blocked"
	// StateWaiting marks a workflow signal task parked until Signal completes
	// it (SourceWorkflow only).
	StateWaiting JobState = "waiting"
	// StateCancelled is the terminal state of a workflow task cancelled by the
	// failure policy or an operator verb (SourceWorkflow only).
	StateCancelled JobState = "cancelled"
)

// EnqueueParams is the durable input for a single queue job (Source
// SourceQueue). Scheduling resolves against the backend clock: when RunAt is
// set it wins; otherwise the backend computes its own now()+Delay so a client
// clock skewed against the database can never hide a job from dequeue.
type EnqueueParams struct {
	// ID is the caller-assigned primary key; drivers must not overwrite it.
	ID uuid.UUID
	// Kind names the job type and selects the fetch partition.
	Kind string
	// Payload is the opaque handler argument, stored verbatim. Required for
	// queue jobs (never nil).
	Payload json.RawMessage
	// Meta carries string-valued annotations propagated to the handler.
	Meta map[string]string
	// TraceID, SpanID and TraceFlags carry an optional propagated trace. Flags
	// are meaningful only when TraceID is set (flags 0 with a trace id is a
	// valid unsampled trace).
	TraceID    string
	SpanID     string
	TraceFlags int16
	// RunAt is an absolute schedule (from At). Zero delegates to now()+Delay.
	RunAt time.Time
	// Delay is a relative schedule resolved against the backend clock; used only
	// when RunAt is zero.
	Delay time.Duration
	// MaxAttempts is the retry budget. It is durable only when
	// MaxAttemptsExplicit is true; otherwise the first lease may replace it with
	// the runtime default (see DequeueParams.DefaultMaxAttempts).
	MaxAttempts int
	// MaxAttemptsExplicit records that the caller set MaxAttempts deliberately,
	// pinning it against divergent runtime defaults on the first lease.
	MaxAttemptsExplicit bool
	// IdempotencyKey deduplicates within (Source, Kind). Empty disables dedupe.
	IdempotencyKey string
	// IdempotencyTTL extends the dedupe window: the live-job uniqueness check
	// (a duplicate is rejected only while a prior job with the key is still
	// alive) always applies. Setting IdempotencyTTL greater than zero ADDS a
	// fixed time-window reservation on top of that check, one that keeps the
	// key rejected for the given duration regardless of the prior job's state
	// (including after it settles to succeeded or dead). Zero relies on the
	// live-job check alone.
	IdempotencyTTL time.Duration
}

// PublishParams is the input for a single event appended to the ledger. Publish
// atomically writes this row and fans out one pending delivery job per matching
// subscriber.
type PublishParams struct {
	// ID is the caller-assigned ledger primary key.
	ID uuid.UUID
	// Type is the event type; subscribers registered for it receive a delivery.
	Type string
	// TenantID scopes the event to a tenant; uuid.Nil means global/unset.
	TenantID uuid.UUID
	// AggregateType and AggregateID identify the source aggregate, if any.
	AggregateType string
	AggregateID   string
	// Version is the aggregate version this event advances to.
	Version int64
	// OccurredAt is the domain time the event happened.
	OccurredAt time.Time
	// Payload is the opaque event body, stored verbatim.
	Payload json.RawMessage
	// Meta carries string-valued annotations.
	Meta map[string]string
	// TraceID, SpanID and TraceFlags carry an optional propagated trace.
	TraceID    string
	SpanID     string
	TraceFlags int16
}

// DequeueParams controls a single DequeueBatch claim within one (Source, Kind)
// partition.
type DequeueParams struct {
	// Kind selects the fetch partition; for event deliveries it is the
	// subscriber name.
	Kind string
	// Limit caps the number of jobs leased; a value <= 0 leases nothing.
	Limit int
	// Lease is how long the claim is held before the job becomes reclaimable by
	// ReapExpired.
	Lease time.Duration
	// DefaultMaxAttempts is the runtime's retry budget, applied durably on a
	// job's first lease unless the job was enqueued with an explicit budget
	// (EnqueueParams.MaxAttemptsExplicit) or OverrideDefault is false.
	DefaultMaxAttempts int
	// OverrideDefault enables applying DefaultMaxAttempts on the first lease. When
	// false the job's stored MaxAttempts is kept even on the first lease.
	OverrideDefault bool
}

// Job is the backend-neutral persisted representation of a job returned by
// dequeue and the admin surface.
type Job struct {
	// ID is the primary key.
	ID uuid.UUID
	// Source is the partition discriminator.
	Source Source
	// Kind is the job type (subscriber name for event deliveries).
	Kind string
	// State is the current lifecycle state.
	State JobState
	// Attempt is the 1-based count of leases so far (0 before the first lease).
	Attempt int
	// MaxAttempts is the resolved retry budget.
	MaxAttempts int
	// ReapCount is how many times the lease has expired and been reclaimed;
	// tracked separately from Attempt so a stuck worker cannot silently burn the
	// retry budget.
	ReapCount int
	// Payload is the opaque handler argument. Nil for event deliveries, whose
	// body lives in the ledger and is exposed through Event after dequeue.
	Payload json.RawMessage
	// Meta carries string-valued annotations. On reads it is never nil: a job
	// with no annotations returns an empty (non-nil) map.
	Meta map[string]string
	// TraceID, SpanID and TraceFlags carry the propagated trace, if any.
	TraceID    string
	SpanID     string
	TraceFlags int16
	// RunAt is when the job becomes (or became) due.
	RunAt time.Time
	// LeaseUntil is the current lease deadline while State is StateActive.
	LeaseUntil time.Time
	// LeaseToken fences settlement: only the holder of the current token may Ack,
	// Reschedule, Dead, Release or ExtendLease the job.
	LeaseToken uuid.UUID
	// LastError is the most recent failure message.
	LastError string
	// EventID links an event delivery to its ledger row; uuid.Nil for queue jobs.
	EventID uuid.UUID
	// Replay is true for deliveries created by Replay rather than the original
	// Publish fan-out.
	Replay bool
	// Event is the rehydrated ledger record, populated by DequeueBatch for
	// Source SourceEvent jobs and nil otherwise.
	Event *EventRecord
	// WorkflowID links a workflow task to its workflow header; uuid.Nil for
	// non-workflow jobs. The remaining workflow fields below are likewise zero
	// for every source but SourceWorkflow.
	WorkflowID uuid.UUID
	// TaskKey is the task's key within its workflow DAG ("comp:<key>" for a
	// compensation task).
	TaskKey string
	// Result is the task's persisted output (from AckTaskResult, or the signal
	// payload for a completed KindSignal task). Nil until the task succeeds
	// with a result.
	Result json.RawMessage
	// SignalName is the signal this task reacts to, if any.
	SignalName string
	// CompensationKind is the declared compensation kind, empty when the task
	// declared none.
	CompensationKind string
	// IgnoreDeadDeps marks the task promotable over dead or cancelled
	// dependencies.
	IgnoreDeadDeps bool
	// EnqueuedAt, FailedAt and CompletedAt are lifecycle timestamps; the latter
	// two are zero until the corresponding transition occurs.
	EnqueuedAt  time.Time
	FailedAt    time.Time
	CompletedAt time.Time
}

// EventRecord is a rehydrated row from the append-only event ledger. It is the
// single source of truth for an event's body, so replay reconstructs deliveries
// from it without the original publish call.
type EventRecord struct {
	ID            uuid.UUID
	Type          string
	TenantID      uuid.UUID
	AggregateType string
	AggregateID   string
	Version       int64
	OccurredAt    time.Time
	Payload       json.RawMessage
	// Meta carries string-valued annotations. On reads it is never nil: an event
	// with no annotations returns an empty (non-nil) map.
	Meta       map[string]string
	TraceID    string
	SpanID     string
	TraceFlags int16
}

// Subscriber is a registration binding a named consumer to an event type with
// its own retry budget. Registrations are unique per (Name, EventType).
type Subscriber struct {
	Name        string
	EventType   string
	MaxAttempts int
}

// Depths are the instantaneous per-state job counters of one kind.
type Depths struct {
	Pending   int64
	Scheduled int64
	Active    int64
	Dead      int64
	Paused    int64
	Succeeded int64
}

// DailyCount is one day of throughput counters for a kind, summed across the
// backend's internal shards.
type DailyCount struct {
	// Date is midnight (UTC) of the counted day.
	Date      time.Time
	Enqueued  int64
	Processed int64
	Failed    int64
	Reaped    int64
}

// AttemptError is one recorded failure in a job's retry history. Every failed
// transition (reschedule, exhaustion to dead, reap to dead) records one so the
// full "why did each attempt fail" trail survives, not just the last error.
type AttemptError struct {
	Attempt int
	Error   string
	At      time.Time
	Trace   string
}

// JobFilter selects jobs for the admin list. A zero field means "no bound":
// empty Kind lists across every kind of the source, and empty State lists every
// state.
type JobFilter struct {
	Kind  string
	State JobState
}

// EventFilter selects events for the ledger admin list. Zero values mean "no
// bound". Undispatched, when non-nil, keeps only events that have (false) or
// lack (true) any delivery.
type EventFilter struct {
	Type         string
	TenantID     uuid.UUID
	Undispatched *bool
	Since        time.Time
	Until        time.Time
}

// ReplayFilter selects ledger events to re-fan-out into fresh deliveries. Zero
// fields are unbounded; Limit <= 0 means the driver's own upper bound.
type ReplayFilter struct {
	Subscriber string
	EventType  string
	EventID    uuid.UUID
	Since      time.Time
	Until      time.Time
	Limit      int
}

// NukeReport summarizes a NukeAll dev reset for one source.
type NukeReport struct {
	Jobs  int64
	Stats int64
	Keys  int64
}

// OpsStats is the event ledger admin summary.
type OpsStats struct {
	// Undispatched is the number of events with zero deliveries.
	Undispatched int64
	// Total24h is the number of events in the last 24 hours.
	Total24h int64
	// Types24h is the number of distinct event types in the last 24 hours.
	Types24h int64
	// Subscribers is the current registration count.
	Subscribers int64
}

// EventAdminRow is one ledger projection for the admin list and detail views.
type EventAdminRow struct {
	ID            uuid.UUID
	Type          string
	TenantID      uuid.UUID
	AggregateType string
	AggregateID   string
	Version       int64
	OccurredAt    time.Time
	// DispatchedAt is zero when the event has no deliveries; otherwise it equals
	// OccurredAt, since Publish creates deliveries atomically with the event.
	DispatchedAt time.Time
	TraceID      string
	SpanID       string
	TraceFlags   int16
	Meta         map[string]string
	Payload      json.RawMessage
	// Deliveries is the number of delivery jobs fanned out from this event.
	Deliveries int64
}

// SubscriberView is one subscriber registration projected for the admin surface.
type SubscriberView struct {
	EventType   string
	Subscriber  string
	MaxAttempts int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
