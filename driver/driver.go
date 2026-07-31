package driver

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// Config carries the infrastructure settings the core resolves from options and
// hands to an [Opener]. Every field has a documented zero value so a driver can
// apply sensible defaults.
type Config struct {
	// Schema is the backend namespace to isolate azync's tables in. Empty means
	// the backend default (for example the public schema in PostgreSQL).
	Schema string
	// NotifyChannel is the wakeup channel name a [Notifier] driver listens on.
	// Empty means the driver's default.
	NotifyChannel string
	// MigrationsTable is the version-tracking table a [Migrator] driver uses.
	// Empty means the driver's default (azync_migrations in the pg driver).
	MigrationsTable string
	// PollOnly disables push wakeups even on a Notifier-capable driver, forcing
	// the correctness path of polling.
	PollOnly bool
	// Logger is the structured logger the driver should use. Nil means
	// slog.Default().
	Logger *slog.Logger
}

// Opener constructs a [Store] from a DSN and resolved [Config]. Drivers register
// one under a scheme with the core's RegisterDriver, typically from an init in a
// blank-imported package. An Opener must redact credentials from any error it
// returns.
type Opener func(dsn string, cfg Config) (Store, error)

// Store is the mandatory, backend-agnostic persistence contract. All durations
// are resolved against the backend's own clock; all fencing is by lease token.
// A Store that implements nothing else is fully functional through polling.
//
// Implementations must be safe for concurrent use.
type Store interface {
	// Enqueue durably inserts one queue job (Source SourceQueue) and signals
	// workers after commit. It returns inserted=false, nil error when the job was
	// deduplicated by its idempotency key. run_at and the pending/scheduled split
	// resolve against the backend clock.
	Enqueue(ctx context.Context, p EnqueueParams) (inserted bool, err error)

	// Publish atomically appends one event to the ledger and fans out one pending
	// delivery job per currently registered matching subscriber, all in a single
	// transaction; callers must not pre-select subscribers. It returns the number
	// of deliveries created.
	Publish(ctx context.Context, p PublishParams) (delivered int, err error)

	// RegisterSubscriber upserts a subscriber registration, keyed by
	// (Name, EventType); an existing registration's MaxAttempts is updated.
	RegisterSubscriber(ctx context.Context, sub Subscriber) error

	// Subscribers returns the registrations for an event type, ordered by name.
	Subscribers(ctx context.Context, eventType string) ([]Subscriber, error)

	// DeleteSubscriber removes the (name, eventType) registration. An empty
	// eventType removes every registration of name. It returns the number of
	// registrations removed; existing delivery jobs already fanned out are
	// untouched (see Manager.DrainSubscriber to also clear those).
	DeleteSubscriber(ctx context.Context, name, eventType string) (int64, error)

	// DequeueBatch leases up to p.Limit due pending jobs of (source, p.Kind) for
	// p.Lease, ordered by run_at then id. Each leased job's attempt is
	// incremented, a fresh lease token minted, and its retry budget resolved
	// durably (see DequeueParams.DefaultMaxAttempts). For Source SourceEvent the
	// returned jobs have their Event rehydrated from the ledger.
	DequeueBatch(ctx context.Context, source Source, p DequeueParams) ([]Job, error)

	// Ack completes an active job, retaining it as StateSucceeded (history), not
	// deleting it. Completion clears the lease and frees the live-job
	// idempotency key (the unique-key check excludes succeeded/dead); a
	// TTL-window key set via EnqueueParams.IdempotencyTTL deliberately survives
	// finalization until it expires. It returns a not-found error (see
	// IsNotFound) when the lease token no longer owns an active row.
	Ack(ctx context.Context, id, leaseToken uuid.UUID) error

	// Reschedule parks a failed active job as StateScheduled with run_at
	// now()+delay and records the failed attempt. Fenced by lease token.
	Reschedule(ctx context.Context, id, leaseToken uuid.UUID, delay time.Duration, lastError string) error

	// Dead moves a failed active job to StateDead (abort or exhausted budget) and
	// records the final attempt. Fenced by lease token.
	Dead(ctx context.Context, id, leaseToken uuid.UUID, lastError string) error

	// Skip settles an active job as StateSkipped: terminal, no result, the
	// reason retained as its last error for the ops surfaces, counted as
	// processed in the stats. A skipped DAG task satisfies its dependents
	// like a succeeded one and never compensates. Fenced by lease token.
	Skip(ctx context.Context, id, leaseToken uuid.UUID, reason string) error

	// Release returns a leased job to StatePending as a safety net, decrementing
	// attempt by one (floored at zero) without recording an attempt. Fenced by
	// lease token.
	Release(ctx context.Context, id, leaseToken uuid.UUID) error

	// Snooze parks an active job as StateScheduled with run_at now()+delay,
	// decrementing attempt by one (floored at zero) so the lease it hands back
	// never consumes the retry budget, and without recording an attempt. It is
	// the polling-wait primitive ("the resource is not ready, re-check in d"):
	// a handler without a deadline can snooze indefinitely without ever
	// exhausting its retries.
	//
	// A job carrying a snooze budget (Job.SnoozeBudget) is bounded: the FIRST
	// Snooze stamps its deadline (now()+budget on the backend clock — the
	// budget measures time spent waiting, not the job's age), and a Snooze
	// settled past that deadline dead-letters the job atomically instead,
	// recording the final attempt with deadlineError as its last error, and
	// returns deadlined=true. Fenced by lease token: it returns a not-found
	// error (see IsNotFound) when the token no longer owns an active row.
	Snooze(ctx context.Context, id, leaseToken uuid.UUID, delay time.Duration, deadlineError string) (deadlined bool, err error)

	// ExtendLease renews an active job's lease for the duration. Fenced by lease
	// token: it returns a not-found error once the token no longer owns the row.
	ExtendLease(ctx context.Context, id, leaseToken uuid.UUID, lease time.Duration) error

	// PromoteDue moves due scheduled jobs of the given kinds to pending and
	// returns the count promoted.
	PromoteDue(ctx context.Context, source Source, kinds []string) (int64, error)

	// ReapExpired reclaims active jobs of the given kinds whose lease expired:
	// each returns to pending with reap_count incremented, or moves to StateDead
	// (recording an attempt) once reap_count reaches maxReaps. It returns the
	// number reaped and the subset killed.
	ReapExpired(ctx context.Context, source Source, kinds []string, maxReaps int) (reaped, killed int64, err error)

	// VacuumStats trims daily stat counters of the source older than retention
	// and returns the rows removed. A retention <= 0 retains all and removes
	// nothing.
	VacuumStats(ctx context.Context, source Source, retention time.Duration) (int64, error)

	// VacuumIdempotency trims expired time-window dedupe keys of the source and
	// returns the rows removed.
	VacuumIdempotency(ctx context.Context, source Source) (int64, error)

	// VacuumCompleted trims succeeded jobs of the source completed before
	// retention ago and returns the rows removed. A retention <= 0 retains
	// succeeded jobs forever and removes nothing.
	//
	// DAG-owned jobs (DAGID != zero) are exempt regardless of retention: a
	// task can be succeeded for the whole span of a long Sleep or WaitSignal
	// further down its DAG, and deleting it here would blind ResultOf and
	// CompleteDAGs while the DAG is still running. Their lifecycle belongs to
	// the DAG — they are removed only by VacuumDAGs' terminal cascade.
	//
	// Workflow-as-code jobs (RunID != zero / source=workflow) are likewise
	// exempt: they are removed only by WorkflowStore.VacuumWorkflows.
	VacuumCompleted(ctx context.Context, source Source, retention time.Duration) (int64, error)

	// ListKinds returns the distinct kinds of the source (from live jobs and stat
	// history), sorted.
	ListKinds(ctx context.Context, source Source) ([]string, error)

	// KindDepths returns per-kind instantaneous state counters of the source.
	KindDepths(ctx context.Context, source Source) (map[string]Depths, error)

	// Stats returns one kind's instantaneous depths and its daily throughput
	// window, oldest day first.
	Stats(ctx context.Context, source Source, kind string) (Depths, []DailyCount, error)

	// AllDaily returns the daily throughput window summed across every kind of
	// the source, oldest day first.
	AllDaily(ctx context.Context, source Source) ([]DailyCount, error)

	// ListJobs lists jobs of the source matching filter, paginated, and returns
	// the page and the total matching count. Ordering depends on
	// filter.State:
	//   - StatePending or StateDead: EnqueuedAt ascending, then ID
	//   - StateScheduled or StatePaused: RunAt ascending, then ID
	//   - StateActive: LeaseUntil ascending, then ID
	//   - StateSucceeded: CompletedAt descending, then ID
	//   - no state filter: EnqueuedAt descending, then ID (newest first, for
	//     admin browsing)
	ListJobs(ctx context.Context, source Source, filter JobFilter, offset, limit int) ([]Job, int64, error)

	// GetJob returns a single job of the source by id, or a not-found error (see
	// IsNotFound) when none matches.
	GetJob(ctx context.Context, source Source, id uuid.UUID) (*Job, error)

	// JobAttempts returns a job's failure history, oldest attempt first.
	JobAttempts(ctx context.Context, source Source, id uuid.UUID) ([]AttemptError, error)

	// RetryJob resets a dead job of the source to pending for immediate retry
	// (attempt and reap_count cleared). It returns a not-found error when the job
	// is not dead.
	RetryJob(ctx context.Context, source Source, id uuid.UUID) error

	// RetryAllDead resets every dead job of (source, kind) to pending and returns
	// the count. An empty kind targets all kinds of the source.
	RetryAllDead(ctx context.Context, source Source, kind string) (int64, error)

	// RunNow expedites one scheduled job: run_at moves to now, the state to
	// pending, and workers are woken — the early-wake verb for a snoozed
	// poll (a webhook arrived, the operator will not wait out the re-check
	// delay). Only StateScheduled qualifies; any other state — already
	// runnable, leased or settled — is a not-found error (see IsNotFound).
	RunNow(ctx context.Context, source Source, id uuid.UUID) error

	// ArchiveJob force-fails a pending or scheduled job of the source to dead. It
	// returns a not-found error when the job is not in an archivable state.
	ArchiveJob(ctx context.Context, source Source, id uuid.UUID) error

	// PauseJob holds a pending or scheduled job of the source out of the ready
	// set (StatePaused). It returns a not-found error when the job is not
	// pausable.
	PauseJob(ctx context.Context, source Source, id uuid.UUID) error

	// ResumeJob returns a paused job of the source to pending or scheduled per its
	// run_at. It returns a not-found error when the job is not paused.
	ResumeJob(ctx context.Context, source Source, id uuid.UUID) error

	// DeleteJob deletes a job of the source in the given state. It returns a
	// not-found error when no such job exists.
	DeleteJob(ctx context.Context, source Source, id uuid.UUID, state JobState) error

	// DeleteAll deletes every job of (source, kind) in the given state and
	// returns the count. An empty kind targets all kinds of the source.
	DeleteAll(ctx context.Context, source Source, kind string, state JobState) (int64, error)

	// VacuumDead deletes dead jobs of (source, kind) enqueued before olderThan ago
	// and returns the count. An empty kind targets all kinds of the source.
	VacuumDead(ctx context.Context, source Source, kind string, olderThan time.Duration) (int64, error)

	// NukeAll deletes all jobs, stats and idempotency keys of the source (a dev
	// reset) and reports the counts. The event ledger is left intact.
	NukeAll(ctx context.Context, source Source) (NukeReport, error)

	// ListEvents lists ledger events matching filter, newest first, paginated,
	// returning the page and total matching count.
	ListEvents(ctx context.Context, filter EventFilter, offset, limit int) ([]EventAdminRow, int64, error)

	// GetEvent returns a single ledger event by id, or a not-found error when
	// none matches.
	GetEvent(ctx context.Context, id uuid.UUID) (*EventAdminRow, error)

	// ListSubscriberViews returns subscriber registrations, ordered by event type
	// then name. An empty eventType returns all.
	ListSubscriberViews(ctx context.Context, eventType string) ([]SubscriberView, error)

	// OpsStats returns the event ledger admin summary.
	OpsStats(ctx context.Context) (OpsStats, error)

	// Replay re-fans-out ledger events matching filter into fresh pending
	// deliveries flagged Replay, and returns the number created.
	Replay(ctx context.Context, filter ReplayFilter) (int64, error)

	// Retain deletes up to limit ledger events occurring before the cutoff whose
	// deliveries have all reached a terminal state (StateSucceeded or
	// StateDead), cascading to those terminal deliveries, and returns the
	// number of events removed. Events with any non-terminal delivery job
	// (pending, scheduled, active, or paused) are skipped.
	Retain(ctx context.Context, before time.Time, limit int) (int64, error)

	// Close releases the driver's resources. It is safe to call once; behavior of
	// a Store after Close is undefined.
	Close(ctx context.Context) error
}

// Notifier is the optional push-wakeup capability. Without it, runtimes fall
// back to polling, which is always correct.
type Notifier interface {
	// Wake returns a channel of wakeups signaled by enqueues and publishes. The
	// channel is closed when ctx ends. A nil channel with nil error means the
	// backend is poll-only.
	Wake(ctx context.Context) (<-chan Wake, error)
}

// Wake identifies the fetch loop to nudge: the (Source, Kind) partition that
// gained ready work.
type Wake struct {
	Source Source
	Kind   string
}

// ChangeNotifier is the optional row-change push capability: a stream of
// [Change] hints describing state transitions of jobs, DAGs and ledger
// events, for external observers (ops UIs, SSE bridges) that would otherwise
// poll. Hints are best-effort and at-most-once — they may be dropped under
// backpressure or lost across a backend reconnect, and every gap is announced
// in-band as a [ChangeReset] change. A consumer treats hints as invalidation
// signals and refetches authoritative state through the Manager surfaces; it
// must never treat the stream as a durable feed.
type ChangeNotifier interface {
	// Changes returns a stream of change hints. The channel is closed when ctx
	// ends or the store closes. The first delivery on every subscription is a
	// [ChangeReset]. A nil channel with nil error means the backend cannot
	// push changes (poll-only).
	Changes(ctx context.Context) (<-chan Change, error)
}

// ChangeEntity discriminates which entity a Change describes, or that the
// stream itself needs attention (ChangeReset).
type ChangeEntity string

const (
	// ChangeJob marks a change to a job row (any Source).
	ChangeJob ChangeEntity = "job"
	// ChangeDAG marks a change to a DAG header row.
	ChangeDAG ChangeEntity = "dag"
	// ChangeEvent marks an append to the event ledger.
	ChangeEvent ChangeEntity = "event"
	// ChangeReset signals a possible delivery gap: the stream just became
	// live, reconnected after a loss, or dropped hints under backpressure.
	// Consumers must refetch anything they care about. Every subscription
	// receives one ChangeReset before any other change.
	ChangeReset ChangeEntity = "reset"
)

// Change is a best-effort, at-most-once row-change hint. It carries only
// identifiers, kind/name, state and a timestamp — never payloads, results or
// metadata, which routinely carry PII; consumers read full rows through the
// Manager surfaces behind their own authorization.
type Change struct {
	// Entity says which table changed, or ChangeReset for a gap signal.
	Entity ChangeEntity
	// Source partitions job changes (queue|event|dag|workflow); empty for
	// dag-header and ledger-event changes.
	Source Source
	// ID is the changed job, DAG or event id; zero for bulk and reset hints.
	ID uuid.UUID
	// DAGID links a job change to its owning DAG when the row is a task.
	DAGID uuid.UUID
	// Kind is the job kind, the event type, or the DAG definition name.
	Kind string
	// TaskKey is set on job changes that belong to a DAG.
	TaskKey string
	// State is the row's new state. Job and DAG state vocabularies share the
	// field; ledger events carry none.
	State string
	// At is the backend transaction time of the change.
	At time.Time
	// Bulk marks a coalesced hint: one statement changed more rows than the
	// backend's per-statement cap, so per-row hints were replaced by this one
	// summary. ID/Kind/TaskKey/State are empty; refetch the whole partition
	// identified by Entity (and Source for jobs).
	Bulk bool
	// Count is the number of rows behind a Bulk hint.
	Count int
}

// LeaderElector is the optional cluster-wide leadership capability. Cron
// scheduling requires it; without it, cron is disabled while every other feature
// keeps working.
//
// LeaderElector alone cannot detect a lost leadership: the returned release
// func is fire-and-forget, so a caller has no way to notice that the backing
// session died and the lock was silently released server-side (a plain
// process crash, a network partition, or an idle-connection kill by an
// intermediary all do this). A driver that can distinguish "still holding
// the lock" from "lock is gone" should additionally implement LeaseElector;
// callers that need real fencing (cron does) prefer it when available and
// fall back to LeaderElector's latch-forever behavior otherwise.
type LeaderElector interface {
	// AcquireLeadership tries to take the named leadership. acquired=false means
	// another instance leads. When acquired, release relinquishes it.
	AcquireLeadership(ctx context.Context, name string) (release func(), acquired bool, err error)
}

// LeaseElector is the optional refinement of LeaderElector that exposes
// leadership as a checkable lease instead of a bare release func, so a caller
// can detect losing it (e.g. the backing session died) instead of believing
// it holds leadership forever after one successful acquire.
type LeaseElector interface {
	// AcquireLeadershipLease tries to take the named leadership. acquired=false
	// means another instance leads. When acquired, the returned lease must be
	// checked periodically with Valid and released with Release when done.
	AcquireLeadershipLease(ctx context.Context, name string) (lease LeadershipLease, acquired bool, err error)
}

// LeadershipLease is a held leadership that can be re-verified without
// re-acquiring it.
type LeadershipLease interface {
	// Valid reports whether this leadership is still held. A cheap,
	// synchronous check (e.g. pinging the session backing the lock) meant to
	// be called once per scheduler tick, not a full re-acquire attempt.
	Valid(ctx context.Context) bool
	// Release relinquishes the leadership. Idempotent.
	Release()
}

// Migrator is the optional schema-migration capability. Core.Migrate requires
// it; a driver without it reports ErrNotSupported.
type Migrator interface {
	// Migrate brings the backend schema up to date.
	Migrate(ctx context.Context) error
}

// TxStore is the optional transactional-enqueue capability, generic over the
// backend's transaction handle TTx (one concrete type per driver, e.g. pgx.Tx).
// It lets a caller enlist an enqueue or publish in an existing business
// transaction so the outbox commits atomically with the caller's writes.
type TxStore[TTx any] interface {
	// EnqueueTx performs Enqueue within the caller's transaction.
	EnqueueTx(ctx context.Context, tx TTx, p EnqueueParams) (inserted bool, err error)
	// PublishTx performs Publish within the caller's transaction.
	PublishTx(ctx context.Context, tx TTx, p PublishParams) (delivered int, err error)
}
