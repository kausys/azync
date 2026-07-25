// Package engine is the shared fetch/execute/settle/maintenance machinery the
// queue and event runtimes are built on, neutral over driver.Source.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// Config assembles an Engine for one job source. The queue runtime builds one
// with Source queue and the event runtime one with Source event; the engine is
// identical for both.
type Config struct {
	// Store is the persistence driver the engine fetches from and settles into.
	Store driver.Store
	// Source is the job partition this engine operates; it never touches jobs of
	// another source.
	Source driver.Source
	// Logger is the structured logger. Nil means slog.Default().
	Logger *slog.Logger
	// Settings are the resolved runtime knobs.
	Settings Settings
	// Acker settles a successful job, receiving the handler's result. Nil
	// means the default: Store.Ack with the result discarded (queue and event
	// handlers produce none). The workflow runtime injects AckTaskResult here
	// so a task's output is persisted atomically with its completion. Like
	// every settlement it is fenced: a not-found error is swallowed as the
	// expected lost-lease race.
	Acker func(ctx context.Context, id, leaseToken uuid.UUID, result json.RawMessage) error
}

// Settings are the resolved runtime knobs an Engine runs with. The consuming
// runtime resolves them from the core defaults plus its own overrides.
type Settings struct {
	// LeaseTTL is how long a claim is held; it also paces lease renewal
	// (LeaseTTL/2) and the reaper (one sweep per LeaseTTL).
	LeaseTTL time.Duration
	// ShutdownDrain is how long Start waits for in-flight handlers after ctx
	// ends before cancelling them.
	ShutdownDrain time.Duration
	// MaxConcurrency caps concurrent handlers across every kind.
	MaxConcurrency int
	// FetchBatchSize caps how many jobs one dequeue leases.
	FetchBatchSize int
	// FetchPollInterval is the minimum polling period while idle.
	FetchPollInterval time.Duration
	// FetchCooldown is the pause after a productive fetch, and the idle backoff
	// floor.
	FetchCooldown time.Duration
	// IdleBackoffMax caps the exponential idle backoff of a fetch loop.
	IdleBackoffMax time.Duration
	// MaxReaps is how many lease expirations a job survives before the reaper
	// kills it.
	MaxReaps int
	// StatsRetention bounds the daily stat counters; 0 retains forever.
	StatsRetention time.Duration
	// CompletedRetention bounds succeeded-job history; 0 retains forever.
	CompletedRetention time.Duration
	// PromoteInterval overrides the scheduled->pending promotion cadence.
	// Zero means the production default (1s).
	PromoteInterval time.Duration
	// VacuumInterval overrides the vacuum cadence. Zero means the production
	// default (1h).
	VacuumInterval time.Duration
}

// OutcomeKind is the classified fate of a failed handler.
type OutcomeKind int

const (
	// OutcomeRetry reschedules the job (the default for plain errors).
	OutcomeRetry OutcomeKind = iota
	// OutcomeAbort sends the job straight to the dead letter.
	OutcomeAbort
	// OutcomeSnooze parks the job as scheduled after Delay via Store.Snooze,
	// without consuming a retry attempt and regardless of the remaining
	// budget: the polling-wait primitive (the workflow runtime maps NotReady
	// to it).
	OutcomeSnooze
)

// Outcome is a consuming package's classification of a handler error. The
// engine turns it into a settlement: Snooze parks the job budget-free after
// Delay; Abort or an exhausted budget dead-letters the job; anything else
// reschedules it after Delay (or the exponential backoff when Delay is zero).
type Outcome struct {
	Kind OutcomeKind
	// Delay overrides the exponential backoff for a retry and is the snooze
	// duration for OutcomeSnooze; 0 means Backoff for a retry and an
	// immediate re-check for a snooze.
	Delay time.Duration
	// Reportable flags the error for loud logging when retries are exhausted.
	Reportable bool
}

// Classifier maps a handler error to its Outcome. Each consuming package
// injects its own (the queue's Abort/Retry/RetryAfter/Reportable taxonomy, the
// event bus's Permanent).
type Classifier func(error) Outcome

// Kind registers one job kind on the engine: its limits, its handler and its
// error classifier. Decoding and error taxonomy live with the consumer; the
// engine only sees driver.Job in and error out.
type Kind struct {
	// Name is the fetch partition (job kind, or subscriber name for events).
	Name string
	// Concurrency caps concurrent handlers of this kind.
	Concurrency int
	// Timeout bounds one handler execution; 0 means unlimited.
	Timeout time.Duration
	// MaxAttempts is the retry budget dequeues resolve durably on a job's first
	// lease when MaxAttemptsSet is true (see driver.DequeueParams).
	MaxAttempts int
	// MaxAttemptsSet marks MaxAttempts as an explicit per-kind override.
	MaxAttemptsSet bool
	// Handler runs one leased job. Its result travels to the engine's acker on
	// success; runtimes without task results (queue, event) return nil.
	Handler func(ctx context.Context, job driver.Job) (json.RawMessage, error)
	// Classify maps a handler error to its outcome. Nil means plain retry.
	Classify Classifier
}

// Engine is the shared fetch/execute/settle/maintenance machinery, neutral over
// driver.Source. A runtime constructs one, registers its kinds, and calls
// Start; the engine owns the slot-reservation invariant (executor capacity is
// reserved before any job is leased), lease renewal with fencing, settlement
// and the maintenance loops.
type Engine struct {
	store    driver.Store
	source   driver.Source
	logger   *slog.Logger
	settings Settings
	acker    func(ctx context.Context, id, leaseToken uuid.UUID, result json.RawMessage) error

	mu    sync.RWMutex
	kinds map[string]Kind

	started   atomic.Bool
	ready     chan struct{}
	readyOnce sync.Once

	semGlobal sem
	inflight  sync.WaitGroup
}

// New builds an Engine from cfg. Kinds are registered afterwards with Register,
// before Start.
func New(cfg Config) *Engine {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	acker := cfg.Acker
	if acker == nil {
		// Default success settlement: plain Ack, the handler result discarded.
		acker = func(ctx context.Context, id, leaseToken uuid.UUID, _ json.RawMessage) error {
			return cfg.Store.Ack(ctx, id, leaseToken)
		}
	}
	return &Engine{
		store:    cfg.Store,
		source:   cfg.Source,
		logger:   logger,
		settings: cfg.Settings,
		acker:    acker,
		kinds:    map[string]Kind{},
		ready:    make(chan struct{}),
	}
}

// Register adds one kind. It fails on a duplicate name and after Start.
func (e *Engine) Register(k Kind) error {
	if e.started.Load() {
		return errors.New("cannot register after start")
	}
	if k.Classify == nil {
		k.Classify = func(error) Outcome { return Outcome{Kind: OutcomeRetry} }
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.kinds[k.Name]; exists {
		return fmt.Errorf("kind %q already registered", k.Name)
	}
	e.kinds[k.Name] = k
	return nil
}

// Started reports whether Start has been called (and not failed during setup).
func (e *Engine) Started() bool { return e.started.Load() }

// Ready closes once wakeup setup succeeded and the loops are running. Poll-only
// engines become ready immediately after Start.
func (e *Engine) Ready() <-chan struct{} { return e.ready }

// Start runs the engine until ctx is cancelled: one fetch loop per registered
// kind (push wake + poll fallback) and the maintenance loop. On cancellation,
// in-flight handlers drain for up to Settings.ShutdownDrain, then are
// cancelled. Handlers run on a context derived from Background so they survive
// the shutdown of ctx during the drain window.
func (e *Engine) Start(ctx context.Context) error {
	if e.store == nil {
		return errors.New("engine has no store")
	}
	if !e.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}

	kinds := e.snapshotKinds()
	names := make([]string, 0, len(kinds))
	for _, k := range kinds {
		names = append(names, k.Name)
	}
	e.logger.Info("worker starting", "source", string(e.source), "kinds", names)

	e.semGlobal = make(sem, e.settings.MaxConcurrency)

	// jobsCtx outlives ctx by the drain budget so in-flight handlers finish.
	jobsCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()

	var wake <-chan driver.Wake
	if notifier, ok := e.store.(driver.Notifier); ok {
		ch, err := notifier.Wake(ctx)
		if err != nil {
			e.started.Store(false)
			return fmt.Errorf("wake: %w", err)
		}
		wake = ch
	}
	e.readyOnce.Do(func() { close(e.ready) })

	wakeChans := make(map[string]chan struct{}, len(names))
	for _, name := range names {
		wakeChans[name] = make(chan struct{}, 1)
	}
	if wake != nil {
		go func() {
			for w := range wake {
				if w.Source != e.source {
					continue
				}
				if ch, ok := wakeChans[w.Kind]; ok {
					select {
					case ch <- struct{}{}:
					default:
					}
				}
			}
		}()
	}

	var loops sync.WaitGroup
	for _, k := range kinds {
		loops.Go(func() { e.fetchLoop(ctx, jobsCtx, k, wakeChans[k.Name]) })
	}
	loops.Go(func() { e.maintenanceLoop(ctx, names) })

	<-ctx.Done()
	// Wait for the loops FIRST: a fetch loop dispatches an already-leased batch
	// before it observes cancellation, so only after loops.Wait() is it
	// guaranteed no further inflight.Add can race the Wait below (and no leased
	// job is silently orphaned by shutdown).
	loops.Wait()
	e.logger.Info("worker draining", "source", string(e.source), "budget", e.settings.ShutdownDrain)

	drained := make(chan struct{})
	go func() { e.inflight.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(e.settings.ShutdownDrain):
		e.logger.Warn("drain budget exhausted, cancelling in-flight jobs")
		cancelJobs()
		e.inflight.Wait()
	}
	e.logger.Info("worker stopped", "source", string(e.source))
	return ctx.Err()
}

func (e *Engine) snapshotKinds() []Kind {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]Kind, 0, len(e.kinds))
	for _, k := range e.kinds {
		out = append(out, k)
	}
	return out
}

// sem is a weight-1 counting semaphore over a buffered channel — the same
// acquire/tryAcquire/release semantics as x/sync/semaphore.Weighted for
// weight-1 operations, without the extra dependency.
type sem chan struct{}

// acquire blocks for one slot; it reports false when ctx ended first.
func (s sem) acquire(ctx context.Context) bool {
	select {
	case s <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// tryAcquire takes one slot without blocking.
func (s sem) tryAcquire() bool {
	select {
	case s <- struct{}{}:
		return true
	default:
		return false
	}
}

// release frees one slot.
func (s sem) release() { <-s }
