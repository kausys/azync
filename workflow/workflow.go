package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
)

const (
	defaultLeaseTTL            = 30 * time.Second
	defaultFetchPollInterval   = 200 * time.Millisecond
	defaultMaxAttempts         = 5
	defaultOperationTimeout    = 5 * time.Minute
	defaultOperationRetryDelay = time.Second
	defaultRetention           = 30 * 24 * time.Hour
	defaultVacuumInterval      = time.Hour
	defaultReconcileInterval   = time.Minute
	defaultConcurrency         = 1
	defaultShutdownDrain       = 25 * time.Second
	// stalledAfterLeaseMultiple sizes ListStalledWorkflows' olderThan window
	// as a multiple of the lease TTL, so a task that is merely between
	// "scheduled" and "visible to this read" — or a single missed lease
	// renewal — is never mistaken for stalled.
	stalledAfterLeaseMultiple = 5
)

// finalDrainGrace bounds the hard-stop wait after ShutdownDrain expires and
// in-flight passes have been asked to stop via jobsCtx cancellation —
// mirroring internal/engine's identical safety net (same rationale: a
// handler ignoring cancellation must not hang Start forever). A var, not a
// const, purely so tests in this package can shrink it; there is no public
// knob for it.
var finalDrainGrace = 10 * time.Second

// WorkerMode selects which job kinds a Worker dequeues (spec §12).
type WorkerMode string

const (
	WorkerModeCombined      WorkerMode = "combined"
	WorkerModeWorkflowOnly  WorkerMode = "workflow-only"
	WorkerModeOperationOnly WorkerMode = "operation-only"
)

// Runtime is the workflow-as-code system over one azync Core: the Client,
// the Worker and the Manager, all operating the workflow job source only
// (Source SourceWorkflow). It requires a driver with the
// [driver.WorkflowStore] capability; New and Open fail with a clear error
// otherwise. Runtime is a sibling of dag.Runtime; see docs/workflow-v1-spec.md
// "Coexistence".
type Runtime struct {
	core      *azync.Core
	ownedCore bool
	store     driver.WorkflowStore

	client  *Client
	worker  *Worker
	manager *Manager
}

// New composes a workflow-as-code runtime over a shared Core. It fails when
// the Core's driver does not implement driver.WorkflowStore.
func New(core *azync.Core, opts ...Option) (*Runtime, error) {
	if core == nil {
		return nil, errors.New("workflow: New core is nil")
	}
	return newRuntime(core, opts, false)
}

// Open builds a standalone workflow-as-code runtime that owns a private
// Core opened from dsn (pass Core options through WithCoreOptions). Close
// closes the owned Core. Open never migrates; call Migrate before using a
// fresh schema.
func Open(dsn string, opts ...Option) (*Runtime, error) {
	probe, err := resolveConfig(opts, true)
	if err != nil {
		return nil, err
	}
	core, err := azync.Open(dsn, probe.coreOptions...)
	if err != nil {
		return nil, err
	}
	r, err := newRuntime(core, opts, true)
	if err != nil {
		_ = core.Close(context.Background())
		return nil, err
	}
	r.ownedCore = true
	return r, nil
}

func newRuntime(core *azync.Core, opts []Option, forOpen bool) (*Runtime, error) {
	cfg, err := resolveConfig(opts, forOpen)
	if err != nil {
		return nil, err
	}
	store, ok := core.Store().(driver.WorkflowStore)
	if !ok {
		return nil, fmt.Errorf("workflow: driver %T does not support workflow-as-code", core.Store())
	}

	r := &Runtime{core: core, store: store}
	r.client = &Client{store: store}
	r.worker = &Worker{
		store:      store,
		jobs:       core.Store(),
		logger:     core.Logger(),
		cfg:        cfg,
		workflows:  map[string]workflowFunc{},
		operations: map[string]operationFunc{},
		done:       make(chan struct{}),
	}
	r.manager = &Manager{store: store, jobs: core.Store()}
	return r, nil
}

// Client returns the workflow creation and signalling client.
func (r *Runtime) Client() *Client { return r.client }

// Worker returns the replay and Operation execution runtime.
func (r *Runtime) Worker() *Worker { return r.worker }

// Manager returns the workflow administration client.
func (r *Runtime) Manager() *Manager { return r.manager }

// Migrate brings the backend schema up to date (requires a driver.Migrator).
// Open and New never migrate automatically.
func (r *Runtime) Migrate(ctx context.Context) error { return r.core.Migrate(ctx) }

// Close releases the runtime's resources: the private Core when the runtime
// was built with Open, nothing when it composes over a shared Core. When the
// runtime owns its Core, Close first waits (bounded by ctx) for a running
// Worker to finish draining, so the store is not closed out from under
// in-flight settlements; on timeout it logs a warning and closes anyway
// rather than hanging indefinitely.
func (r *Runtime) Close(ctx context.Context) error {
	if r.ownedCore {
		if err := r.worker.Wait(ctx); err != nil {
			r.core.Logger().Warn("workflow: Close: worker did not finish draining before ctx ended, closing store anyway", "error", err)
		}
		return r.core.Close(ctx)
	}
	return nil
}

// config is the runtime's resolved settings, overridable per runtime by
// With* options (package option > default). It deliberately does not
// inherit azync.Defaults: the MVP worker's polling loop has few enough knobs
// that a single default set keeps this file simple.
type config struct {
	leaseTTL            time.Duration
	fetchPollInterval   time.Duration
	defaultMaxAttempts  int
	operationTimeout    time.Duration
	operationRetryDelay time.Duration
	workerMode          WorkerMode
	retention           time.Duration
	vacuumInterval      time.Duration
	reconcileInterval   time.Duration
	concurrency         int
	shutdownDrain       time.Duration

	coreOptions []azync.Option
}

// Option configures a workflow Runtime. Options compose; later options win.
type Option func(*config) error

func resolveConfig(opts []Option, forOpen bool) (config, error) {
	c := config{
		leaseTTL:            defaultLeaseTTL,
		fetchPollInterval:   defaultFetchPollInterval,
		defaultMaxAttempts:  defaultMaxAttempts,
		operationTimeout:    defaultOperationTimeout,
		operationRetryDelay: defaultOperationRetryDelay,
		workerMode:          WorkerModeCombined,
		retention:           defaultRetention,
		vacuumInterval:      defaultVacuumInterval,
		reconcileInterval:   defaultReconcileInterval,
		concurrency:         defaultConcurrency,
		shutdownDrain:       defaultShutdownDrain,
	}
	for _, opt := range opts {
		if err := opt(&c); err != nil {
			return config{}, err
		}
	}
	if !forOpen && len(c.coreOptions) > 0 {
		return config{}, errors.New(
			"workflow: WithCoreOptions applies only to Open; New composes over an already-built Core")
	}
	return c, nil
}

// WithLeaseTTL overrides how long the worker holds a workflow-task job's
// lease. Must be positive.
func WithLeaseTTL(d time.Duration) Option {
	return positiveDuration("WithLeaseTTL", d, func(c *config) { c.leaseTTL = d })
}

// WithFetchPollInterval overrides the worker's idle polling period. Must be
// positive.
func WithFetchPollInterval(d time.Duration) Option {
	return positiveDuration("WithFetchPollInterval", d, func(c *config) { c.fetchPollInterval = d })
}

// WithDefaultMaxRetries overrides the retry budget applied to workflow-task
// and Operation jobs. Must be positive.
func WithDefaultMaxRetries(n int) Option {
	return positiveInt("WithDefaultMaxRetries", n, func(c *config) { c.defaultMaxAttempts = n })
}

// WithConcurrency sets how many workflow-task/Operation-task passes this
// worker runs concurrently (default 1). Each pass still leases and processes
// one job at a time (ProcessNext's Limit is always 1); this controls how
// many such passes run in parallel across goroutines. Must be positive.
func WithConcurrency(n int) Option {
	return positiveInt("WithConcurrency", n, func(c *config) { c.concurrency = n })
}

// WithShutdownDrain overrides how long Start waits for in-flight
// workflow-task/Operation passes on shutdown before cancelling their
// context (default 25s, matching the core default). A pass past that budget
// is cancelled but still given a final bounded grace period to finish
// settling before Start gives up on it and returns anyway (see
// internal/engine's identical drain shape). Must be positive.
func WithShutdownDrain(d time.Duration) Option {
	return positiveDuration("WithShutdownDrain", d, func(c *config) { c.shutdownDrain = d })
}

// WithOperationTimeout sets StartToClose for Operation handlers. Zero disables
// the timeout (not recommended). Must be non-negative.
func WithOperationTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("workflow: WithOperationTimeout: duration must be non-negative, got %v", d)
		}
		c.operationTimeout = d
		return nil
	}
}

// WithOperationRetryDelay sets the backoff used when an Operation fails with
// attempts remaining (retry_wait). Must be positive.
func WithOperationRetryDelay(d time.Duration) Option {
	return positiveDuration("WithOperationRetryDelay", d, func(c *config) { c.operationRetryDelay = d })
}

// WithWorkerMode selects combined / workflow-only / operation-only dequeue
// (docs/workflow-v1-spec.md §12). Default is combined.
func WithWorkerMode(mode WorkerMode) Option {
	return func(c *config) error {
		switch mode {
		case WorkerModeCombined, WorkerModeWorkflowOnly, WorkerModeOperationOnly, "":
			if mode == "" {
				mode = WorkerModeCombined
			}
			c.workerMode = mode
			return nil
		default:
			return fmt.Errorf("workflow: WithWorkerMode: unknown mode %q", mode)
		}
	}
}

// WithRetention overrides how long terminal workflow executions (succeeded,
// failed or cancelled) are kept before the vacuum removes them together with
// their history, signals, timers and jobs (default 30 days). A negative value
// is rejected; zero means retain forever.
func WithRetention(d time.Duration) Option {
	return nonNegativeDuration("WithRetention", d, func(c *config) { c.retention = d })
}

// withVacuumInterval shrinks the vacuum cadence. Test seam only.
func withVacuumInterval(d time.Duration) Option {
	return positiveDuration("withVacuumInterval", d, func(c *config) { c.vacuumInterval = d })
}

// withReconcileInterval shrinks the stalled-workflow reconciler's sweep
// cadence. Test seam only.
func withReconcileInterval(d time.Duration) Option {
	return positiveDuration("withReconcileInterval", d, func(c *config) { c.reconcileInterval = d })
}

// WithCoreOptions forwards options to the Core that Open builds internally
// (schema, logger, notify channel, shared defaults...). Valid only with
// Open; New rejects it because the Core is already constructed.
func WithCoreOptions(opts ...azync.Option) Option {
	return func(c *config) error {
		c.coreOptions = append(c.coreOptions, opts...)
		return nil
	}
}

func positiveDuration(name string, d time.Duration, set func(*config)) Option {
	return func(c *config) error {
		if d <= 0 {
			return fmt.Errorf("workflow: %s: duration must be positive, got %v", name, d)
		}
		set(c)
		return nil
	}
}

func nonNegativeDuration(name string, d time.Duration, set func(*config)) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("workflow: %s: duration must be non-negative, got %v", name, d)
		}
		set(c)
		return nil
	}
}

func positiveInt(name string, n int, set func(*config)) Option {
	return func(c *config) error {
		if n <= 0 {
			return fmt.Errorf("workflow: %s: value must be positive, got %d", name, n)
		}
		set(c)
		return nil
	}
}
