package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
)

// workflow-only defaults; the shared knobs default from the core
// (azync.Defaults).
const (
	defaultTaskTimeout       = 5 * time.Minute
	defaultWorkflowRetention = 30 * 24 * time.Hour
	defaultSchedulerTick     = time.Second
	defaultWorkflowVacuum    = time.Hour
)

// Runtime is the workflow system over one azync Core: the Client, the Worker
// and the Manager, all operating the workflow job source only. It requires a
// driver with the [driver.WorkflowStore] capability; New and Open fail with a
// clear error otherwise.
type Runtime struct {
	core      *azync.Core
	ownedCore bool
	cfg       config
	store     driver.WorkflowStore

	client  *Client
	worker  *Worker
	manager *Manager
}

// New composes a workflow runtime over a shared Core. Settings start from the
// Core's defaults and workflow options override them per runtime. It fails
// when the Core's driver does not implement driver.WorkflowStore.
func New(core *azync.Core, opts ...Option) (*Runtime, error) {
	if core == nil {
		return nil, errors.New("workflow: New core is nil")
	}
	return newRuntime(core, opts, false)
}

// Open builds a standalone workflow runtime that owns a private Core opened
// from dsn (pass Core options through WithCoreOptions). Close closes the owned
// Core. Open never migrates; call Migrate before using a fresh schema.
func Open(dsn string, opts ...Option) (*Runtime, error) {
	// Validate options and harvest the core options before touching the DSN.
	probe, err := resolveConfig(azync.Defaults{}, opts, true)
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
	cfg, err := resolveConfig(core.Defaults(), opts, forOpen)
	if err != nil {
		return nil, err
	}
	store, ok := core.Store().(driver.WorkflowStore)
	if !ok {
		return nil, fmt.Errorf("workflow: driver %T does not support workflows", core.Store())
	}

	eng := engine.New(engine.Config{
		Store:  core.Store(),
		Source: driver.SourceWorkflow,
		Logger: core.Logger(),
		// A task's output is persisted atomically with its completion; the
		// engine swallows the fenced not-found race like every settlement.
		Acker: store.AckTaskResult,
		Settings: engine.Settings{
			LeaseTTL:           cfg.LeaseTTL,
			ShutdownDrain:      cfg.ShutdownDrain,
			MaxConcurrency:     cfg.MaxConcurrency,
			FetchBatchSize:     cfg.FetchBatchSize,
			FetchPollInterval:  cfg.FetchPollInterval,
			FetchCooldown:      cfg.FetchCooldown,
			IdleBackoffMax:     cfg.IdleBackoffMax,
			MaxReaps:           cfg.MaxReaps,
			StatsRetention:     cfg.StatsRetention,
			CompletedRetention: cfg.CompletedRetention,
			PromoteInterval:    cfg.promoteInterval,
			VacuumInterval:     cfg.vacuumInterval,
		},
	})

	r := &Runtime{core: core, cfg: cfg, store: store}
	r.client = &Client{store: store, defaultMaxAttempts: cfg.DefaultMaxAttempts}
	r.worker = &Worker{
		engine: eng,
		cfg:    cfg,
		store:  store,
		logger: core.Logger(),
	}
	r.manager = &Manager{store: store}
	return r, nil
}

// Client returns the workflow creation and signalling client.
func (r *Runtime) Client() *Client { return r.client }

// Worker returns the task execution and scheduling runtime.
func (r *Runtime) Worker() *Worker { return r.worker }

// Manager returns the workflow administration client.
func (r *Runtime) Manager() *Manager { return r.manager }

// Migrate brings the backend schema up to date (requires a driver.Migrator).
// Open and New never migrate automatically.
func (r *Runtime) Migrate(ctx context.Context) error { return r.core.Migrate(ctx) }

// Close releases the runtime's resources: the private Core when the runtime
// was built with Open, nothing when it composes over a shared Core.
func (r *Runtime) Close(ctx context.Context) error {
	if r.ownedCore {
		return r.core.Close(ctx)
	}
	return nil
}

// config is the runtime's resolved settings: the core's shared defaults as the
// baseline, plus workflow-only knobs, all overridable per runtime by With*
// options (package option > core option > default).
type config struct {
	azync.Defaults

	taskTimeout       time.Duration
	workflowRetention time.Duration

	coreOptions []azync.Option

	// schedulerTick and workflowVacuumInterval shrink the scheduler cadences in
	// tests; promoteInterval and vacuumInterval do the same for the engine
	// maintenance. They are deliberately not exposed as public options.
	schedulerTick          time.Duration
	workflowVacuumInterval time.Duration
	promoteInterval        time.Duration
	vacuumInterval         time.Duration
}

// Option configures a workflow Runtime. Options compose; later options win.
type Option func(*config) error

// resolveConfig layers opts over the core defaults. forOpen permits
// WithCoreOptions, which only makes sense when the runtime owns its Core.
func resolveConfig(defaults azync.Defaults, opts []Option, forOpen bool) (config, error) {
	c := config{
		Defaults:               defaults,
		taskTimeout:            defaultTaskTimeout,
		workflowRetention:      defaultWorkflowRetention,
		schedulerTick:          defaultSchedulerTick,
		workflowVacuumInterval: defaultWorkflowVacuum,
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

// WithLeaseTTL overrides the shared lease duration for this runtime. Must be
// positive.
func WithLeaseTTL(d time.Duration) Option {
	return positiveDuration("WithLeaseTTL", d, func(c *config) { c.LeaseTTL = d })
}

// WithDefaultMaxRetries overrides the retry budget applied to tasks declared
// without an explicit budget (see the MaxRetries task option and the
// WithMaxRetries register option). Must be positive.
func WithDefaultMaxRetries(n int) Option {
	return positiveInt("WithDefaultMaxRetries", n, func(c *config) { c.DefaultMaxAttempts = n })
}

// WithShutdownDrain overrides how long Start waits for in-flight tasks on
// shutdown. Must be positive.
func WithShutdownDrain(d time.Duration) Option {
	return positiveDuration("WithShutdownDrain", d, func(c *config) { c.ShutdownDrain = d })
}

// WithMaxConcurrency overrides the total concurrent-handler cap across every
// kind. Must be positive.
func WithMaxConcurrency(n int) Option {
	return positiveInt("WithMaxConcurrency", n, func(c *config) { c.MaxConcurrency = n })
}

// WithDefaultConcurrency overrides the per-kind concurrency used when a
// registration does not set its own. Must be positive.
func WithDefaultConcurrency(n int) Option {
	return positiveInt("WithDefaultConcurrency", n, func(c *config) { c.DefaultConcurrency = n })
}

// WithDefaultTaskTimeout overrides the default per-task wall clock applied to
// registrations that do not set their own (default 5m). Must be positive; a
// single kind can still opt out with the WithTaskTimeout(0) register option.
func WithDefaultTaskTimeout(d time.Duration) Option {
	return positiveDuration("WithDefaultTaskTimeout", d, func(c *config) { c.taskTimeout = d })
}

// WithMaxReaps overrides how many lease expirations a task survives before the
// reaper kills it. Must be positive.
func WithMaxReaps(n int) Option {
	return positiveInt("WithMaxReaps", n, func(c *config) { c.MaxReaps = n })
}

// WithFetchBatchSize overrides how many tasks one dequeue leases. Must be
// positive.
func WithFetchBatchSize(n int) Option {
	return positiveInt("WithFetchBatchSize", n, func(c *config) { c.FetchBatchSize = n })
}

// WithFetchPollInterval overrides the idle polling period. Must be positive.
func WithFetchPollInterval(d time.Duration) Option {
	return positiveDuration("WithFetchPollInterval", d, func(c *config) { c.FetchPollInterval = d })
}

// WithFetchCooldown overrides the pause after a productive fetch. Must be
// positive.
func WithFetchCooldown(d time.Duration) Option {
	return positiveDuration("WithFetchCooldown", d, func(c *config) { c.FetchCooldown = d })
}

// WithIdleBackoffMax overrides the idle backoff cap of the fetch loops. Must
// be positive.
func WithIdleBackoffMax(d time.Duration) Option {
	return positiveDuration("WithIdleBackoffMax", d, func(c *config) { c.IdleBackoffMax = d })
}

// WithStatsRetention overrides how long daily stat counters are kept. A
// negative value is rejected; zero means retain forever.
func WithStatsRetention(d time.Duration) Option {
	return nonNegativeDuration("WithStatsRetention", d, func(c *config) { c.StatsRetention = d })
}

// WithCompletedRetention overrides how long succeeded task jobs are kept. A
// negative value is rejected; zero means retain forever.
func WithCompletedRetention(d time.Duration) Option {
	return nonNegativeDuration("WithCompletedRetention", d, func(c *config) { c.CompletedRetention = d })
}

// WithWorkflowRetention overrides how long terminal workflows (succeeded,
// failed or cancelled) are kept before the vacuum removes them together with
// their task jobs and dependency edges (default 30 days). A negative value is
// rejected; zero means retain forever.
func WithWorkflowRetention(d time.Duration) Option {
	return nonNegativeDuration("WithWorkflowRetention", d, func(c *config) { c.workflowRetention = d })
}

// WithCoreOptions forwards options to the Core that Open builds internally
// (schema, logger, notify channel, shared defaults...). Valid only with Open;
// New rejects it because the Core is already constructed.
func WithCoreOptions(opts ...azync.Option) Option {
	return func(c *config) error {
		c.coreOptions = append(c.coreOptions, opts...)
		return nil
	}
}

// withSchedulerIntervals shrinks the scheduler tick and the workflow vacuum
// cadence. Test seam only.
func withSchedulerIntervals(tick, vacuum time.Duration) Option {
	return func(c *config) error {
		c.schedulerTick = tick
		c.workflowVacuumInterval = vacuum
		return nil
	}
}

// withMaintenanceIntervals shrinks the engine's promotion/vacuum cadences.
// Test seam only.
func withMaintenanceIntervals(promote, vacuum time.Duration) Option {
	return func(c *config) error {
		c.promoteInterval = promote
		c.vacuumInterval = vacuum
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

func positiveInt(name string, n int, set func(*config)) Option {
	return func(c *config) error {
		if n <= 0 {
			return fmt.Errorf("workflow: %s: value must be positive, got %d", name, n)
		}
		set(c)
		return nil
	}
}

func nonNegativeDuration(name string, d time.Duration, set func(*config)) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("workflow: %s: duration must not be negative, got %v", name, d)
		}
		set(c)
		return nil
	}
}
