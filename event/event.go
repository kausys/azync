package event

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
)

// Runtime is the event bus over one azync Core: the Publisher, the Worker and
// the Manager, all operating the event delivery job source only. Publishing
// appends to the durable ledger and fans out one delivery job per matching
// subscriber; consuming runs the shared engine with one job kind per subscriber.
type Runtime struct {
	core      *azync.Core
	ownedCore bool
	cfg       config

	publisher *Publisher
	worker    *Worker
	manager   *Manager
}

// New composes an event runtime over a shared Core. Settings start from the
// Core's defaults and event options override them per runtime.
func New(core *azync.Core, opts ...Option) (*Runtime, error) {
	if core == nil {
		return nil, errors.New("event: New core is nil")
	}
	return newRuntime(core, opts, false)
}

// Open builds a standalone event runtime that owns a private Core opened from
// dsn (pass Core options through WithCoreOptions). Close closes the owned Core.
// Open never migrates; call Migrate before using a fresh schema.
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

	eng := engine.New(engine.Config{
		Store:  core.Store(),
		Source: driver.SourceEvent,
		Logger: core.Logger(),
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

	r := &Runtime{core: core, cfg: cfg}
	r.publisher = &Publisher{store: core.Store(), defaultMaxAttempts: cfg.DefaultMaxAttempts}
	r.worker = &Worker{
		engine: eng,
		cfg:    cfg,
		store:  core.Store(),
		logger: core.Logger(),
		subs:   map[string]subRegistration{},
	}
	r.manager = &Manager{store: core.Store()}
	return r, nil
}

// Publisher returns the event append + subscriber registration client.
func (r *Runtime) Publisher() *Publisher { return r.publisher }

// Worker returns the delivery execution runtime.
func (r *Runtime) Worker() *Worker { return r.worker }

// Manager returns the event administration client.
func (r *Runtime) Manager() *Manager { return r.manager }

// Migrate brings the backend schema up to date (requires a driver.Migrator).
// Open and New never migrate automatically.
func (r *Runtime) Migrate(ctx context.Context) error { return r.core.Migrate(ctx) }

// Close releases the runtime's resources: the private Core when the runtime was
// built with Open, nothing when it composes over a shared Core.
func (r *Runtime) Close(ctx context.Context) error {
	if r.ownedCore {
		return r.core.Close(ctx)
	}
	return nil
}

// EventArgs is a JSON-serializable event whose EventType is wire-stable
// (decoupled from the Go type path), e.g. "orders.created.v1".
type EventArgs interface {
	EventType() string
}

// Subscription is a durable registration binding a named consumer to one event
// type with its own retry budget. A newly registered subscription receives
// future publishes only; use Manager.Replay for historical events. Registration
// is an upsert keyed by (Name, EventType).
//
// Worker.Register and RegisterFunc upsert their subscriptions automatically in
// Start; construct a Subscription by hand only for the administrative
// Publisher.Register path (migrations, or subscribers consumed by external
// processes).
type Subscription struct {
	Name        string
	EventType   string
	MaxAttempts int
}

// event-only default; the shared knobs default from the core (azync.Defaults).
const defaultHandlerTimeout = 5 * time.Minute

// config is the runtime's resolved settings: the core's shared defaults as the
// baseline, plus the event-only handler timeout, all overridable per runtime by
// With* options (package option > core option > default).
type config struct {
	azync.Defaults

	handlerTimeout time.Duration

	coreOptions []azync.Option

	// promoteInterval and vacuumInterval shrink the engine maintenance cadences
	// in tests; they are deliberately not exposed as public options.
	promoteInterval time.Duration
	vacuumInterval  time.Duration
}

// Option configures an event Runtime. Options compose; later options win.
type Option func(*config) error

// resolveConfig layers opts over the core defaults. forOpen permits
// WithCoreOptions, which only makes sense when the runtime owns its Core.
func resolveConfig(defaults azync.Defaults, opts []Option, forOpen bool) (config, error) {
	c := config{
		Defaults:       defaults,
		handlerTimeout: defaultHandlerTimeout,
	}
	for _, opt := range opts {
		if err := opt(&c); err != nil {
			return config{}, err
		}
	}
	if !forOpen && len(c.coreOptions) > 0 {
		return config{}, errors.New(
			"event: WithCoreOptions applies only to Open; New composes over an already-built Core")
	}
	return c, nil
}

// WithLeaseTTL overrides the shared lease duration for this runtime. Must be
// positive.
func WithLeaseTTL(d time.Duration) Option {
	return positiveDuration("WithLeaseTTL", d, func(c *config) { c.LeaseTTL = d })
}

// WithDefaultMaxAttempts overrides the retry budget applied to subscribers
// registered without their own MaxAttempts. Must be positive.
func WithDefaultMaxAttempts(n int) Option {
	return positiveInt("WithDefaultMaxAttempts", n, func(c *config) { c.DefaultMaxAttempts = n })
}

// WithShutdownDrain overrides how long Start waits for in-flight handlers on
// shutdown. Must be positive.
func WithShutdownDrain(d time.Duration) Option {
	return positiveDuration("WithShutdownDrain", d, func(c *config) { c.ShutdownDrain = d })
}

// WithMaxConcurrency overrides the total concurrent-handler cap across every
// subscriber. Must be positive.
func WithMaxConcurrency(n int) Option {
	return positiveInt("WithMaxConcurrency", n, func(c *config) { c.MaxConcurrency = n })
}

// WithDefaultConcurrency overrides the per-subscriber concurrency (each
// subscriber is one engine fetch partition). Must be positive.
func WithDefaultConcurrency(n int) Option {
	return positiveInt("WithDefaultConcurrency", n, func(c *config) { c.DefaultConcurrency = n })
}

// WithHandlerTimeout overrides the per-delivery wall clock applied to every
// handler (default 5m; analogous to the queue's job timeout). Must be positive.
func WithHandlerTimeout(d time.Duration) Option {
	return positiveDuration("WithHandlerTimeout", d, func(c *config) { c.handlerTimeout = d })
}

// WithMaxReaps overrides how many lease expirations a delivery survives before
// the reaper kills it. Must be positive.
func WithMaxReaps(n int) Option {
	return positiveInt("WithMaxReaps", n, func(c *config) { c.MaxReaps = n })
}

// WithFetchBatchSize overrides how many deliveries one dequeue leases. Must be
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

// WithIdleBackoffMax overrides the idle backoff cap of the fetch loops. Must be
// positive.
func WithIdleBackoffMax(d time.Duration) Option {
	return positiveDuration("WithIdleBackoffMax", d, func(c *config) { c.IdleBackoffMax = d })
}

// WithStatsRetention overrides how long daily stat counters are kept. A
// negative value is rejected; zero means retain forever.
func WithStatsRetention(d time.Duration) Option {
	return nonNegativeDuration("WithStatsRetention", d, func(c *config) { c.StatsRetention = d })
}

// WithCompletedRetention overrides how long succeeded deliveries are kept. A
// negative value is rejected; zero means retain forever.
func WithCompletedRetention(d time.Duration) Option {
	return nonNegativeDuration("WithCompletedRetention", d, func(c *config) { c.CompletedRetention = d })
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

// withMaintenanceIntervals shrinks the engine's promotion/vacuum cadences. Test
// seam only.
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
			return fmt.Errorf("event: %s: duration must be positive, got %v", name, d)
		}
		set(c)
		return nil
	}
}

func positiveInt(name string, n int, set func(*config)) Option {
	return func(c *config) error {
		if n <= 0 {
			return fmt.Errorf("event: %s: value must be positive, got %d", name, n)
		}
		set(c)
		return nil
	}
}

func nonNegativeDuration(name string, d time.Duration, set func(*config)) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("event: %s: duration must not be negative, got %v", name, d)
		}
		set(c)
		return nil
	}
}
