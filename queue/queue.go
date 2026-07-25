package queue

import (
	"context"
	"errors"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
)

// Runtime is the queue system over one azync Core: the Producer, the Worker
// and the Manager, all operating the queue job source only.
type Runtime struct {
	core      *azync.Core
	ownedCore bool
	cfg       config

	producer *Producer
	worker   *Worker
	manager  *Manager
}

// New composes a queue runtime over a shared Core. Settings start from the
// Core's defaults and queue options override them per runtime.
func New(core *azync.Core, opts ...Option) (*Runtime, error) {
	if core == nil {
		return nil, errors.New("queue: New core is nil")
	}
	return newRuntime(core, opts, false)
}

// Open builds a standalone queue runtime that owns a private Core opened from
// dsn (pass Core options through WithCoreOptions). Close closes the owned
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

	eng := engine.New(engine.Config{
		Store:  core.Store(),
		Source: driver.SourceQueue,
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
	r.producer = &Producer{store: core.Store(), defaultMaxAttempts: cfg.DefaultMaxAttempts}
	r.worker = &Worker{
		engine:   eng,
		cfg:      cfg,
		store:    core.Store(),
		logger:   core.Logger(),
		producer: r.producer,
		cron:     newCronRegistry(),
		now:      time.Now,
	}
	r.manager = &Manager{store: core.Store()}
	return r, nil
}

// Producer returns the enqueue client.
func (r *Runtime) Producer() *Producer { return r.producer }

// Worker returns the job execution runtime.
func (r *Runtime) Worker() *Worker { return r.worker }

// Manager returns the queue administration client.
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
