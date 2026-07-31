package watch

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// defaultBuffer is the per-subscription delivery buffer. Watch runs no jobs,
// so the shared job knobs (azync.Defaults) do not apply to this package; the
// buffer is its only tunable.
const defaultBuffer = 256

// Change is one best-effort, at-most-once row-change hint. See
// [driver.Change] for the field contract; the one rule that matters to a
// consumer is: on an [EntityReset] change, refetch everything you care about
// through the Manager surfaces — the stream never replays what a gap lost.
type Change = driver.Change

// Entity discriminates what a Change describes.
type Entity = driver.ChangeEntity

// Source partitions job changes by their runtime.
type Source = driver.Source

// Re-exported entity constants, so callers filter without importing driver.
const (
	// EntityJob marks a change to a job row (any Source).
	EntityJob = driver.ChangeJob
	// EntityDAG marks a change to a DAG header row.
	EntityDAG = driver.ChangeDAG
	// EntityEvent marks an append to the event ledger.
	EntityEvent = driver.ChangeEvent
	// EntityReset signals a possible delivery gap: refetch. Every
	// subscription receives one reset before any other change.
	EntityReset = driver.ChangeReset
)

// Watcher is the change-hint subscription surface over one azync Core. It is
// an observer: it runs no jobs and owns no worker — it only fans the driver's
// [driver.ChangeNotifier] stream out to filtered subscriptions. Pure library,
// no auth: gate it behind your own authorization like the Managers.
type Watcher struct {
	core      *azync.Core
	ownedCore bool
	cfg       config
	notifier  driver.ChangeNotifier
}

// New composes a Watcher over a shared Core. It fails when the Core's driver
// does not implement [driver.ChangeNotifier].
func New(core *azync.Core, opts ...Option) (*Watcher, error) {
	if core == nil {
		return nil, errors.New("watch: New core is nil")
	}
	return newWatcher(core, opts, false)
}

// Open builds a standalone Watcher that owns a private Core opened from dsn
// (pass Core options through WithCoreOptions). Close closes the owned Core.
// Open never migrates; call Migrate before using a fresh schema.
func Open(dsn string, opts ...Option) (*Watcher, error) {
	// Validate options and harvest the core options before touching the DSN.
	probe, err := resolveConfig(opts, true)
	if err != nil {
		return nil, err
	}
	core, err := azync.Open(dsn, probe.coreOptions...)
	if err != nil {
		return nil, err
	}
	w, err := newWatcher(core, opts, true)
	if err != nil {
		_ = core.Close(context.Background())
		return nil, err
	}
	w.ownedCore = true
	return w, nil
}

func newWatcher(core *azync.Core, opts []Option, forOpen bool) (*Watcher, error) {
	cfg, err := resolveConfig(opts, forOpen)
	if err != nil {
		return nil, err
	}
	notifier, ok := core.Store().(driver.ChangeNotifier)
	if !ok {
		return nil, fmt.Errorf("watch: driver %T does not support change notifications", core.Store())
	}
	return &Watcher{core: core, cfg: cfg, notifier: notifier}, nil
}

// Filter selects the changes a Watch subscription receives. A zero field
// means "no bound". Two kinds of change bypass parts of a filter by design:
// reset changes always pass (a gap concerns every consumer), and bulk
// changes pass the Kinds and DAGID bounds (a coalesced hint carries no kind
// or ids — Entities and Sources still apply).
type Filter struct {
	// Entities admits only these entity kinds when non-empty.
	Entities []Entity
	// Sources admits only job changes of these runtimes when non-empty; it
	// never excludes dag-header or ledger-event changes.
	Sources []Source
	// Kinds admits only these job kinds / event types / DAG definition names
	// when non-empty.
	Kinds []string
	// DAGID, when non-zero, admits only that DAG's header change and its
	// task-job changes.
	DAGID uuid.UUID
}

// matches reports whether the filter admits c.
func (f Filter) matches(c Change) bool {
	if c.Entity == EntityReset {
		return true
	}
	if len(f.Entities) > 0 && !slices.Contains(f.Entities, c.Entity) {
		return false
	}
	if len(f.Sources) > 0 && c.Entity == EntityJob && !slices.Contains(f.Sources, c.Source) {
		return false
	}
	if c.Bulk {
		// A coalesced hint carries no kind or ids; the remaining bounds
		// cannot apply, and hiding it would hide the very changes the
		// consumer filtered for.
		return true
	}
	if len(f.Kinds) > 0 && !slices.Contains(f.Kinds, c.Kind) {
		return false
	}
	if f.DAGID != uuid.Nil {
		switch c.Entity {
		case EntityDAG:
			return c.ID == f.DAGID
		case EntityJob:
			return c.DAGID == f.DAGID
		default:
			return false
		}
	}
	return true
}

// Watch subscribes to change hints matching f. The channel is closed when
// ctx ends or the store closes; the first delivery is always an EntityReset.
// Hints are best-effort and at-most-once: when the subscription's buffer
// fills, dropped hints are replaced by one in-band reset, so a slow consumer
// sees "refetch" instead of a silent gap. A poll-only driver cannot push
// changes; Watch reports that as an error.
func (w *Watcher) Watch(ctx context.Context, f Filter) (<-chan Change, error) {
	src, err := w.notifier.Changes(ctx)
	if err != nil {
		return nil, fmt.Errorf("watch: changes: %w", err)
	}
	if src == nil {
		return nil, errors.New("watch: driver is poll-only; change notifications are unavailable")
	}
	out := make(chan Change, w.cfg.buffer)
	go func() {
		defer close(out)
		pendingReset := false
		for c := range src {
			if !f.matches(c) {
				continue
			}
			if pendingReset && c.Entity != EntityReset {
				select {
				case out <- Change{Entity: EntityReset, At: c.At}:
					pendingReset = false
				default:
					// Still full: the pending reset keeps standing in for
					// everything dropped, this hint included.
					continue
				}
			}
			select {
			case out <- c:
				if c.Entity == EntityReset {
					pendingReset = false
				}
			default:
				pendingReset = true
			}
		}
	}()
	return out, nil
}

// Migrate brings the backend schema up to date (requires a driver.Migrator).
// Open and New never migrate automatically.
func (w *Watcher) Migrate(ctx context.Context) error { return w.core.Migrate(ctx) }

// Close releases the private Core when the Watcher was built with Open;
// composed over a shared Core it is a no-op.
func (w *Watcher) Close(ctx context.Context) error {
	if w.ownedCore {
		return w.core.Close(ctx)
	}
	return nil
}

// config is the Watcher's resolved settings. Unlike the job-running runtimes
// it does not embed azync.Defaults: watch leases nothing, retries nothing and
// retains nothing, so the shared job knobs have no meaning here.
type config struct {
	buffer      int
	coreOptions []azync.Option
}

// Option configures a Watcher. Options compose; later options win.
type Option func(*config) error

// resolveConfig layers opts over the package defaults. forOpen permits
// WithCoreOptions, which only makes sense when the Watcher owns its Core.
func resolveConfig(opts []Option, forOpen bool) (config, error) {
	c := config{buffer: defaultBuffer}
	for _, opt := range opts {
		if err := opt(&c); err != nil {
			return config{}, err
		}
	}
	if !forOpen && len(c.coreOptions) > 0 {
		return config{}, errors.New(
			"watch: WithCoreOptions applies only to Open; New composes over an already-built Core")
	}
	return c, nil
}

// WithBuffer overrides the per-subscription delivery buffer (default 256).
// A larger buffer tolerates slower consumers before hints collapse into a
// reset. Must be positive.
func WithBuffer(n int) Option {
	return positiveInt("WithBuffer", n, func(c *config) { c.buffer = n })
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

func positiveInt(name string, n int, set func(*config)) Option {
	return func(c *config) error {
		if n <= 0 {
			return fmt.Errorf("watch: %s: value must be positive, got %d", name, n)
		}
		set(c)
		return nil
	}
}
