package azync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/kausys/azync/driver"
)

// maxIdentifierBytes bounds a schema/channel/table name, matching PostgreSQL's
// unquoted-identifier limit; the validation is applied generically so no
// arbitrary string ever reaches a driver as a name.
const maxIdentifierBytes = 63

var (
	driversMu sync.RWMutex
	drivers   = map[string]driver.Opener{}
)

// RegisterDriver registers a driver.Opener under a DSN scheme, in the style of
// database/sql. Drivers call it from an init function so a blank import wires
// them in. It panics if scheme is empty, opener is nil, or the scheme is already
// registered.
func RegisterDriver(scheme string, opener driver.Opener) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if scheme == "" {
		panic("azync: RegisterDriver scheme is empty")
	}
	if opener == nil {
		panic("azync: RegisterDriver opener is nil")
	}
	if _, dup := drivers[scheme]; dup {
		panic("azync: RegisterDriver called twice for scheme " + scheme)
	}
	drivers[scheme] = opener
}

// registeredSchemes returns the sorted set of registered schemes, for error
// messages.
func registeredSchemes() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	out := make([]string, 0, len(drivers))
	for s := range drivers {
		out = append(out, s)
	}
	return out
}

// Core is the shared root: it owns the storage driver, the resolved defaults and
// the logger. The queue and event runtimes compose over one Core, either sharing
// it (queue.New(core)) or owning a private one (queue.Open(dsn)).
type Core struct {
	store    driver.Store
	logger   *slog.Logger
	defaults Defaults
}

// Open resolves the driver for the DSN's scheme from the registry, builds the
// driver.Config from the options, and opens a Store. The DSN scheme selects the
// driver; register one with a blank import. Open never migrates and never
// includes the DSN (which may carry credentials) in an error.
func Open(dsn string, opts ...Option) (*Core, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		// url.Parse embeds the raw input in its error, so never wrap it.
		return nil, errors.New("azync: dsn is not a valid URL")
	}
	scheme := u.Scheme
	if scheme == "" {
		return nil, errors.New("azync: dsn has no scheme; expected e.g. \"postgres://...\"")
	}

	driversMu.RLock()
	opener, ok := drivers[scheme]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf(
			"azync: no driver registered for scheme %q (add a blank import such as "+
				"`import _ \"github.com/kausys/azync/driver/azyncpgx\"`; registered: %v)",
			scheme, registeredSchemes())
	}

	cfg, err := buildConfig(opts, false)
	if err != nil {
		return nil, err
	}
	store, err := opener(dsn, cfg.driverConfig())
	if err != nil {
		return nil, fmt.Errorf("azync: open driver for scheme %q: %w", scheme, err)
	}
	return cfg.newCore(store), nil
}

// New builds a Core over an already-constructed Store. Infrastructure options
// (WithSchema, WithNotifyChannel, WithMigrationsTable, PollOnly) are rejected
// here because the store is already built; only the logger and defaults options
// apply.
func New(store driver.Store, opts ...Option) (*Core, error) {
	if store == nil {
		return nil, errors.New("azync: New store is nil")
	}
	cfg, err := buildConfig(opts, true)
	if err != nil {
		return nil, err
	}
	return cfg.newCore(store), nil
}

// Store returns the underlying storage driver the runtimes operate on.
func (c *Core) Store() driver.Store { return c.store }

// Defaults returns the resolved shared defaults. Runtimes read these as their
// baseline and may override individual values per runtime.
func (c *Core) Defaults() Defaults { return c.defaults }

// Logger returns the Core's structured logger (never nil).
func (c *Core) Logger() *slog.Logger { return c.logger }

// Migrate brings the backend schema up to date. It requires a driver.Migrator;
// otherwise it returns an error wrapping driver.ErrNotSupported.
func (c *Core) Migrate(ctx context.Context) error {
	m, ok := c.store.(driver.Migrator)
	if !ok {
		return fmt.Errorf("azync: migrate: %w", driver.ErrNotSupported)
	}
	return m.Migrate(ctx)
}

// Close releases the driver's resources.
func (c *Core) Close(ctx context.Context) error { return c.store.Close(ctx) }

// Defaults are the shared baseline settings a Core resolves from options. Each
// value is a starting point the queue and event runtimes may override per
// runtime (package option > core option > default).
type Defaults struct {
	// LeaseTTL is how long a worker holds a job before its lease is reclaimable.
	LeaseTTL time.Duration
	// DefaultMaxAttempts is the retry budget applied to jobs enqueued without an
	// explicit budget.
	DefaultMaxAttempts int
	// ShutdownDrain is how long Close waits for in-flight jobs to settle.
	ShutdownDrain time.Duration
	// MaxConcurrency caps the total concurrent handlers across a runtime.
	MaxConcurrency int
	// DefaultConcurrency is the per-kind handler concurrency when unset.
	DefaultConcurrency int
	// FetchBatchSize is how many jobs one dequeue leases at a time.
	FetchBatchSize int
	// FetchPollInterval is the polling period when no wakeups arrive.
	FetchPollInterval time.Duration
	// FetchCooldown is the pause after a full batch before fetching again.
	FetchCooldown time.Duration
	// IdleBackoffMax caps the backoff a fetch loop reaches while idle.
	IdleBackoffMax time.Duration
	// MaxReaps is how many lease expirations a job survives before it is killed.
	MaxReaps int
	// StatsRetention is how long daily stat counters are kept; 0 keeps them
	// forever.
	StatsRetention time.Duration
	// CompletedRetention is how long succeeded jobs are kept; 0 keeps them
	// forever.
	CompletedRetention time.Duration
}

func defaultDefaults() Defaults {
	return Defaults{
		LeaseTTL:           30 * time.Second,
		DefaultMaxAttempts: 25,
		ShutdownDrain:      25 * time.Second,
		MaxConcurrency:     64,
		DefaultConcurrency: 4,
		FetchBatchSize:     10,
		FetchPollInterval:  time.Second,
		FetchCooldown:      100 * time.Millisecond,
		IdleBackoffMax:     2 * time.Second,
		MaxReaps:           5,
		StatsRetention:     35 * 24 * time.Hour,
		CompletedRetention: 7 * 24 * time.Hour,
	}
}

// coreConfig accumulates option effects. It starts from defaultDefaults so an
// unset option leaves the documented default in place; a retention option may
// set 0 explicitly, which the pre-seeded default preserves as "retain forever".
type coreConfig struct {
	schema          string
	notifyChannel   string
	migrationsTable string
	pollOnly        bool
	infraOptions    []string
	logger          *slog.Logger
	defaults        Defaults
}

func buildConfig(opts []Option, forNew bool) (*coreConfig, error) {
	c := &coreConfig{defaults: defaultDefaults()}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	if forNew && len(c.infraOptions) > 0 {
		return nil, fmt.Errorf(
			"azync: New: option(s) %s apply only to Open; the store is already constructed",
			strings.Join(c.infraOptions, ", "))
	}
	return c, nil
}

func (c *coreConfig) newCore(store driver.Store) *Core {
	logger := c.logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Core{store: store, logger: logger, defaults: c.defaults}
}

func (c *coreConfig) driverConfig() driver.Config {
	return driver.Config{
		Schema:          c.schema,
		NotifyChannel:   c.notifyChannel,
		MigrationsTable: c.migrationsTable,
		PollOnly:        c.pollOnly,
		Logger:          c.logger,
	}
}

func (c *coreConfig) markInfra(name string) { c.infraOptions = append(c.infraOptions, name) }

// Option configures a Core. Options compose; later options win.
type Option func(*coreConfig) error

// WithSchema isolates azync's tables in the named backend schema (empty uses the
// backend default). The name is validated as an identifier. Infrastructure
// option: valid only with Open.
func WithSchema(schema string) Option {
	return func(c *coreConfig) error {
		if err := validateIdentifier("schema", schema); err != nil {
			return err
		}
		c.schema = schema
		c.markInfra("WithSchema")
		return nil
	}
}

// WithNotifyChannel sets the driver's wakeup channel name. Infrastructure
// option: valid only with Open.
func WithNotifyChannel(channel string) Option {
	return func(c *coreConfig) error {
		if err := validateIdentifier("notify channel", channel); err != nil {
			return err
		}
		c.notifyChannel = channel
		c.markInfra("WithNotifyChannel")
		return nil
	}
}

// WithMigrationsTable overrides the migration version-tracking table name
// (default azync_migrations in the pg driver). Infrastructure option: valid only
// with Open.
func WithMigrationsTable(table string) Option {
	return func(c *coreConfig) error {
		if err := validateIdentifier("migrations table", table); err != nil {
			return err
		}
		c.migrationsTable = table
		c.markInfra("WithMigrationsTable")
		return nil
	}
}

// PollOnly disables push wakeups, forcing the always-correct polling path.
// Infrastructure option: valid only with Open.
func PollOnly() Option {
	return func(c *coreConfig) error {
		c.pollOnly = true
		c.markInfra("PollOnly")
		return nil
	}
}

// WithLogger sets the Core's structured logger. A nil logger is rejected.
func WithLogger(logger *slog.Logger) Option {
	return func(c *coreConfig) error {
		if logger == nil {
			return errors.New("azync: WithLogger: logger is nil")
		}
		c.logger = logger
		return nil
	}
}

// WithLeaseTTL sets Defaults.LeaseTTL. Must be positive.
func WithLeaseTTL(d time.Duration) Option {
	return positiveDuration("WithLeaseTTL", d, func(cfg *Defaults) { cfg.LeaseTTL = d })
}

// WithDefaultMaxAttempts sets Defaults.DefaultMaxAttempts. Must be positive.
func WithDefaultMaxAttempts(n int) Option {
	return positiveInt("WithDefaultMaxAttempts", n, func(cfg *Defaults) { cfg.DefaultMaxAttempts = n })
}

// WithShutdownDrain sets Defaults.ShutdownDrain. Must be positive.
func WithShutdownDrain(d time.Duration) Option {
	return positiveDuration("WithShutdownDrain", d, func(cfg *Defaults) { cfg.ShutdownDrain = d })
}

// WithMaxConcurrency sets Defaults.MaxConcurrency. Must be positive.
func WithMaxConcurrency(n int) Option {
	return positiveInt("WithMaxConcurrency", n, func(cfg *Defaults) { cfg.MaxConcurrency = n })
}

// WithDefaultConcurrency sets Defaults.DefaultConcurrency. Must be positive.
func WithDefaultConcurrency(n int) Option {
	return positiveInt("WithDefaultConcurrency", n, func(cfg *Defaults) { cfg.DefaultConcurrency = n })
}

// WithFetchBatchSize sets Defaults.FetchBatchSize. Must be positive.
func WithFetchBatchSize(n int) Option {
	return positiveInt("WithFetchBatchSize", n, func(cfg *Defaults) { cfg.FetchBatchSize = n })
}

// WithFetchPollInterval sets Defaults.FetchPollInterval. Must be positive.
func WithFetchPollInterval(d time.Duration) Option {
	return positiveDuration("WithFetchPollInterval", d, func(cfg *Defaults) { cfg.FetchPollInterval = d })
}

// WithFetchCooldown sets Defaults.FetchCooldown. Must be positive.
func WithFetchCooldown(d time.Duration) Option {
	return positiveDuration("WithFetchCooldown", d, func(cfg *Defaults) { cfg.FetchCooldown = d })
}

// WithIdleBackoffMax sets Defaults.IdleBackoffMax. Must be positive.
func WithIdleBackoffMax(d time.Duration) Option {
	return positiveDuration("WithIdleBackoffMax", d, func(cfg *Defaults) { cfg.IdleBackoffMax = d })
}

// WithMaxReaps sets Defaults.MaxReaps. Must be positive.
func WithMaxReaps(n int) Option {
	return positiveInt("WithMaxReaps", n, func(cfg *Defaults) { cfg.MaxReaps = n })
}

// WithStatsRetention sets Defaults.StatsRetention. A negative value is rejected;
// zero means retain stat counters forever.
func WithStatsRetention(d time.Duration) Option {
	return nonNegativeDuration("WithStatsRetention", d, func(cfg *Defaults) { cfg.StatsRetention = d })
}

// WithCompletedRetention sets Defaults.CompletedRetention. A negative value is
// rejected; zero means retain succeeded jobs forever.
func WithCompletedRetention(d time.Duration) Option {
	return nonNegativeDuration("WithCompletedRetention", d, func(cfg *Defaults) { cfg.CompletedRetention = d })
}

func positiveDuration(name string, d time.Duration, set func(*Defaults)) Option {
	return func(c *coreConfig) error {
		if d <= 0 {
			return fmt.Errorf("azync: %s: duration must be positive, got %v", name, d)
		}
		set(&c.defaults)
		return nil
	}
}

func positiveInt(name string, n int, set func(*Defaults)) Option {
	return func(c *coreConfig) error {
		if n <= 0 {
			return fmt.Errorf("azync: %s: value must be positive, got %d", name, n)
		}
		set(&c.defaults)
		return nil
	}
}

func nonNegativeDuration(name string, d time.Duration, set func(*Defaults)) Option {
	return func(c *coreConfig) error {
		if d < 0 {
			return fmt.Errorf("azync: %s: duration must not be negative, got %v", name, d)
		}
		set(&c.defaults)
		return nil
	}
}

// validateIdentifier accepts a leading letter or underscore followed by
// letters, digits, underscores or dollar signs, up to 63 bytes — the
// PostgreSQL unquoted-identifier rule, applied generically so an arbitrary
// string never reaches a driver as a name.
func validateIdentifier(kind, name string) error {
	if name == "" {
		return fmt.Errorf("azync: %s must not be empty", kind)
	}
	if len(name) > maxIdentifierBytes {
		return fmt.Errorf("azync: %s %q exceeds %d bytes", kind, name, maxIdentifierBytes)
	}
	if !isIdentifierStart(name[0]) {
		return fmt.Errorf("azync: %s %q must begin with a letter or underscore", kind, name)
	}
	for i := 1; i < len(name); i++ {
		if !isIdentifierPart(name[i]) {
			return fmt.Errorf("azync: %s %q contains an invalid character", kind, name)
		}
	}
	return nil
}

func isIdentifierStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentifierPart(c byte) bool {
	return isIdentifierStart(c) || (c >= '0' && c <= '9') || c == '$'
}
