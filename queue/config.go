package queue

import (
	"errors"
	"fmt"
	"time"

	"github.com/kausys/azync"
)

// queue-only defaults; the shared knobs default from the core (azync.Defaults).
const (
	defaultCronTick   = 30 * time.Second
	defaultJobTimeout = 5 * time.Minute
)

// config is the runtime's resolved settings: the core's shared defaults as the
// baseline, plus queue-only knobs, all overridable per runtime by With*
// options (package option > core option > default).
type config struct {
	azync.Defaults

	cronEnabled bool
	cronTick    time.Duration
	jobTimeout  time.Duration

	coreOptions []azync.Option

	// promoteInterval and vacuumInterval shrink the engine maintenance
	// cadences in tests; they are deliberately not exposed as public options.
	promoteInterval time.Duration
	vacuumInterval  time.Duration
}

// Option configures a queue Runtime. Options compose; later options win.
type Option func(*config) error

// resolveConfig layers opts over the core defaults. forOpen permits
// WithCoreOptions, which only makes sense when the runtime owns its Core.
func resolveConfig(defaults azync.Defaults, opts []Option, forOpen bool) (config, error) {
	c := config{
		Defaults:    defaults,
		cronEnabled: true,
		cronTick:    defaultCronTick,
		jobTimeout:  defaultJobTimeout,
	}
	for _, opt := range opts {
		if err := opt(&c); err != nil {
			return config{}, err
		}
	}
	if !forOpen && len(c.coreOptions) > 0 {
		return config{}, errors.New(
			"queue: WithCoreOptions applies only to Open; New composes over an already-built Core")
	}
	return c, nil
}

// WithLeaseTTL overrides the shared lease duration for this runtime. Must be
// positive.
func WithLeaseTTL(d time.Duration) Option {
	return positiveDuration("WithLeaseTTL", d, func(c *config) { c.LeaseTTL = d })
}

// WithDefaultMaxRetries overrides the retry budget applied to jobs enqueued
// without an explicit budget. Must be positive.
func WithDefaultMaxRetries(n int) Option {
	return positiveInt("WithDefaultMaxRetries", n, func(c *config) { c.DefaultMaxAttempts = n })
}

// WithShutdownDrain overrides how long Start waits for in-flight jobs on
// shutdown. Must be positive.
func WithShutdownDrain(d time.Duration) Option {
	return positiveDuration("WithShutdownDrain", d, func(c *config) { c.ShutdownDrain = d })
}

// WithMaxConcurrency overrides the total concurrent-handler cap. Must be
// positive.
func WithMaxConcurrency(n int) Option {
	return positiveInt("WithMaxConcurrency", n, func(c *config) { c.MaxConcurrency = n })
}

// WithDefaultConcurrency overrides the per-kind concurrency used when a
// registration does not set its own. Must be positive.
func WithDefaultConcurrency(n int) Option {
	return positiveInt("WithDefaultConcurrency", n, func(c *config) { c.DefaultConcurrency = n })
}

// WithDefaultJobTimeout overrides the default per-job wall clock applied to
// registrations that do not set their own (default 5m). Must be positive; a
// single kind can still opt out with the WithJobTimeout(0) register option.
func WithDefaultJobTimeout(d time.Duration) Option {
	return positiveDuration("WithDefaultJobTimeout", d, func(c *config) { c.jobTimeout = d })
}

// WithMaxReaps overrides how many lease expirations a job survives before the
// reaper kills it. Must be positive.
func WithMaxReaps(n int) Option {
	return positiveInt("WithMaxReaps", n, func(c *config) { c.MaxReaps = n })
}

// WithCron enables or disables the cron scheduler (default enabled).
func WithCron(enabled bool) Option {
	return func(c *config) error {
		c.cronEnabled = enabled
		return nil
	}
}

// WithCronTick overrides how often the cron leader checks its schedules
// (default 30s). Must be positive.
func WithCronTick(d time.Duration) Option {
	return positiveDuration("WithCronTick", d, func(c *config) { c.cronTick = d })
}

// WithFetchBatchSize overrides how many jobs one dequeue leases. Must be
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

// WithCompletedRetention overrides how long succeeded jobs are kept. A
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
			return fmt.Errorf("queue: %s: duration must be positive, got %v", name, d)
		}
		set(c)
		return nil
	}
}

func positiveInt(name string, n int, set func(*config)) Option {
	return func(c *config) error {
		if n <= 0 {
			return fmt.Errorf("queue: %s: value must be positive, got %d", name, n)
		}
		set(c)
		return nil
	}
}

func nonNegativeDuration(name string, d time.Duration, set func(*config)) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("queue: %s: duration must not be negative, got %v", name, d)
		}
		set(c)
		return nil
	}
}
