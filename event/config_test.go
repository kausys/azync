package event

import (
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

func TestConfigInheritsCoreDefaults(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	core, err := azync.New(drivertest.NewFake(),
		azync.WithLogger(discardLogger()),
		azync.WithLeaseTTL(77*time.Second),
		azync.WithDefaultMaxAttempts(11),
		azync.WithCompletedRetention(7*24*time.Hour))
	is.NoError(err)

	r, err := New(core)
	is.NoError(err)
	is.Equal(77*time.Second, r.cfg.LeaseTTL, "core option must flow into the runtime")
	is.Equal(11, r.cfg.DefaultMaxAttempts)
	is.Equal(7*24*time.Hour, r.cfg.CompletedRetention)
	// Untouched knobs keep the core defaults.
	is.Equal(64, r.cfg.MaxConcurrency)
	is.Equal(5, r.cfg.MaxReaps)
	// The event-only knob keeps its default.
	is.Equal(5*time.Minute, r.cfg.handlerTimeout)
}

func TestConfigEventOverrideWinsOverCore(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	core, err := azync.New(drivertest.NewFake(),
		azync.WithLogger(discardLogger()),
		azync.WithLeaseTTL(77*time.Second),
		azync.WithCompletedRetention(7*24*time.Hour))
	is.NoError(err)

	r, err := New(core,
		WithLeaseTTL(11*time.Second),
		WithMaxConcurrency(3),
		WithCompletedRetention(3*24*time.Hour))
	is.NoError(err)
	is.Equal(11*time.Second, r.cfg.LeaseTTL, "package option > core option")
	is.Equal(3, r.cfg.MaxConcurrency)
	is.Equal(3*24*time.Hour, r.cfg.CompletedRetention, "event CompletedRetention 3d must win over core 7d")
}

func TestConfigHandlerTimeoutOverride(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	core, err := azync.New(drivertest.NewFake(), azync.WithLogger(discardLogger()))
	is.NoError(err)

	r, err := New(core, WithHandlerTimeout(90*time.Second))
	is.NoError(err)
	is.Equal(90*time.Second, r.cfg.handlerTimeout)
}

func TestConfigExplicitZeroRetentionIsPreserved(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	core, err := azync.New(drivertest.NewFake(),
		azync.WithLogger(discardLogger()),
		azync.WithCompletedRetention(7*24*time.Hour))
	is.NoError(err)

	// Explicit 0 at the event layer means "retain forever" and must not be
	// swallowed back into the core default.
	r, err := New(core, WithCompletedRetention(0), WithStatsRetention(0))
	is.NoError(err)
	is.Zero(r.cfg.CompletedRetention)
	is.Zero(r.cfg.StatsRetention)
}

func TestConfigRejectsInvalidValues(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	core, err := azync.New(drivertest.NewFake(), azync.WithLogger(discardLogger()))
	is.NoError(err)

	for name, opt := range map[string]Option{
		"WithLeaseTTL(0)":            WithLeaseTTL(0),
		"WithDefaultMaxAttempts(0)":  WithDefaultMaxAttempts(0),
		"WithShutdownDrain(-1)":      WithShutdownDrain(-time.Second),
		"WithMaxConcurrency(0)":      WithMaxConcurrency(0),
		"WithDefaultConcurrency(-2)": WithDefaultConcurrency(-2),
		"WithHandlerTimeout(0)":      WithHandlerTimeout(0),
		"WithMaxReaps(0)":            WithMaxReaps(0),
		"WithFetchBatchSize(0)":      WithFetchBatchSize(0),
		"WithFetchPollInterval(0)":   WithFetchPollInterval(0),
		"WithFetchCooldown(0)":       WithFetchCooldown(0),
		"WithIdleBackoffMax(0)":      WithIdleBackoffMax(0),
		"WithStatsRetention(-1)":     WithStatsRetention(-time.Hour),
		"WithCompletedRetention(-1)": WithCompletedRetention(-time.Hour),
	} {
		_, err := New(core, opt)
		is.Error(err, "%s must be rejected", name)
		is.Contains(err.Error(), "event: ")
	}
}

func TestConfigCoreOptionsRejectedByNew(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	core, err := azync.New(drivertest.NewFake(), azync.WithLogger(discardLogger()))
	is.NoError(err)

	_, err = New(core, WithCoreOptions(azync.WithLeaseTTL(time.Second)))
	is.Error(err)
	is.Contains(err.Error(), "WithCoreOptions applies only to Open")
}
