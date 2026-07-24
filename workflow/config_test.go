package workflow

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
		azync.WithCompletedRetention(3*24*time.Hour))
	is.NoError(err)

	r, err := New(core)
	is.NoError(err)
	is.Equal(77*time.Second, r.cfg.LeaseTTL, "a core option flows into the runtime")
	is.Equal(11, r.cfg.DefaultMaxAttempts)
	is.Equal(3*24*time.Hour, r.cfg.CompletedRetention)
	// Untouched knobs keep the core defaults.
	is.Equal(64, r.cfg.MaxConcurrency)
	is.Equal(5, r.cfg.MaxReaps)
	// Workflow-only knobs take their package defaults.
	is.Equal(defaultWorkflowRetention, r.cfg.workflowRetention)
	is.Equal(defaultTaskTimeout, r.cfg.taskTimeout)
}

func TestConfigWorkflowOverrideWinsOverCore(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	core, err := azync.New(drivertest.NewFake(),
		azync.WithLogger(discardLogger()),
		azync.WithLeaseTTL(77*time.Second))
	is.NoError(err)

	r, err := New(core,
		WithLeaseTTL(11*time.Second),
		WithMaxConcurrency(3),
		WithWorkflowRetention(90*24*time.Hour),
		WithDefaultTaskTimeout(90*time.Second))
	is.NoError(err)
	is.Equal(11*time.Second, r.cfg.LeaseTTL, "package option > core option")
	is.Equal(3, r.cfg.MaxConcurrency)
	is.Equal(90*24*time.Hour, r.cfg.workflowRetention)
	is.Equal(90*time.Second, r.cfg.taskTimeout)
}

func TestConfigExplicitZeroRetentionIsPreserved(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	core, err := azync.New(drivertest.NewFake(), azync.WithLogger(discardLogger()))
	is.NoError(err)

	// Explicit 0 at the workflow layer means "retain forever" and must not be
	// swallowed back into the core default or the package default.
	r, err := New(core,
		WithStatsRetention(0),
		WithWorkflowRetention(0))
	is.NoError(err)
	is.Zero(r.cfg.StatsRetention)
	is.Zero(r.cfg.workflowRetention, "explicit 0 workflow retention retains terminal workflows forever")
}

func TestConfigRejectsInvalidValues(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	core, err := azync.New(drivertest.NewFake(), azync.WithLogger(discardLogger()))
	is.NoError(err)

	for name, opt := range map[string]Option{
		"WithLeaseTTL(0)":            WithLeaseTTL(0),
		"WithDefaultMaxRetries(0)":   WithDefaultMaxRetries(0),
		"WithShutdownDrain(-1)":      WithShutdownDrain(-time.Second),
		"WithMaxConcurrency(0)":      WithMaxConcurrency(0),
		"WithDefaultConcurrency(-2)": WithDefaultConcurrency(-2),
		"WithDefaultTaskTimeout(0)":  WithDefaultTaskTimeout(0),
		"WithMaxReaps(0)":            WithMaxReaps(0),
		"WithFetchBatchSize(0)":      WithFetchBatchSize(0),
		"WithFetchPollInterval(0)":   WithFetchPollInterval(0),
		"WithFetchCooldown(0)":       WithFetchCooldown(0),
		"WithIdleBackoffMax(0)":      WithIdleBackoffMax(0),
		"WithStatsRetention(-1)":     WithStatsRetention(-time.Hour),
		"WithWorkflowRetention(-1)":  WithWorkflowRetention(-time.Hour),
	} {
		_, err := New(core, opt)
		is.Error(err, "%s must be rejected", name)
		is.Contains(err.Error(), "workflow: ")
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
