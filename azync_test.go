package azync_test

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

func fakeOpener(string, driver.Config) (driver.Store, error) {
	return drivertest.NewFake(), nil
}

func TestRegisterDriverDuplicatePanics(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	azync.RegisterDriver("azynctest-dup", fakeOpener)
	is.PanicsWithValue(
		"azync: RegisterDriver called twice for scheme azynctest-dup",
		func() { azync.RegisterDriver("azynctest-dup", fakeOpener) })
}

func TestRegisterDriverRejectsEmptyAndNil(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	is.Panics(func() { azync.RegisterDriver("", fakeOpener) })
	is.Panics(func() { azync.RegisterDriver("azynctest-nilopener", nil) })
}

func TestOpenUnregisteredSchemeMentionsBlankImportAndHidesDSN(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	_, err := azync.Open("unregistered://user:s3cr3t@db.internal:5432/app?sslmode=require")
	is.Error(err)
	is.Contains(err.Error(), "blank import")
	is.Contains(err.Error(), "unregistered")
	is.NotContains(err.Error(), "s3cr3t", "the DSN's credentials must never appear in the error")
	is.NotContains(err.Error(), "db.internal")
}

func TestOpenInvalidDSN(t *testing.T) {
	t.Parallel()
	is := require.New(t)

	// A control character makes url.Parse fail; the raw input must not leak.
	_, err := azync.Open("\x7fpostgres://user:s3cr3t@host")
	is.Error(err)
	is.NotContains(err.Error(), "s3cr3t")

	// A DSN with no scheme is rejected with a clear message.
	_, err = azync.Open("notaurl")
	is.Error(err)
	is.Contains(err.Error(), "scheme")
}

func TestOpenSuccess(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	azync.RegisterDriver("azynctest-open", fakeOpener)
	core, err := azync.Open("azynctest-open://localhost/app")
	is.NoError(err)
	is.NotNil(core)
	is.NotNil(core.Store())
	is.NotNil(core.Logger())
	is.Equal(30*time.Second, core.Defaults().LeaseTTL)
}

func TestNewRejectsInfraOptions(t *testing.T) {
	t.Parallel()
	store := drivertest.NewFake()

	for _, tc := range []struct {
		name string
		opt  azync.Option
		want string
	}{
		{"schema", azync.WithSchema("tenant"), "WithSchema"},
		{"channel", azync.WithNotifyChannel("azync"), "WithNotifyChannel"},
		{"migrations", azync.WithMigrationsTable("azync_migrations"), "WithMigrationsTable"},
		{"pollonly", azync.PollOnly(), "PollOnly"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			is := require.New(t)
			_, err := azync.New(store, tc.opt)
			is.Error(err)
			is.Contains(err.Error(), tc.want)
			is.Contains(err.Error(), "Open")
		})
	}
}

func TestNewAcceptsLoggerAndDefaults(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	store := drivertest.NewFake()
	core, err := azync.New(store, azync.WithLeaseTTL(5*time.Second))
	is.NoError(err)
	is.Equal(5*time.Second, core.Defaults().LeaseTTL)
	is.Same(store, core.Store())
}

func TestNewNilStore(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	_, err := azync.New(nil)
	is.Error(err)
}

func TestOptionValidation(t *testing.T) {
	t.Parallel()
	store := drivertest.NewFake()

	invalid := []struct {
		name string
		opt  azync.Option
	}{
		{"negative lease", azync.WithLeaseTTL(-time.Second)},
		{"zero lease", azync.WithLeaseTTL(0)},
		{"zero max attempts", azync.WithDefaultMaxAttempts(0)},
		{"negative concurrency", azync.WithMaxConcurrency(-1)},
		{"negative stats retention", azync.WithStatsRetention(-time.Hour)},
		{"negative completed retention", azync.WithCompletedRetention(-time.Hour)},
		{"empty schema", azync.WithSchema("")},
		{"schema starting with digit", azync.WithSchema("1bad")},
		{"schema with dash", azync.WithSchema("bad-name")},
		{"nil logger", azync.WithLogger(nil)},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			is := require.New(t)
			_, err := azync.New(store, tc.opt)
			is.Error(err)
		})
	}
}

func TestRetentionZeroMeansRetainForever(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	store := drivertest.NewFake()
	core, err := azync.New(store, azync.WithCompletedRetention(0), azync.WithStatsRetention(0))
	is.NoError(err)
	is.Equal(time.Duration(0), core.Defaults().CompletedRetention)
	is.Equal(time.Duration(0), core.Defaults().StatsRetention)
}

func TestDefaultsLayering(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	store := drivertest.NewFake()

	// No options: every documented default is present.
	base, err := azync.New(store)
	is.NoError(err)
	d := base.Defaults()
	is.Equal(30*time.Second, d.LeaseTTL)
	is.Equal(25, d.DefaultMaxAttempts)
	is.Equal(25*time.Second, d.ShutdownDrain)
	is.Equal(64, d.MaxConcurrency)
	is.Equal(4, d.DefaultConcurrency)
	is.Equal(10, d.FetchBatchSize)
	is.Equal(time.Second, d.FetchPollInterval)
	is.Equal(100*time.Millisecond, d.FetchCooldown)
	is.Equal(2*time.Second, d.IdleBackoffMax)
	is.Equal(5, d.MaxReaps)
	is.Equal(35*24*time.Hour, d.StatsRetention)
	is.Equal(7*24*time.Hour, d.CompletedRetention)

	// One override does not disturb the other defaults.
	over, err := azync.New(store, azync.WithMaxReaps(9))
	is.NoError(err)
	is.Equal(9, over.Defaults().MaxReaps)
	is.Equal(30*time.Second, over.Defaults().LeaseTTL)
}

func TestMigrateWithoutMigratorIsNotSupported(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	store := drivertest.NewFake() // the fake is not a driver.Migrator
	core, err := azync.New(store)
	is.NoError(err)
	err = core.Migrate(context.Background())
	is.Error(err)
	is.ErrorIs(err, driver.ErrNotSupported)
}

func TestCloseDelegatesToStore(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	store := drivertest.NewFake()
	core, err := azync.New(store)
	is.NoError(err)
	is.NoError(core.Close(context.Background()))
}
