package engine

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// startMaintenance runs only the maintenance loop (no fetch loops), so jobs
// stay wherever maintenance puts them.
func startMaintenance(t *testing.T, e *Engine, kinds []string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.maintenanceLoop(ctx, kinds)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func fastMaintenanceSettings() Settings {
	s := testSettings()
	s.LeaseTTL = 20 * time.Millisecond // reap sweep cadence
	s.PromoteInterval = 5 * time.Millisecond
	s.VacuumInterval = 5 * time.Millisecond
	return s
}

func TestMaintenancePromotesDueScheduledJobs(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	e := newTestEngine(f, fastMaintenanceSettings())

	id := uuid.New()
	_, err := f.Enqueue(context.Background(), driver.EnqueueParams{
		ID: id, Kind: "send", Payload: []byte(`{}`), Delay: time.Minute,
	})
	is.NoError(err)
	is.Equal(driver.StateScheduled, getJob(t, f, id).State)

	startMaintenance(t, e, []string{"send"})

	// Not yet due: it must stay scheduled across several promote ticks.
	time.Sleep(25 * time.Millisecond)
	is.Equal(driver.StateScheduled, getJob(t, f, id).State)

	clk.Advance(2 * time.Minute)
	is.Eventually(func() bool {
		return getJob(t, f, id).State == driver.StatePending
	}, 2*time.Second, 2*time.Millisecond, "a due scheduled job must be promoted to pending")
}

func TestMaintenanceReaperRevivesExpiredLease(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	e := newTestEngine(f, fastMaintenanceSettings())

	job := leaseOne(t, f, "send", 5)
	startMaintenance(t, e, []string{"send"})

	clk.Advance(2 * time.Minute) // past the lease
	is.Eventually(func() bool {
		got := getJob(t, f, job.ID)
		return got.State == driver.StatePending && got.ReapCount == 1
	}, 2*time.Second, 2*time.Millisecond, "an expired lease must be reclaimed to pending")
}

func TestMaintenanceReaperKillsPastMaxReaps(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	settings := fastMaintenanceSettings()
	settings.MaxReaps = 2
	e := newTestEngine(f, settings)

	job := leaseOne(t, f, "send", 5)
	startMaintenance(t, e, []string{"send"})
	ctx := context.Background()

	// First expiry: reaped back to pending.
	clk.Advance(2 * time.Minute)
	is.Eventually(func() bool {
		got := getJob(t, f, job.ID)
		return got.State == driver.StatePending && got.ReapCount == 1
	}, 2*time.Second, 2*time.Millisecond)

	// Lease again and expire again: reap_count reaches MaxReaps, job dies.
	jobs, err := f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: "send", Limit: 1, Lease: time.Minute})
	is.NoError(err)
	is.Len(jobs, 1)
	clk.Advance(2 * time.Minute)
	is.Eventually(func() bool {
		got := getJob(t, f, job.ID)
		return got.State == driver.StateDead && got.ReapCount == 2
	}, 2*time.Second, 2*time.Millisecond, "a poison job must be killed once reap_count reaches MaxReaps")
}

func TestMaintenanceVacuumCompletedHonorsRetention(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	settings := fastMaintenanceSettings()
	settings.CompletedRetention = time.Hour
	e := newTestEngine(f, settings)
	ctx := context.Background()

	job := leaseOne(t, f, "send", 5)
	is.NoError(f.Ack(ctx, job.ID, job.LeaseToken))

	startMaintenance(t, e, []string{"send"})

	// Within retention: history is kept.
	clk.Advance(30 * time.Minute)
	time.Sleep(25 * time.Millisecond)
	is.Equal(driver.StateSucceeded, getJob(t, f, job.ID).State)

	// Past retention: the succeeded row is trimmed.
	clk.Advance(time.Hour)
	is.Eventually(func() bool {
		_, err := f.GetJob(ctx, driver.SourceQueue, job.ID)
		return driver.IsNotFound(err)
	}, 2*time.Second, 2*time.Millisecond)
}

func TestMaintenanceVacuumCompletedZeroRetentionKeepsForever(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	settings := fastMaintenanceSettings()
	settings.CompletedRetention = 0 // explicit zero: retain forever
	e := newTestEngine(f, settings)
	ctx := context.Background()

	job := leaseOne(t, f, "send", 5)
	is.NoError(f.Ack(ctx, job.ID, job.LeaseToken))

	startMaintenance(t, e, []string{"send"})
	clk.Advance(365 * 24 * time.Hour)
	time.Sleep(30 * time.Millisecond) // several vacuum ticks
	is.Equal(driver.StateSucceeded, getJob(t, f, job.ID).State)
}
