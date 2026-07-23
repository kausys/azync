package queue

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// seedJob enqueues one raw job of kind directly on the fake.
func seedJob(t *testing.T, f *drivertest.Fake, kind string) uuid.UUID {
	t.Helper()
	is := require.New(t)
	id := uuid.New()
	_, err := f.Enqueue(context.Background(), driver.EnqueueParams{
		ID: id, Kind: kind, Payload: json.RawMessage(`{}`), MaxAttempts: 3, MaxAttemptsExplicit: true,
	})
	is.NoError(err)
	return id
}

// leaseJob claims one pending job of kind.
func leaseJob(t *testing.T, f *drivertest.Fake, kind string) driver.Job {
	t.Helper()
	is := require.New(t)
	jobs, err := f.DequeueBatch(context.Background(), driver.SourceQueue,
		driver.DequeueParams{Kind: kind, Limit: 1, Lease: time.Minute})
	is.NoError(err)
	is.Len(jobs, 1)
	return jobs[0]
}

// makeDead seeds a job and fails it terminally.
func makeDead(t *testing.T, f *drivertest.Fake, kind, lastError string) uuid.UUID {
	t.Helper()
	is := require.New(t)
	id := seedJob(t, f, kind)
	job := leaseJob(t, f, kind)
	is.Equal(id, job.ID)
	is.NoError(f.Dead(context.Background(), job.ID, job.LeaseToken, lastError))
	return id
}

func TestManagerListQueues(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	seedJob(t, f, "kind.a")
	seedJob(t, f, "kind.a")
	job := hasLeaseJob(t, f, "kind.b") // seed + activate one of kind.b
	is.NoError(f.Ack(ctx, job.ID, job.LeaseToken))
	seedJob(t, f, "kind.b")

	queues, err := r.Manager().ListQueues(ctx)
	is.NoError(err)
	is.Len(queues, 2)
	is.Equal("kind.a", queues[0].Name)
	is.Equal("kind.a", queues[0].Namespace, "namespace is the kind itself, unprefixed")
	is.Equal(int64(2), queues[0].Pending)
	is.Equal("kind.b", queues[1].Name)
	is.Equal(int64(1), queues[1].Pending)
	is.Equal(int64(1), queues[1].Succeeded)
}

func hasLeaseJob(t *testing.T, f *drivertest.Fake, kind string) driver.Job {
	t.Helper()
	seedJob(t, f, kind)
	return leaseJob(t, f, kind)
}

func TestManagerStatsZeroFilledWindow(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake() // system clock: stats land on "today"
	r := newTestRuntime(t, f)
	ctx := context.Background()

	// enqueued 3, processed 1, failed 1.
	seedJob(t, f, "kind.s")
	first := hasLeaseJob(t, f, "kind.s")
	is.NoError(f.Ack(ctx, first.ID, first.LeaseToken))
	second := hasLeaseJob(t, f, "kind.s")
	is.NoError(f.Reschedule(ctx, second.ID, second.LeaseToken, time.Hour, "boom"))

	stats, err := r.Manager().Stats(ctx, "kind.s")
	is.NoError(err)
	is.Equal("kind.s", stats.Queue)
	is.Equal(30, stats.WindowDays)
	is.Len(stats.Daily, 30, "the daily series must be zero-filled to the full window")
	is.Equal(int64(3), stats.Enqueued)
	is.Equal(int64(1), stats.Processed)
	is.Equal(int64(1), stats.Failed)
	is.Equal(int64(1), stats.Pending)
	is.Equal(int64(1), stats.Scheduled)
	is.Equal(int64(1), stats.Succeeded)

	// Oldest first; today (the last entry) carries the counters.
	last := stats.Daily[len(stats.Daily)-1]
	is.Equal(int64(3), last.Enqueued)
	for i, d := range stats.Daily[:len(stats.Daily)-1] {
		is.Zero(d.Enqueued, "day %d must be zero-filled", i)
		is.True(d.Date.Before(stats.Daily[i+1].Date), "series must be oldest first")
	}
}

func TestManagerAllStatsAggregates(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	seedJob(t, f, "kind.x")
	seedJob(t, f, "kind.y")
	job := leaseJob(t, f, "kind.x")
	is.NoError(f.Ack(ctx, job.ID, job.LeaseToken))

	stats, err := r.Manager().AllStats(ctx)
	is.NoError(err)
	is.Empty(stats.Queue, "aggregate scope has no queue name")
	is.Equal(int64(1), stats.Pending)
	is.Equal(int64(1), stats.Succeeded)
	is.Equal(int64(2), stats.Enqueued)
	is.Equal(int64(1), stats.Processed)
	is.Len(stats.Daily, 30)
}

func TestManagerListPaginates(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	for range 3 {
		seedJob(t, f, "kind.p")
	}
	page0, err := r.Manager().List(ctx, "kind.p", StatePending, 0, 2)
	is.NoError(err)
	is.Equal(int64(3), page0.Total)
	is.Len(page0.Items, 2)
	is.Equal(0, page0.Page)
	is.Equal(2, page0.Size)

	page1, err := r.Manager().List(ctx, "kind.p", StatePending, 1, 2)
	is.NoError(err)
	is.Len(page1.Items, 1)
	is.Equal("kind.p", page1.Items[0].Kind)
}

func TestManagerListAllJobsAcrossKinds(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	makeDead(t, f, "kind.a", "boom-a")
	makeDead(t, f, "kind.b", "boom-b")
	seedJob(t, f, "kind.c") // pending: filtered out

	page, err := r.Manager().ListAllJobs(context.Background(), StateDead, 0, 10)
	is.NoError(err)
	is.Equal(int64(2), page.Total)
	kinds := []string{page.Items[0].Kind, page.Items[1].Kind}
	is.ElementsMatch([]string{"kind.a", "kind.b"}, kinds)
}

func TestManagerGetReturnsNilWhenMissing(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	view, err := r.Manager().Get(ctx, uuid.New())
	is.NoError(err)
	is.Nil(view, "a missing job is nil, nil — not an error")

	id := seedJob(t, f, "kind.g")
	view, err = r.Manager().Get(ctx, id)
	is.NoError(err)
	is.NotNil(view)
	is.Equal(id, view.ID)
	is.Equal("kind.g", view.Kind)
	is.Equal(StatePending, view.State)
	is.JSONEq(`{}`, string(view.Payload))
}

func TestManagerRetryAndRetryAll(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	dead1 := makeDead(t, f, "kind.r", "boom")
	dead2 := makeDead(t, f, "kind.r", "boom")

	is.NoError(r.Manager().Retry(ctx, dead1))
	is.Equal(driver.StatePending, getJob(t, f, dead1).State)
	is.Zero(getJob(t, f, dead1).Attempt, "retry resets the attempt budget")

	// Retrying a non-dead job is a not-found error.
	err := r.Manager().Retry(ctx, dead1)
	is.Error(err)
	is.True(IsNotFound(err))

	n, err := r.Manager().RetryAll(ctx, "kind.r")
	is.NoError(err)
	is.Equal(int64(1), n)
	is.Equal(driver.StatePending, getJob(t, f, dead2).State)
}

func TestManagerArchivePauseResume(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	archived := seedJob(t, f, "kind.m")
	is.NoError(r.Manager().Archive(ctx, archived))
	is.Equal(driver.StateDead, getJob(t, f, archived).State)

	paused := seedJob(t, f, "kind.m")
	is.NoError(r.Manager().Pause(ctx, paused))
	is.Equal(driver.StatePaused, getJob(t, f, paused).State)
	is.NoError(r.Manager().Resume(ctx, paused))
	is.Equal(driver.StatePending, getJob(t, f, paused).State)

	// Pausing an active job is refused.
	active := hasLeaseJob(t, f, "kind.m")
	err := r.Manager().Pause(ctx, active.ID)
	is.Error(err)
	is.True(IsNotFound(err))
}

func TestManagerDeleteRequiresMatchingState(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	id := seedJob(t, f, "kind.d")
	err := r.Manager().Delete(ctx, id, StateDead) // wrong state
	is.Error(err)
	is.True(IsNotFound(err))

	is.NoError(r.Manager().Delete(ctx, id, StatePending))
	_, err = f.GetJob(ctx, driver.SourceQueue, id)
	is.True(driver.IsNotFound(err))
}

func TestManagerPurgeSparesActiveAndPaused(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f)
	ctx := context.Background()

	// Build each state with at most one pending job live, so every lease is
	// deterministic.
	makeDead(t, f, "kind.z", "boom")
	active := hasLeaseJob(t, f, "kind.z")
	pausedID := seedJob(t, f, "kind.z")
	is.NoError(r.Manager().Pause(ctx, pausedID))
	seedJob(t, f, "kind.z") // pending
	scheduled := uuid.New()
	_, err := f.Enqueue(ctx, driver.EnqueueParams{
		ID: scheduled, Kind: "kind.z", Payload: json.RawMessage(`{}`), Delay: time.Hour,
	})
	is.NoError(err)

	report, err := r.Manager().Purge(ctx, "kind.z")
	is.NoError(err)
	is.Equal(int64(1), report.Pending)
	is.Equal(int64(1), report.Scheduled)
	is.Equal(int64(1), report.Dead)
	is.Equal(int64(1), report.ActiveRemaining)

	is.Equal(driver.StateActive, getJob(t, f, active.ID).State, "active jobs survive a purge")
	is.Equal(driver.StatePaused, getJob(t, f, pausedID).State, "paused jobs survive a purge")
}

func TestManagerVacuumDead(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f)
	ctx := context.Background()

	old := makeDead(t, f, "kind.v", "boom")
	clk.Advance(2 * time.Hour)
	fresh := makeDead(t, f, "kind.v", "boom")

	n, err := r.Manager().VacuumDead(ctx, "kind.v", time.Hour)
	is.NoError(err)
	is.Equal(int64(1), n)
	_, err = f.GetJob(ctx, driver.SourceQueue, old)
	is.True(driver.IsNotFound(err))
	is.Equal(driver.StateDead, getJob(t, f, fresh).State)
}

func TestManagerNukeAll(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	makeDead(t, f, "kind.n", "boom")
	seedJob(t, f, "kind.n")

	report, err := r.Manager().NukeAll(ctx)
	is.NoError(err)
	is.Equal(int64(2), report.Jobs)

	queues, err := r.Manager().ListQueues(ctx)
	is.NoError(err)
	is.Empty(queues)
}

func TestManagerJobAttemptsOldestFirst(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	id := seedJob(t, f, "kind.h")
	job := leaseJob(t, f, "kind.h")
	is.NoError(f.Reschedule(ctx, job.ID, job.LeaseToken, 0, "boom-1"))
	_, err := f.PromoteDue(ctx, driver.SourceQueue, []string{"kind.h"})
	is.NoError(err)
	job = leaseJob(t, f, "kind.h")
	is.NoError(f.Dead(ctx, job.ID, job.LeaseToken, "boom-2"))

	attempts, err := r.Manager().JobAttempts(ctx, id)
	is.NoError(err)
	is.Len(attempts, 2)
	is.Equal(1, attempts[0].Attempt)
	is.Equal("boom-1", attempts[0].Error)
	is.Equal(2, attempts[1].Attempt)
	is.Equal("boom-2", attempts[1].Error)
}
