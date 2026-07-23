package engine

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// discardLogger keeps test output pristine.
func discardLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func testSettings() Settings {
	return Settings{
		LeaseTTL:           time.Minute,
		ShutdownDrain:      time.Second,
		MaxConcurrency:     8,
		FetchBatchSize:     4,
		FetchPollInterval:  5 * time.Millisecond,
		FetchCooldown:      time.Millisecond,
		IdleBackoffMax:     10 * time.Millisecond,
		MaxReaps:           3,
		StatsRetention:     35 * 24 * time.Hour,
		CompletedRetention: 7 * 24 * time.Hour,
	}
}

func newTestEngine(f *drivertest.Fake, settings Settings) *Engine {
	return New(Config{Store: f, Source: driver.SourceQueue, Logger: discardLogger(), Settings: settings})
}

// leaseOne enqueues and manually leases one job so execute can be driven
// directly, without the fetch loop.
func leaseOne(t *testing.T, f *drivertest.Fake, kind string, maxAttempts int) driver.Job {
	t.Helper()
	is := require.New(t)
	ctx := context.Background()
	id := uuid.New()
	inserted, err := f.Enqueue(ctx, driver.EnqueueParams{
		ID: id, Kind: kind, Payload: json.RawMessage(`{}`),
		MaxAttempts: maxAttempts, MaxAttemptsExplicit: true,
	})
	is.NoError(err)
	is.True(inserted)
	jobs, err := f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{Kind: kind, Limit: 1, Lease: time.Minute})
	is.NoError(err)
	is.Len(jobs, 1)
	return jobs[0]
}

func getJob(t *testing.T, f *drivertest.Fake, id uuid.UUID) driver.Job {
	t.Helper()
	is := require.New(t)
	j, err := f.GetJob(context.Background(), driver.SourceQueue, id)
	is.NoError(err)
	return *j
}

func retryClassifier(o Outcome) Classifier {
	return func(error) Outcome { return o }
}

func TestExecuteSuccessAcksAndRetainsSucceeded(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	job := leaseOne(t, f, "send", 5)
	ran := false
	k := Kind{Name: "send", Concurrency: 1, Handler: func(context.Context, driver.Job) error {
		ran = true
		return nil
	}}
	e.execute(context.Background(), k, job, func() {})

	is.True(ran)
	got := getJob(t, f, job.ID)
	is.Equal(driver.StateSucceeded, got.State)
	is.Equal(1, got.Attempt)
}

func TestExecutePlainErrorReschedulesWithBackoff(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	e := newTestEngine(f, testSettings())

	job := leaseOne(t, f, "send", 5)
	k := Kind{Name: "send", Concurrency: 1,
		Handler:  func(context.Context, driver.Job) error { return errors.New("boom") },
		Classify: retryClassifier(Outcome{Kind: OutcomeRetry}),
	}
	e.execute(context.Background(), k, job, func() {})

	got := getJob(t, f, job.ID)
	is.Equal(driver.StateScheduled, got.State)
	is.Equal(clk.Now().Add(Backoff(1)), got.RunAt, "retry delay must be the deterministic backoff for attempt 1")
	is.Equal("boom", got.LastError)
}

func TestExecuteClassifiedDelayWinsOverBackoff(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	e := newTestEngine(f, testSettings())

	job := leaseOne(t, f, "send", 5)
	delay := 42 * time.Second
	k := Kind{Name: "send", Concurrency: 1,
		Handler:  func(context.Context, driver.Job) error { return errors.New("later") },
		Classify: retryClassifier(Outcome{Kind: OutcomeRetry, Delay: delay}),
	}
	e.execute(context.Background(), k, job, func() {})

	got := getJob(t, f, job.ID)
	is.Equal(driver.StateScheduled, got.State)
	is.Equal(clk.Now().Add(delay), got.RunAt)
}

func TestExecuteAbortDeadLetters(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	job := leaseOne(t, f, "send", 5)
	k := Kind{Name: "send", Concurrency: 1,
		Handler:  func(context.Context, driver.Job) error { return errors.New("permanent") },
		Classify: retryClassifier(Outcome{Kind: OutcomeAbort}),
	}
	e.execute(context.Background(), k, job, func() {})

	got := getJob(t, f, job.ID)
	is.Equal(driver.StateDead, got.State)
	is.Equal("permanent", got.LastError)
}

func TestExecuteExhaustedBudgetDeadLetters(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	// MaxAttempts 1: the first failure exhausts the budget even though the
	// classifier says retry.
	job := leaseOne(t, f, "send", 1)
	k := Kind{Name: "send", Concurrency: 1,
		Handler:  func(context.Context, driver.Job) error { return errors.New("boom") },
		Classify: retryClassifier(Outcome{Kind: OutcomeRetry}),
	}
	e.execute(context.Background(), k, job, func() {})

	got := getJob(t, f, job.ID)
	is.Equal(driver.StateDead, got.State)
}

func TestExecuteExhaustedReportableStillDeadLetters(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	job := leaseOne(t, f, "send", 1)
	k := Kind{Name: "send", Concurrency: 1,
		Handler:  func(context.Context, driver.Job) error { return errors.New("loud") },
		Classify: retryClassifier(Outcome{Kind: OutcomeRetry, Reportable: true}),
	}
	e.execute(context.Background(), k, job, func() {})

	got := getJob(t, f, job.ID)
	is.Equal(driver.StateDead, got.State)
	is.Equal("loud", got.LastError)
}

func TestExecuteStaleLeaseTokenIsSwallowed(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())
	ctx := context.Background()

	job := leaseOne(t, f, "send", 5)
	// Settle the job out from under the executor: its token goes stale.
	is.NoError(f.Ack(ctx, job.ID, job.LeaseToken))

	k := Kind{Name: "send", Concurrency: 1, Handler: func(context.Context, driver.Job) error {
		return nil
	}}
	// Must not panic and must return; the not-found settle error is logged only.
	e.execute(ctx, k, job, func() {})

	failing := Kind{Name: "send", Concurrency: 1,
		Handler:  func(context.Context, driver.Job) error { return errors.New("boom") },
		Classify: retryClassifier(Outcome{Kind: OutcomeRetry}),
	}
	e.execute(ctx, failing, job, func() {})

	got := getJob(t, f, job.ID)
	is.Equal(driver.StateSucceeded, got.State, "the earlier Ack must stand; stale settles are ignored")
}

func TestExecuteSettlementSurvivesHandlerTimeout(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	job := leaseOne(t, f, "send", 5)
	k := Kind{Name: "send", Concurrency: 1, Timeout: 10 * time.Millisecond,
		Handler: func(ctx context.Context, _ driver.Job) error {
			<-ctx.Done() // block until the per-job timeout fires
			return ctx.Err()
		},
		Classify: retryClassifier(Outcome{Kind: OutcomeRetry}),
	}
	e.execute(context.Background(), k, job, func() {})

	// Settlement uses context.WithoutCancel, so the expired handler ctx must
	// not block the reschedule.
	got := getJob(t, f, job.ID)
	is.Equal(driver.StateScheduled, got.State)
	is.Contains(got.LastError, context.DeadlineExceeded.Error())
}

func TestExecuteHandlerPanicIsRecoveredAndRetried(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	job := leaseOne(t, f, "send", 5)
	k := Kind{Name: "send", Concurrency: 1,
		Handler:  func(context.Context, driver.Job) error { panic("kaboom") },
		Classify: retryClassifier(Outcome{Kind: OutcomeRetry}),
	}
	released := false
	is.NotPanics(func() { e.execute(context.Background(), k, job, func() { released = true }) },
		"a handler panic must never escape the executor")
	is.True(released, "the executor slot must be released after a panic")

	got := getJob(t, f, job.ID)
	is.Equal(driver.StateScheduled, got.State, "a recovered panic settles through the normal retry path")
	is.Contains(got.LastError, "panic")
	is.Contains(got.LastError, "kaboom")
}

func TestExecuteHandlerPanicExhaustedBudgetDeadLetters(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	job := leaseOne(t, f, "send", 1)
	k := Kind{Name: "send", Concurrency: 1,
		Handler:  func(context.Context, driver.Job) error { panic("kaboom") },
		Classify: retryClassifier(Outcome{Kind: OutcomeRetry}),
	}
	is.NotPanics(func() { e.execute(context.Background(), k, job, func() {}) })

	got := getJob(t, f, job.ID)
	is.Equal(driver.StateDead, got.State, "a panic with an exhausted budget dead-letters like any failure")
	is.Contains(got.LastError, "kaboom")
}

func TestExecuteReleaseIsCalledOnEveryPath(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	for _, handler := range []func(context.Context, driver.Job) error{
		func(context.Context, driver.Job) error { return nil },
		func(context.Context, driver.Job) error { return errors.New("boom") },
	} {
		job := leaseOne(t, f, "send", 5)
		released := false
		k := Kind{Name: "send", Concurrency: 1, Handler: handler,
			Classify: retryClassifier(Outcome{Kind: OutcomeRetry})}
		e.execute(context.Background(), k, job, func() { released = true })
		is.True(released)
	}
}
