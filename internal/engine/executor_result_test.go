package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// These tests cover the two engine seams the workflow runtime relies on: the
// injected success acker that receives the handler's result, and the snooze
// outcome that parks a job without consuming its retry budget.

func TestExecuteResultReachesInjectedAcker(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()

	var gotID, gotToken uuid.UUID
	var gotResult json.RawMessage
	e := New(Config{
		Store: f, Source: driver.SourceQueue, Logger: discardLogger(), Settings: testSettings(),
		Acker: func(ctx context.Context, id, leaseToken uuid.UUID, result json.RawMessage) error {
			gotID, gotToken, gotResult = id, leaseToken, result
			return f.Ack(ctx, id, leaseToken)
		},
	})

	job := leaseOne(t, f, "send", 5)
	k := Kind{Name: "send", Concurrency: 1, Handler: func(context.Context, driver.Job) (json.RawMessage, error) {
		return json.RawMessage(`{"out":1}`), nil
	}}
	e.execute(context.Background(), k, job, func() {})

	is.Equal(job.ID, gotID)
	is.Equal(job.LeaseToken, gotToken)
	is.JSONEq(`{"out":1}`, string(gotResult), "the handler result reaches the injected acker verbatim")
	is.Equal(driver.StateSucceeded, getJob(t, f, job.ID).State)
}

func TestExecuteDefaultAckerAcksIgnoringResult(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings()) // no Acker: the store.Ack default

	job := leaseOne(t, f, "send", 5)
	k := Kind{Name: "send", Concurrency: 1, Handler: func(context.Context, driver.Job) (json.RawMessage, error) {
		return json.RawMessage(`{"ignored":true}`), nil
	}}
	e.execute(context.Background(), k, job, func() {})

	got := getJob(t, f, job.ID)
	is.Equal(driver.StateSucceeded, got.State, "the default acker completes through store.Ack")
	is.Nil(got.Result, "the default acker discards the handler result")
}

func TestExecuteInjectedAckerNotFoundIsSwallowed(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := New(Config{
		Store: f, Source: driver.SourceQueue, Logger: discardLogger(), Settings: testSettings(),
		Acker: func(context.Context, uuid.UUID, uuid.UUID, json.RawMessage) error {
			return driver.NewNotFound("ack task result")
		},
	})

	job := leaseOne(t, f, "send", 5)
	k := Kind{Name: "send", Concurrency: 1, Handler: func(context.Context, driver.Job) (json.RawMessage, error) {
		return nil, nil
	}}
	is.NotPanics(func() { e.execute(context.Background(), k, job, func() {}) },
		"a fenced acker is the expected race outcome, logged and swallowed")
}

func TestExecuteSnoozeParksScheduledWithoutConsumingAttempt(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	e := newTestEngine(f, testSettings())

	job := leaseOne(t, f, "poll", 5)
	is.Equal(1, job.Attempt)
	delay := 42 * time.Second
	k := Kind{Name: "poll", Concurrency: 1,
		Handler:  func(context.Context, driver.Job) (json.RawMessage, error) { return nil, errors.New("not ready") },
		Classify: retryClassifier(Outcome{Kind: OutcomeSnooze, Delay: delay}),
	}
	e.execute(context.Background(), k, job, func() {})

	got := getJob(t, f, job.ID)
	is.Equal(driver.StateScheduled, got.State)
	is.Equal(clk.Now().Add(delay), got.RunAt, "the snooze delay resolves against the store clock")
	is.Equal(0, got.Attempt, "snooze hands back the attempt: the budget is not consumed")
	attempts, err := f.JobAttempts(context.Background(), driver.SourceQueue, job.ID)
	is.NoError(err)
	is.Empty(attempts, "snooze records no attempt history")
}

func TestExecuteSnoozeWinsOverExhaustedBudget(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())

	// MaxAttempts 1: a plain retry would dead-letter, but snooze never counts
	// against the budget, so the job must go back to scheduled.
	job := leaseOne(t, f, "poll", 1)
	k := Kind{Name: "poll", Concurrency: 1,
		Handler:  func(context.Context, driver.Job) (json.RawMessage, error) { return nil, errors.New("not ready") },
		Classify: retryClassifier(Outcome{Kind: OutcomeSnooze, Delay: time.Minute}),
	}
	e.execute(context.Background(), k, job, func() {})

	got := getJob(t, f, job.ID)
	is.Equal(driver.StateScheduled, got.State, "snooze must win over budget exhaustion")
	is.Equal(0, got.Attempt)
}

func TestExecuteSnoozeStaleTokenIsSwallowed(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	e := newTestEngine(f, testSettings())
	ctx := context.Background()

	job := leaseOne(t, f, "poll", 5)
	is.NoError(f.Ack(ctx, job.ID, job.LeaseToken)) // token goes stale

	k := Kind{Name: "poll", Concurrency: 1,
		Handler:  func(context.Context, driver.Job) (json.RawMessage, error) { return nil, errors.New("not ready") },
		Classify: retryClassifier(Outcome{Kind: OutcomeSnooze, Delay: time.Minute}),
	}
	is.NotPanics(func() { e.execute(ctx, k, job, func() {}) })
	is.Equal(driver.StateSucceeded, getJob(t, f, job.ID).State, "the earlier Ack must stand")
}
