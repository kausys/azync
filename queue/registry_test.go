package queue

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRegisterDuplicateKindFails(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())

	handler := func(context.Context, Job[testArgs]) error { return nil }
	is.NoError(Register(r.Worker(), handler))
	err := Register(r.Worker(), handler)
	is.Error(err)
	is.Contains(err.Error(), `queue: kind "queue.test" already registered`)
}

func TestRegisterAfterStartFails(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	is.NoError(Register(r.Worker(), func(context.Context, Job[testArgs]) error { return nil }))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	err := RegisterKind(r.Worker(), "late.kind", func(context.Context, RawJob) error { return nil })
	is.Error(err)
	is.Contains(err.Error(), "queue: cannot register after start")
}

func TestUndecodablePayloadDeadLettersWithoutHandler(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	var handlerRuns atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, Job[testArgs]) error {
		handlerRuns.Add(1)
		return nil
	}))

	// A payload that can never decode into testArgs (string vs object).
	id := uuid.New()
	_, err := f.Enqueue(context.Background(), driver.EnqueueParams{
		ID: id, Kind: "queue.test", Payload: json.RawMessage(`"not-an-object"`), MaxAttempts: 5,
	})
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return getJob(t, f, id).State == driver.StateDead
	}, 2*time.Second, 2*time.Millisecond, "an undecodable payload must abort straight to dead")

	got := getJob(t, f, id)
	is.Contains(got.LastError, "decode queue.test payload")
	is.Equal(int32(0), handlerRuns.Load(), "the typed handler must never see an undecodable payload")
	is.Equal(1, got.Attempt, "decode failure must not burn retries")
}

func TestRegisterKindPassesRawPayloadThrough(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	got := make(chan RawJob, 1)
	is.NoError(RegisterKind(r.Worker(), "raw.kind", func(_ context.Context, job RawJob) error {
		got <- job
		return nil
	}))

	id := uuid.New()
	payload := json.RawMessage(`{"anything":["goes",1,true]}`)
	_, err := f.Enqueue(context.Background(), driver.EnqueueParams{
		ID: id, Kind: "raw.kind", Payload: payload, MaxAttempts: 5,
		Meta: map[string]string{"a": "1"},
	})
	is.NoError(err)

	startWorker(t, r.Worker())
	select {
	case job := <-got:
		is.Equal(id, job.ID)
		is.Equal("raw.kind", job.Kind)
		is.JSONEq(string(payload), string(job.Payload), "the raw payload must pass through undecoded")
		is.Equal(1, job.Attempt)
		is.Equal(map[string]string{"a": "1"}, job.Meta)
	case <-time.After(2 * time.Second):
		t.Fatal("raw handler did not run")
	}
}

func TestWithMaxRetriesResolvesOnFirstLease(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithDefaultMaxRetries(25))

	done := make(chan struct{}, 1)
	is.NoError(Register(r.Worker(), func(context.Context, Job[testArgs]) error {
		done <- struct{}{}
		return nil
	}, WithMaxRetries(7)))

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "x"})
	is.NoError(err)
	is.Equal(25, getJob(t, f, res.ID).MaxAttempts, "enqueue stamps the runtime default")

	startWorker(t, r.Worker())
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not run")
	}
	is.Eventually(func() bool {
		got := getJob(t, f, res.ID)
		return got.State == driver.StateSucceeded && got.MaxAttempts == 7
	}, 2*time.Second, 2*time.Millisecond, "the registration budget must resolve durably on first lease")
}
