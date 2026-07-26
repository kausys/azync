package queue

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestEnqueueStampsDefaultsAndPayload(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithDefaultMaxRetries(9))

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "hello"})
	is.NoError(err)
	is.False(res.Deduplicated)

	job := getJob(t, f, res.ID)
	is.Equal("queue.test", job.Kind)
	is.Equal(driver.StatePending, job.State)
	is.Equal(9, job.MaxAttempts)
	is.JSONEq(`{"value":"hello"}`, string(job.Payload))
}

func TestAtWinsOverDelay(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f)

	at := clk.Now().Add(30 * time.Minute)
	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "x"}, Delay(2*time.Hour), At(at))
	is.NoError(err)

	job := getJob(t, f, res.ID)
	is.Equal(driver.StateScheduled, job.State)
	is.True(job.RunAt.Equal(at), "At must win over Delay: RunAt=%v want %v", job.RunAt, at)
}

func TestDelaySchedulesRelativeToBackendClock(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f)

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "x"}, Delay(time.Hour))
	is.NoError(err)

	job := getJob(t, f, res.ID)
	is.Equal(driver.StateScheduled, job.State)
	is.True(job.RunAt.Equal(clk.Now().Add(time.Hour)))
}

func TestIdempotencyKeyDeduplicates(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)
	ctx := context.Background()

	first, err := r.Producer().Enqueue(ctx, testArgs{Value: "once"}, IdempotencyKey("only-one"))
	is.NoError(err)
	is.False(first.Deduplicated)

	second, err := r.Producer().Enqueue(ctx, testArgs{Value: "twice"}, IdempotencyKey("only-one"))
	is.NoError(err)
	is.True(second.Deduplicated)

	_, total, err := f.ListJobs(ctx, driver.SourceQueue, driver.JobFilter{}, 0, 10)
	is.NoError(err)
	is.Equal(int64(1), total)
}

func TestMetaIsStored(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "x"},
		Meta("tenant", "t1"), Meta("origin", "test"))
	is.NoError(err)

	job := getJob(t, f, res.ID)
	is.Equal(map[string]string{"tenant": "t1", "origin": "test"}, job.Meta)
}

// withValidSpanContext returns ctx carrying a valid, sampled span context
// built directly from the W3C spec's example trace/span IDs — no SDK
// TracerProvider needed, just the otel/trace API.
func withValidSpanContext(ctx context.Context) context.Context {
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	return trace.ContextWithSpanContext(ctx, sc)
}

func TestEnqueueInjectsTraceparentWhenCtxCarriesASpan(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	ctx := withValidSpanContext(context.Background())
	res, err := r.Producer().Enqueue(ctx, testArgs{Value: "x"})
	is.NoError(err)

	job := getJob(t, f, res.ID)
	is.Contains(job.Meta, "traceparent")
	is.Contains(job.Meta["traceparent"], "4bf92f3577b34da6a3ce929d0e0e4736")
}

func TestEnqueueOmitsTraceparentWithoutASpan(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	res, err := r.Producer().Enqueue(context.Background(), testArgs{Value: "x"})
	is.NoError(err)

	job := getJob(t, f, res.ID)
	is.NotContains(job.Meta, "traceparent")
}

func TestMaxRetriesExplicitSurvivesDivergentWorkerDefault(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithDefaultMaxRetries(25))
	ctx := context.Background()

	res, err := r.Producer().Enqueue(ctx, testArgs{Value: "x"}, MaxRetries(2))
	is.NoError(err)
	is.Equal(2, getJob(t, f, res.ID).MaxAttempts)

	// A first lease carrying a different runtime default must not override the
	// explicit budget.
	jobs, err := f.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{
		Kind: "queue.test", Limit: 1, Lease: time.Minute, DefaultMaxAttempts: 99, OverrideDefault: true,
	})
	is.NoError(err)
	is.Len(jobs, 1)
	is.Equal(2, jobs[0].MaxAttempts)
}

// txFake adds a trivial driver.TxStore[struct{}] over the fake so the positive
// TxProducer path can be exercised without a transactional backend.
type txFake struct {
	*drivertest.Fake
}

func (f *txFake) EnqueueTx(ctx context.Context, _ struct{}, p driver.EnqueueParams) (bool, error) {
	return f.Enqueue(ctx, p)
}

func (f *txFake) PublishTx(ctx context.Context, _ struct{}, p driver.PublishParams) (int, error) {
	return f.Publish(ctx, p)
}

var _ driver.TxStore[struct{}] = (*txFake)(nil)

func TestTxProducerRequiresTxStoreDriver(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake() // implements no TxStore
	r := newTestRuntime(t, f)

	_, err := TxProducer[struct{}](r)
	is.Error(err)
	is.Contains(err.Error(), "does not support transactional enqueues")
	is.Contains(err.Error(), "struct {}")
}

func TestTxProducerEnqueuesThroughTx(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := &txFake{Fake: drivertest.NewFake()}
	core, err := azync.New(f, azync.WithLogger(discardLogger()))
	is.NoError(err)
	r, err := New(core, fastOptions()...)
	is.NoError(err)

	tp, err := TxProducer[struct{}](r)
	is.NoError(err)

	res, err := tp.EnqueueTx(context.Background(), struct{}{}, testArgs{Value: "in-tx"}, MaxRetries(3))
	is.NoError(err)
	is.False(res.Deduplicated)

	job := getJob(t, f.Fake, res.ID)
	is.Equal("queue.test", job.Kind)
	is.Equal(3, job.MaxAttempts)
	is.JSONEq(`{"value":"in-tx"}`, string(job.Payload))
}

func TestEnqueueMarshalFailure(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	_, err := r.Producer().Enqueue(context.Background(), badArgs{})
	is.Error(err)
	is.Contains(err.Error(), "marshal")
}

// badArgs cannot marshal (channels are not JSON-serializable).
type badArgs struct {
	Ch chan int `json:"ch"`
}

func (badArgs) Kind() string { return "queue.bad" }
