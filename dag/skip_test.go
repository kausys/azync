package dag

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

// submitTask is the "maybe nothing to do" shape: the handler checks remote
// state and skips when the work already happened.
type submitTask struct{}

func (submitTask) Kind() string { return "skip.submit" }

type actTask struct{}

func (actTask) Kind() string { return "skip.act" }

// TestSkipSettlesTaskAndDownstreamRuns proves the Skip sentinel end to end:
// the handler returns Skip, the task lands terminal skipped (visible as such
// in Manager.Tasks), its dependent still runs, the workflow succeeds, and a
// downstream ResultOf on the skipped task reports ErrTaskSkipped instead of
// a silent zero value.
func TestSkipSettlesTaskAndDownstreamRuns(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	sawSkipped := make(chan error, 1)
	is.NoError(Register(r.Worker(), func(context.Context, submitTask) (None, error) {
		return None{}, Skip("already VERIFIED")
	}))
	is.NoError(Register(r.Worker(), func(ctx context.Context, _ actTask) (None, error) {
		_, err := ResultOf[string](ctx, "submit")
		sawSkipped <- err
		return None{}, nil
	}))

	res, err := r.Client().Run(context.Background(),
		Define("skip-flow").
			Task("submit", submitTask{}).
			Task("act", actTask{}, After("submit")))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return dagState(t, f, res.ID) == driver.DAGSucceeded
	}, 2*time.Second, 2*time.Millisecond, "a skipped upstream must not block completion")

	skipped := taskByKey(t, f, res.ID, "submit")
	is.Equal(driver.StateSkipped, skipped.State)
	is.Contains(skipped.LastError, "already VERIFIED")

	select {
	case err := <-sawSkipped:
		is.ErrorIs(err, ErrTaskSkipped, "ResultOf on a skipped dependency must be distinguishable")
	case <-time.After(2 * time.Second):
		t.Fatal("the downstream task never ran")
	}

	// Manager surfaces: the state is first-class, the result gated getter
	// reports the skip too.
	tasks, err := r.Manager().Tasks(context.Background(), res.ID)
	is.NoError(err)
	var found bool
	for _, tv := range tasks {
		if tv.Key == "submit" {
			found = true
			is.Equal(TaskSkipped, tv.State)
			is.False(tv.HasResult)
		}
	}
	is.True(found)
	_, err = r.Manager().TaskResult(context.Background(), res.ID, "submit")
	is.ErrorIs(err, ErrTaskSkipped)
}

// TestManagerTaskResultReadsOneResult pins the gated single-task getter: the
// payload for a succeeded task, (nil, nil) for succeeded-without-result, and
// not-found for a key that never settled.
func TestManagerTaskResultReadsOneResult(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	is.NoError(Register(r.Worker(), func(context.Context, submitTask) (map[string]string, error) {
		return map[string]string{"provider_id": "cus_42"}, nil
	}))
	is.NoError(Register(r.Worker(), func(context.Context, actTask) (None, error) {
		return None{}, nil
	}))

	res, err := r.Client().Run(context.Background(),
		Define("results").
			Task("submit", submitTask{}).
			Task("act", actTask{}, After("submit")))
	is.NoError(err)
	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return dagState(t, f, res.ID) == driver.DAGSucceeded
	}, 2*time.Second, 2*time.Millisecond)

	raw, err := r.Manager().TaskResult(context.Background(), res.ID, "submit")
	is.NoError(err)
	is.JSONEq(`{"provider_id":"cus_42"}`, string(raw))

	raw, err = r.Manager().TaskResult(context.Background(), res.ID, "act")
	is.NoError(err)
	is.Nil(raw, "a None handler settles without a result")

	_, err = r.Manager().TaskResult(context.Background(), res.ID, "ghost")
	is.True(IsNotFound(err), "an unknown key is not-found")
}

// TestSignalByKeyResolvesLiveRunAndDelivers pins the webhook-handler shape:
// signal by (definition, idempotency key) with no run-UUID bookkeeping, and
// not-found once the run settled (a late webhook is late, not wrong).
func TestSignalByKeyResolvesLiveRunAndDelivers(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	res, err := r.Client().Run(context.Background(),
		Define("bykey").WaitSignal("approval"),
		WithIdempotencyKey("ws1:kyc42"))
	is.NoError(err)

	is.NoError(r.Client().SignalByKey(context.Background(),
		"bykey", "ws1:kyc42", "approval", map[string]string{"by": "ops"},
		WithMessageID("wh-9")))
	g := taskByKey(t, f, res.ID, "approval")
	is.Equal(driver.StateSucceeded, g.State)
	is.JSONEq(`{"by":"ops"}`, string(g.Result))

	// Once terminal, the key is freed: a late webhook is not-found.
	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return dagState(t, f, res.ID) == driver.DAGSucceeded
	}, 2*time.Second, 2*time.Millisecond)
	err = r.Client().SignalByKey(context.Background(),
		"bykey", "ws1:kyc42", "approval", nil)
	is.True(IsNotFound(err), "a late webhook resolves nothing: the run settled")
}
