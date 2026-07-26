package workflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestShutdownDrainLetsInFlightOperationSettle proves Start waits for an
// in-flight Operation to finish (using context.WithoutCancel-style
// isolation from ctx's cancellation — see dequeueOperations/dequeueWorkflows
// deriving process*Job's context from Start's own long-lived jobsCtx, not
// from ctx) instead of abandoning it the instant ctx is cancelled.
func TestShutdownDrainLetsInFlightOperationSettle(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithLeaseTTL(time.Minute), WithShutdownDrain(2*time.Second))

	started := make(chan struct{})
	release := make(chan struct{})
	RegisterOperation(r.Worker(), "slow", "1", func(context.Context, struct{}) (string, error) {
		close(started)
		<-release
		return "done", nil
	})
	RegisterWorkflow(r.Worker(), "wf-drain", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "slow", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	res, err := r.Client().Start(ctx, "wf-drain", "1", nil)
	is.NoError(err)
	runOneWorkflowTask(t, r.Worker()) // schedules the operation, parks

	workerCtx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- r.Worker().Start(workerCtx) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not start")
	}

	cancel() // begin shutdown while the operation is still running
	select {
	case err := <-startDone:
		t.Fatalf("Start returned before the in-flight operation settled (err=%v)", err)
	case <-time.After(50 * time.Millisecond):
		// still draining, as expected
	}
	close(release) // let the operation finish

	select {
	case err := <-startDone:
		is.NoError(err)
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after the operation settled")
	}

	view := drainUntilTerminal(t, r, res.WorkflowID, 10)
	is.Equal(driver.WorkflowSucceeded, view.State)
	is.JSONEq(`"done"`, string(view.Result))
}

// TestConcurrencyProcessesOperationsInParallel proves WithConcurrency(2)
// actually runs two Operation passes at once: both handlers rendezvous on a
// shared WaitGroup, which can only unblock if both are running
// simultaneously — a concurrency of 1 would deadlock here.
func TestConcurrencyProcessesOperationsInParallel(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f, WithLeaseTTL(time.Minute), WithConcurrency(2))

	var wg sync.WaitGroup
	wg.Add(2)
	release := make(chan struct{})
	RegisterOperation(r.Worker(), "par", "1", func(context.Context, struct{}) (string, error) {
		wg.Done()
		wg.Wait() // only unblocks once both passes are running concurrently
		<-release
		return "ok", nil
	})
	RegisterWorkflow(r.Worker(), "wf-par", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "par", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	res1, err := r.Client().Start(ctx, "wf-par", "1", nil)
	is.NoError(err)
	res2, err := r.Client().Start(ctx, "wf-par", "1", nil, WithWorkflowID(uuid.New()))
	is.NoError(err)

	workerCtx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- r.Worker().Start(workerCtx) }()
	t.Cleanup(func() {
		cancel()
		<-startDone
	})

	bothStarted := make(chan struct{})
	go func() { wg.Wait(); close(bothStarted) }()
	select {
	case <-bothStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("both operations did not start concurrently (concurrency=2 not honored)")
	}
	close(release)

	view1 := drainUntilTerminal(t, r, res1.WorkflowID, 30)
	view2 := drainUntilTerminal(t, r, res2.WorkflowID, 30)
	is.Equal(driver.WorkflowSucceeded, view1.State)
	is.Equal(driver.WorkflowSucceeded, view2.State)
}

// TestHardDrainTimeoutAbandonsStuckOperation proves that an Operation which
// ignores cancellation entirely does not hang Start forever: past
// finalDrainGrace, Start abandons the wait and returns nil anyway (the
// abandoned pass is leaked, not killed — Go cannot force-stop a goroutine —
// and its job recovers via the lease reaper once the lease expires).
func TestHardDrainTimeoutAbandonsStuckOperation(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()

	original := finalDrainGrace
	finalDrainGrace = 50 * time.Millisecond
	t.Cleanup(func() { finalDrainGrace = original })

	r := newTestRuntime(t, f, WithLeaseTTL(time.Minute), WithShutdownDrain(10*time.Millisecond))

	started := make(chan struct{})
	stuck := make(chan struct{})
	RegisterOperation(r.Worker(), "stuck", "1", func(context.Context, struct{}) (string, error) {
		close(started)
		<-stuck // never released within the test: ignores cancellation entirely
		return "", nil
	})
	RegisterWorkflow(r.Worker(), "wf-stuck", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "stuck", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})
	t.Cleanup(func() { close(stuck) }) // release the leaked goroutine so the test process can exit cleanly

	_, err := r.Client().Start(ctx, "wf-stuck", "1", nil)
	is.NoError(err)
	runOneWorkflowTask(t, r.Worker()) // schedules the operation, parks

	workerCtx, cancel := context.WithCancel(context.Background())
	startDone := make(chan error, 1)
	go func() { startDone <- r.Worker().Start(workerCtx) }()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("operation did not start")
	}
	cancel()

	select {
	case err := <-startDone:
		is.NoError(err, "Start must return even after abandoning a stuck operation")
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return within the drain budget + hard grace period")
	}
}
