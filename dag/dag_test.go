package dag

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// newTestRuntime composes a runtime over a fake store with fast tuning.
func newTestRuntime(t *testing.T, f *drivertest.Fake, opts ...Option) *Runtime {
	t.Helper()
	is := require.New(t)
	core, err := azync.New(f, azync.WithLogger(discardLogger()))
	is.NoError(err)
	r, err := New(core, append(fastOptions(), opts...)...)
	is.NoError(err)
	return r
}

func fastOptions() []Option {
	return []Option{
		WithFetchCooldown(time.Millisecond),
		WithFetchPollInterval(5 * time.Millisecond),
		WithIdleBackoffMax(10 * time.Millisecond),
		WithLeaseTTL(time.Minute),
		WithShutdownDrain(2 * time.Second),
		withSchedulerIntervals(2*time.Millisecond, time.Hour),
		withMaintenanceIntervals(2*time.Millisecond, time.Hour),
	}
}

// startWorker runs the worker and returns a stop func that cancels it and
// waits for Start to return its error.
func startWorker(t *testing.T, w *Worker) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Start(ctx) }()
	var result error
	var once atomic.Bool
	stop := func() error {
		if once.CompareAndSwap(false, true) {
			cancel()
			result = <-done
		}
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func awaitReady(t *testing.T, w *Worker) {
	t.Helper()
	select {
	case <-w.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not become ready")
	}
}

// getDAG reads one workflow header straight from the fake.
func getDAG(t *testing.T, f *drivertest.Fake, id uuid.UUID) driver.DAGView {
	t.Helper()
	is := require.New(t)
	view, err := f.GetDAG(context.Background(), id)
	is.NoError(err)
	return *view
}

// taskByKey reads one task job of a workflow straight from the fake.
func taskByKey(t *testing.T, f *drivertest.Fake, dagID uuid.UUID, key string) driver.Job {
	t.Helper()
	is := require.New(t)
	jobs, err := f.DAGTasks(context.Background(), dagID)
	is.NoError(err)
	for _, j := range jobs {
		if j.TaskKey == key {
			return j
		}
	}
	t.Fatalf("workflow %s has no task %q", dagID, key)
	return driver.Job{}
}

// dagState polls the current state of one workflow.
func dagState(t *testing.T, f *drivertest.Fake, id uuid.UUID) driver.DAGState {
	t.Helper()
	return getDAG(t, f, id).State
}

func TestNewRejectsNilCore(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	_, err := New(nil)
	is.Error(err)
	is.Contains(err.Error(), "core is nil")
}

// storeOnly masks every optional capability of the fake (DAGStore,
// Notifier, LeaderElector), leaving the bare driver.Store.
type storeOnly struct {
	driver.Store
}

func TestNewRejectsDriverWithoutWorkflowSupport(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	core, err := azync.New(storeOnly{Store: drivertest.NewFake()}, azync.WithLogger(discardLogger()))
	is.NoError(err)

	_, err = New(core)
	is.Error(err)
	is.Contains(err.Error(), "does not support dags")
}
