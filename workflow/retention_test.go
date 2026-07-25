package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

func TestVacuumWorkflowsRemovesTerminalPastRetention(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	clk := drivertest.NewManualClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	f.Clock = clk
	r := newTestRuntime(t, f, WithRetention(time.Hour))

	RegisterWorkflow(r.Worker(), "wf-vac", "1", func(ctx Context, _ struct{}) (struct{}, error) {
		return struct{}{}, nil
	})

	res, err := r.Client().Start(ctx, "wf-vac", "1", nil)
	is.NoError(err)
	drainUntilTerminal(t, r, res.WorkflowID, 10)

	removed, err := f.VacuumWorkflows(ctx, 0)
	is.NoError(err)
	is.Zero(removed)

	clk.Advance(2 * time.Hour)
	removed, err = f.VacuumWorkflows(ctx, time.Hour)
	is.NoError(err)
	is.Equal(int64(1), removed)

	_, err = r.Manager().Get(ctx, res.WorkflowID)
	is.True(IsNotFound(err))
}

func TestVacuumCompletedExemptsWorkflowJobs(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	RegisterOperation(r.Worker(), "noop", "1", func(_ context.Context, _ struct{}) (string, error) {
		return "x", nil
	})
	RegisterWorkflow(r.Worker(), "wf-exempt", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "noop", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	res, err := r.Client().Start(ctx, "wf-exempt", "1", nil)
	is.NoError(err)
	view := drainUntilTerminal(t, r, res.WorkflowID, 10)
	is.Equal(driver.WorkflowSucceeded, view.State)

	time.Sleep(20 * time.Millisecond)
	removed, err := f.VacuumCompleted(ctx, driver.SourceWorkflow, time.Millisecond)
	is.NoError(err)
	is.Zero(removed)

	_, err = f.GetWorkflowExecution(ctx, res.WorkflowID)
	is.NoError(err)
}

func TestOperationHeartbeatSurvivesShortLease(t *testing.T) {
	is := require.New(t)
	ctx := context.Background()
	f := drivertest.NewFake()
	// Lease shorter than the Operation; heartbeats must ExtendLease.
	r := newTestRuntime(t, f,
		WithLeaseTTL(80*time.Millisecond),
		WithOperationTimeout(5*time.Second),
	)

	RegisterOperation(r.Worker(), "slow", "1", func(ctx context.Context, _ struct{}) (string, error) {
		select {
		case <-time.After(250 * time.Millisecond):
			return "ok", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	RegisterWorkflow(r.Worker(), "wf-hb", "1", func(ctx Context, _ struct{}) (string, error) {
		var out string
		if err := ExecuteOperation(ctx, "slow", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	res, err := r.Client().Start(ctx, "wf-hb", "1", nil)
	is.NoError(err)
	view := drainUntilTerminal(t, r, res.WorkflowID, 40)
	is.Equal(driver.WorkflowSucceeded, view.State)
	is.JSONEq(`"ok"`, string(view.Result))
}
