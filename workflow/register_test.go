package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type regArgs struct {
	V string `json:"v"`
}

func (regArgs) Kind() string { return "reg.typed" }

type regDecode struct {
	N int `json:"n"`
}

func (regDecode) Kind() string { return "reg.decode" }

type regNone struct{}

func (regNone) Kind() string { return "reg.none" }

func TestRegisterDuplicateKindFails(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	handler := func(context.Context, regArgs) (None, error) { return None{}, nil }
	is.NoError(Register(r.Worker(), handler))
	err := Register(r.Worker(), handler)
	is.Error(err)
	is.Contains(err.Error(), `workflow: kind "reg.typed" already registered`)
}

func TestRegisterAfterStartFails(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	is.NoError(Register(r.Worker(), func(context.Context, regArgs) (None, error) { return None{}, nil }))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	err := RegisterKind(r.Worker(), "late.kind",
		func(context.Context, json.RawMessage) (json.RawMessage, error) { return nil, nil })
	is.Error(err)
	is.Contains(err.Error(), "workflow: cannot register after start")
}

func TestRegisterRejectsReservedKinds(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	err := RegisterKind(r.Worker(), "$custom",
		func(context.Context, json.RawMessage) (json.RawMessage, error) { return nil, nil })
	is.Error(err)
	is.Contains(err.Error(), "reserved")
	is.Contains(err.Error(), "$")
}

func TestUndecodablePayloadDeadLettersWithoutHandler(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	var handlerRuns atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, regDecode) (None, error) {
		handlerRuns.Add(1)
		return None{}, nil
	}))

	// A payload that can never decode into regDecode (string vs object) reaches
	// the store directly, as if written by a producer on another schema version.
	id := uuid.New()
	_, _, err := f.CreateWorkflow(context.Background(), driver.WorkflowParams{
		ID:   id,
		Name: "decode",
		Tasks: []driver.WorkflowTask{
			{Key: "t", Kind: "reg.decode", Payload: json.RawMessage(`"not-an-object"`), MaxAttempts: 5},
		},
	})
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return taskByKey(t, f, id, "t").State == driver.StateDead
	}, 2*time.Second, 2*time.Millisecond, "an undecodable payload must abort straight to dead")

	got := taskByKey(t, f, id, "t")
	is.Contains(got.LastError, "decode reg.decode payload")
	is.Equal(int32(0), handlerRuns.Load(), "the typed handler must never see an undecodable payload")
	is.Equal(1, got.Attempt, "a decode failure must not burn the retry budget")
}

func TestRegisterKindPersistsRawResultAndExposesContext(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	type seen struct {
		id      uuid.UUID
		key     string
		payload json.RawMessage
	}
	got := make(chan seen, 1)
	is.NoError(RegisterKind(r.Worker(), "reg.raw",
		func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
			got <- seen{id: WorkflowID(ctx), key: TaskKey(ctx), payload: payload}
			return json.RawMessage(`{"ok":true}`), nil
		}))

	id := uuid.New()
	_, _, err := f.CreateWorkflow(context.Background(), driver.WorkflowParams{
		ID:   id,
		Name: "raw",
		Tasks: []driver.WorkflowTask{
			{Key: "t", Kind: "reg.raw", Payload: json.RawMessage(`{"anything":[1,true]}`), MaxAttempts: 5},
		},
	})
	is.NoError(err)

	startWorker(t, r.Worker())
	select {
	case s := <-got:
		is.Equal(id, s.id, "the raw handler reads the workflow id from ctx")
		is.Equal("t", s.key)
		is.JSONEq(`{"anything":[1,true]}`, string(s.payload), "the payload passes through undecoded")
	case <-time.After(3 * time.Second):
		t.Fatal("the raw handler never ran")
	}

	is.Eventually(func() bool {
		return taskByKey(t, f, id, "t").State == driver.StateSucceeded
	}, 2*time.Second, 2*time.Millisecond)
	results, err := f.TaskResults(context.Background(), id, nil)
	is.NoError(err)
	is.JSONEq(`{"ok":true}`, string(results["t"]), "the raw result is persisted verbatim")
}

func TestReportableDiesWhenBudgetExhausts(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	clk := drivertest.NewManualClock(time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC))
	f := drivertest.NewFake()
	f.Clock = clk
	r := newTestRuntime(t, f)

	var runs atomic.Int32
	is.NoError(Register(r.Worker(), func(context.Context, regArgs) (None, error) {
		runs.Add(1)
		return None{}, Reportable(errors.New("keeps failing"))
	}, WithMaxRetries(2)))

	res, err := r.Client().Run(context.Background(), Define("reportable-wf").Task("t", regArgs{V: "x"}))
	is.NoError(err)

	startWorker(t, r.Worker())
	// Attempt 1 fails -> scheduled at now+Backoff(1); make it due.
	is.Eventually(func() bool {
		return taskByKey(t, f, res.ID, "t").State == driver.StateScheduled
	}, 2*time.Second, 2*time.Millisecond)
	clk.Advance(time.Minute)

	// Attempt 2 exhausts the budget: dead.
	is.Eventually(func() bool {
		return taskByKey(t, f, res.ID, "t").State == driver.StateDead
	}, 2*time.Second, 2*time.Millisecond)
	is.Equal(int32(2), runs.Load(), "Reportable must still retry like Retry, not abort early")
}

func TestNoneResultPersistsNothing(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	is.NoError(Register(r.Worker(), func(context.Context, regNone) (None, error) { return None{}, nil }))

	res, err := r.Client().Run(context.Background(), Define("none").Task("t", regNone{}))
	is.NoError(err)

	startWorker(t, r.Worker())
	is.Eventually(func() bool {
		return workflowState(t, f, res.ID) == driver.WorkflowSucceeded
	}, 2*time.Second, 2*time.Millisecond)

	tasks, err := r.Manager().Tasks(context.Background(), res.ID)
	is.NoError(err)
	is.Len(tasks, 1)
	is.False(tasks[0].HasResult, "a None-returning handler persists no durable result")
}
