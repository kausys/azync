package kernel_test

import (
	"encoding/json"
	"testing"

	"github.com/kausys/azync/workflow/kernel"

	"github.com/stretchr/testify/require"
)

func TestHistoryAppendIsMonotonic(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	var h kernel.History
	e1 := h.Append(kernel.EventWorkflowStarted, json.RawMessage(`{}`))
	e2 := h.Append(kernel.EventOperationScheduled, json.RawMessage(`{"name":"op"}`))
	is.Equal(int64(1), e1.Seq)
	is.Equal(int64(2), e2.Seq)
	is.Equal(int64(3), h.NextSeq())
}

func TestCursorReplayDeterministic(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	var h kernel.History
	h.Append(kernel.EventWorkflowStarted, nil)
	h.Append(kernel.EventOperationCompleted, json.RawMessage(`{"v":1}`))

	c := kernel.NewCursor(&h)
	ev, err := c.Expect(kernel.EventWorkflowStarted)
	is.NoError(err)
	is.Equal(int64(1), ev.Seq)

	ev, ok, err := c.TakeOrCommand(kernel.EventOperationCompleted)
	is.NoError(err)
	is.True(ok)
	is.JSONEq(`{"v":1}`, string(ev.Payload))

	_, ok, err = c.TakeOrCommand(kernel.EventTimerFired)
	is.NoError(err)
	is.False(ok, "exhausted history must yield command opportunity")
}

func TestCursorMismatchIsReplayError(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	var h kernel.History
	h.Append(kernel.EventSignalReceived, nil)
	c := kernel.NewCursor(&h)
	_, err := c.Expect(kernel.EventTimerFired)
	is.Error(err)
	var re kernel.ReplayError
	is.ErrorAs(err, &re)
}
