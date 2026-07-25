// Package kernel is the pure in-memory history/command/replay engine for
// workflow-as-code. It has no I/O and no driver dependency: persistence and
// workers sit above it.
package kernel

import (
	"encoding/json"
	"fmt"
)

// EventType classifies a history event.
type EventType string

const (
	EventWorkflowStarted   EventType = "WorkflowStarted"
	EventWorkflowCompleted EventType = "WorkflowCompleted"
	EventWorkflowFailed    EventType = "WorkflowFailed"
	EventWorkflowCancelled EventType = "WorkflowCancelled"
	EventWorkflowSuspended EventType = "WorkflowSuspended"

	EventOperationScheduled EventType = "OperationScheduled"
	EventOperationCompleted EventType = "OperationCompleted"
	EventOperationFailed    EventType = "OperationFailed"
	EventTimerStarted       EventType = "TimerStarted"
	EventTimerFired         EventType = "TimerFired"
	EventSignalReceived     EventType = "SignalReceived"
	EventMarkerRecorded     EventType = "MarkerRecorded"
)

// Event is one append-only history record.
type Event struct {
	Seq     int64
	Type    EventType
	Payload json.RawMessage
}

// Command is a side-effect request produced by replay when history is exhausted
// for a given workflow decision point.
type CommandType string

const (
	CommandScheduleOperation CommandType = "ScheduleOperation"
	CommandStartTimer        CommandType = "StartTimer"
	CommandCompleteWorkflow  CommandType = "CompleteWorkflow"
	CommandFailWorkflow      CommandType = "FailWorkflow"
	CommandSuspendWorkflow   CommandType = "SuspendWorkflow"
)

// Command is emitted by the workflow function during replay when it reaches a
// point that needs a new durable effect.
type Command struct {
	Type    CommandType
	Payload json.RawMessage
}

// History is an append-only ordered event log.
type History struct {
	Events []Event
}

// NextSeq returns the next EventSeq to append.
func (h *History) NextSeq() int64 {
	if len(h.Events) == 0 {
		return 1
	}
	return h.Events[len(h.Events)-1].Seq + 1
}

// Append adds an event with the next sequence number.
func (h *History) Append(typ EventType, payload json.RawMessage) Event {
	ev := Event{Seq: h.NextSeq(), Type: typ, Payload: payload}
	h.Events = append(h.Events, ev)
	return ev
}

// Cursor walks history during replay.
type Cursor struct {
	h   *History
	idx int
}

// NewCursor starts at the beginning of h.
func NewCursor(h *History) *Cursor { return &Cursor{h: h} }

// Next returns the next event, or false when the cursor has caught up (the
// workflow must then emit commands).
func (c *Cursor) Next() (Event, bool) {
	if c.idx >= len(c.h.Events) {
		return Event{}, false
	}
	ev := c.h.Events[c.idx]
	c.idx++
	return ev, true
}

// Peek returns the next event without advancing.
func (c *Cursor) Peek() (Event, bool) {
	if c.idx >= len(c.h.Events) {
		return Event{}, false
	}
	return c.h.Events[c.idx], true
}

// ReplayError is a non-deterministic or corrupt-history failure.
type ReplayError struct {
	Msg string
}

func (e ReplayError) Error() string { return "workflow/kernel: " + e.Msg }

// Expect consumes the next event and requires typ. On mismatch or exhaustion
// it returns a ReplayError (exhaustion means the caller should schedule a
// command instead — use TakeOrCommand).
func (c *Cursor) Expect(typ EventType) (Event, error) {
	ev, ok := c.Next()
	if !ok {
		return Event{}, ReplayError{Msg: fmt.Sprintf("expected %s, history exhausted", typ)}
	}
	if ev.Type != typ {
		return Event{}, ReplayError{Msg: fmt.Sprintf("expected %s, got %s at seq %d", typ, ev.Type, ev.Seq)}
	}
	return ev, nil
}

// TakeOrCommand returns the next matching event, or ok=false when history is
// exhausted so the caller can emit a command.
func (c *Cursor) TakeOrCommand(typ EventType) (Event, bool, error) {
	ev, ok := c.Peek()
	if !ok {
		return Event{}, false, nil
	}
	if ev.Type != typ {
		return Event{}, false, ReplayError{Msg: fmt.Sprintf("expected %s, got %s at seq %d", typ, ev.Type, ev.Seq)}
	}
	c.idx++
	return ev, true, nil
}
