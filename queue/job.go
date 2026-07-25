package queue

import (
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// JobArgs identifies a unit of work by its wire-stable Kind (decoupled from
// the Go type path), e.g. "auth.email_otp.send".
type JobArgs interface {
	Kind() string
}

// JobInfo is the cross-cutting metadata of one job execution. Handlers receive
// the decoded arguments as their typed value; everything about the job itself —
// its id, kind, attempt and publish-time annotations — travels on the context
// and is read through the package accessors (JobID, Kind, Attempt, ...).
type JobInfo struct {
	ID          uuid.UUID
	Kind        string
	Attempt     int // 1-based: first execution is attempt 1
	MaxAttempts int
	EnqueuedAt  time.Time
	Meta        map[string]string
}

// JobState is the wire state of a job — the same values the driver persists.
type JobState = driver.JobState

// Job lifecycle states, re-exported from the driver contract.
const (
	StatePending   = driver.StatePending
	StateScheduled = driver.StateScheduled
	StateActive    = driver.StateActive
	StateDead      = driver.StateDead
	StatePaused    = driver.StatePaused
	StateSucceeded = driver.StateSucceeded
)
