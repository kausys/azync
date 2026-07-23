package queue

import (
	"encoding/json"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// JobArgs identifies a unit of work by its wire-stable Kind (decoupled from
// the Go type path), e.g. "auth.email_otp.send".
type JobArgs interface {
	Kind() string
}

// Job carries a decoded unit of work into its handler. Cross-cutting context
// travels in ctx; job-specific data lives here.
type Job[T JobArgs] struct {
	ID          uuid.UUID
	Args        T
	Attempt     int // 1-based: first execution is attempt 1
	MaxAttempts int
	EnqueuedAt  time.Time
	Meta        map[string]string
}

// RawJob carries an undecoded unit of work into a raw handler — the seam for
// dynamic kinds (undecoded payloads for adapters that cannot use Register[T]).
type RawJob struct {
	ID          uuid.UUID
	Kind        string
	Payload     json.RawMessage
	Attempt     int // 1-based
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
