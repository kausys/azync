package queue

import (
	"context"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// jobKey is the private context key carrying the JobInfo of the job in flight. A
// single value per package holds the whole struct; the accessors read their
// field from it.
type jobKey struct{}

// NewContext returns a copy of parent carrying j, so a handler can be exercised
// in isolation in a test without a running worker: build a JobInfo, attach it,
// and the accessors below read from it exactly as they do in production.
func NewContext(parent context.Context, j JobInfo) context.Context {
	return context.WithValue(parent, jobKey{}, j)
}

// JobFromContext returns the JobInfo carried by ctx and whether one was present.
// Outside a job (a ctx that never passed through a worker) it returns the zero
// JobInfo and false.
func JobFromContext(ctx context.Context) (JobInfo, bool) {
	j, ok := ctx.Value(jobKey{}).(JobInfo)
	return j, ok
}

func jobFromContext(ctx context.Context) JobInfo {
	j, _ := JobFromContext(ctx)
	return j
}

// jobInfoFrom projects the cross-cutting metadata of a leased job onto a
// JobInfo the handler reads through the accessors.
func jobInfoFrom(job driver.Job) JobInfo {
	return JobInfo{
		ID:          job.ID,
		Kind:        job.Kind,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		EnqueuedAt:  job.EnqueuedAt,
		Meta:        job.Meta,
	}
}

// The accessors below read the metadata a handler is running under. Each is
// zero-value-safe: called on a context that did not come from a job (for example
// in a unit test that forgot NewContext), it returns the zero value of its
// result rather than panicking.

// JobID is the job's primary key. uuid.Nil outside a job.
func JobID(ctx context.Context) uuid.UUID { return jobFromContext(ctx).ID }

// Kind is the job's kind. Empty outside a job.
func Kind(ctx context.Context) string { return jobFromContext(ctx).Kind }

// Attempt is the 1-based execution attempt; the first run is attempt 1. Zero
// outside a job.
func Attempt(ctx context.Context) int { return jobFromContext(ctx).Attempt }

// MaxAttempts is the resolved retry budget for the job. Zero outside a job.
func MaxAttempts(ctx context.Context) int { return jobFromContext(ctx).MaxAttempts }

// IsRetry reports whether this is a re-execution (Attempt > 1). False outside a
// job.
func IsRetry(ctx context.Context) bool { return jobFromContext(ctx).Attempt > 1 }

// EnqueuedAt is when the job was durably inserted. Zero outside a job.
func EnqueuedAt(ctx context.Context) time.Time { return jobFromContext(ctx).EnqueuedAt }

// Metadata returns the string-valued annotations attached at enqueue time. Nil
// outside a job.
func Metadata(ctx context.Context) map[string]string { return jobFromContext(ctx).Meta }
