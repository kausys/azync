// Package eventtest provides an in-memory publisher seam for tests: a Recorder
// that satisfies the same Publish signature as the real event.Publisher, so
// application code under test can publish without a running runtime and the test
// can assert on what was published. It is safe for concurrent use.
package eventtest

import (
	"context"
	"sync"
	"testing"

	"github.com/kausys/azync/event"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Record is one captured Publish call: the assigned event id, the event args,
// and how many publish options were passed.
type Record struct {
	ID       uuid.UUID
	Args     event.EventArgs
	OptCount int
}

// Recorder captures Publish calls in memory.
type Recorder struct {
	mu      sync.Mutex
	records []Record
}

// New returns an empty Recorder.
func New(t *testing.T) *Recorder {
	t.Helper()
	return &Recorder{}
}

// Publish records the call and returns a fresh event id, matching the real
// event.Publisher.Publish signature so it drops in as a seam.
func (r *Recorder) Publish(_ context.Context, args event.EventArgs, opts ...event.PublishOption) (uuid.UUID, error) {
	id := uuid.New()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, Record{ID: id, Args: args, OptCount: len(opts)})
	return id, nil
}

// Len returns how many events were published.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

// All returns a copy of every captured record, in publish order.
func (r *Recorder) All() []Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Record, len(r.records))
	copy(out, r.records)
	return out
}

// Reset discards all captured records.
func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = nil
}

// RequireLen asserts exactly n events were published.
func (r *Recorder) RequireLen(t *testing.T, n int) {
	t.Helper()
	require.Len(t, r.All(), n)
}

// Of returns every captured event whose args are of type T, in publish order.
func Of[T event.EventArgs](r *Recorder) []T {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []T
	for _, rec := range r.records {
		if value, ok := rec.Args.(T); ok {
			out = append(out, value)
		}
	}
	return out
}

// RequireOne asserts exactly one event of type T was published and returns it.
func RequireOne[T event.EventArgs](t *testing.T, r *Recorder) T {
	t.Helper()
	got := Of[T](r)
	var zero T
	require.Len(t, got, 1, "expected exactly one %T publish", zero)
	return got[0]
}

// RequireNone asserts no event of type T was published.
func RequireNone[T event.EventArgs](t *testing.T, r *Recorder) {
	t.Helper()
	var zero T
	require.Empty(t, Of[T](r), "expected no %T publish", zero)
}
