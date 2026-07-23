package drivertest

import (
	"sync"
	"time"
)

// ManualClock is a thread-safe, manually advanced clock.Clock for tests that
// need to control the Fake's view of time (lease expiry, run_at promotion,
// retention cutoffs) without sleeping.
type ManualClock struct {
	mu sync.Mutex
	t  time.Time
}

// NewManualClock returns a ManualClock frozen at start.
func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{t: start}
}

// Now returns the clock's current frozen time.
func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Advance moves the clock forward by d.
func (c *ManualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// Set jumps the clock to t (backwards jumps are allowed, e.g. to simulate
// clock skew between instances).
func (c *ManualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}
