// Package clock provides a minimal injectable time source so runtimes and the
// in-memory test store can be driven by a controllable clock in tests while
// using the real wall clock in production.
package clock

import "time"

// Clock returns the current time.
type Clock interface {
	Now() time.Time
}

// SystemClock is a [Clock] backed by the wall clock.
type SystemClock struct{}

// Now returns the current system time.
func (SystemClock) Now() time.Time { return time.Now() }
