package engine

import (
	"testing"
	"time"
)

func TestBackoffKnownValues(t *testing.T) {
	t.Parallel()

	// Deterministic jitter makes these exact and reproducible.
	cases := map[int]time.Duration{
		0: 1 * time.Second,
		1: 2 * time.Second,
		2: 4 * time.Second,
		3: 8 * time.Second,
		4: 20 * time.Second,
	}
	for attempt, want := range cases {
		if got := Backoff(attempt); got != want {
			t.Errorf("Backoff(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func TestBackoffDeterministic(t *testing.T) {
	t.Parallel()

	for attempt := range 50 {
		first := Backoff(attempt)
		second := Backoff(attempt)
		if first != second {
			t.Errorf("Backoff(%d) not deterministic: %v then %v", attempt, first, second)
		}
	}
}

func TestBackoffGrowthAndCap(t *testing.T) {
	t.Parallel()

	const hardCap = 300 * time.Second
	for attempt := range 100 {
		got := Backoff(attempt)
		// Never exceeds the hard cap.
		if got > hardCap {
			t.Errorf("Backoff(%d) = %v exceeds cap %v", attempt, got, hardCap)
		}
		// Always at least the exponential base 2^min(attempt,8) seconds.
		exp := min(attempt, 8)
		wantMin := min(time.Duration(int64(1)<<exp)*time.Second, hardCap)
		if got < wantMin {
			t.Errorf("Backoff(%d) = %v below base %v", attempt, got, wantMin)
		}
	}
}

func TestBackoffNegativeClampedToZero(t *testing.T) {
	t.Parallel()

	if got, want := Backoff(-1), Backoff(0); got != want {
		t.Errorf("Backoff(-1) = %v, want %v (clamped to attempt 0)", got, want)
	}
	if got, want := Backoff(-1000), Backoff(0); got != want {
		t.Errorf("Backoff(-1000) = %v, want %v", got, want)
	}
}

func TestBackoffLargeAttemptNoOverflow(t *testing.T) {
	t.Parallel()

	// A very large attempt must not overflow the exponent or the jitter math and
	// must stay within the cap.
	for _, attempt := range []int{1_000, 1_000_000, 1 << 30} {
		got := Backoff(attempt)
		if got <= 0 || got > 300*time.Second {
			t.Errorf("Backoff(%d) = %v, want in (0, 300s]", attempt, got)
		}
	}
}
