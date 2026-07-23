package engine

import "time"

// Backoff returns the retry delay for a 1-based attempt: exponential growth
// (2^n seconds, exponent capped at 8) with deterministic jitter (no rand
// dependency, so results are reproducible in tests), hard-capped at 300s. A
// negative attempt is treated as zero.
func Backoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	exp := min(attempt, 8)
	base := int64(1) << exp // 2, 4, 8, ... 256
	jitter := (int64(attempt) * 2_654_435_761) % (base/4 + 1)
	secs := min(base+jitter, 300)
	return time.Duration(secs) * time.Second
}
