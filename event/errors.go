package event

import (
	"errors"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
)

// Handler error taxonomy. Event delivery is a job source alongside queues, but
// its taxonomy is deliberately smaller: a plain error retries with the engine
// backoff, and Permanent aborts straight to the dead letter. There is no
// RetryAfter or Reportable — the minimal taxonomy keeps the event bus a thin
// classification layer over the shared engine.

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks a handler error as non-retryable: the delivery goes straight
// to the dead letter instead of consuming its remaining retry budget.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// isPermanent reports whether err is (or wraps) a Permanent error.
func isPermanent(err error) bool {
	var target *permanentError
	return errors.As(err, &target)
}

// classify maps a handler error to the engine outcome the executor settles by:
// Permanent aborts to dead, anything else retries with the engine backoff.
func classify(err error) engine.Outcome {
	if isPermanent(err) {
		return engine.Outcome{Kind: engine.OutcomeAbort}
	}
	return engine.Outcome{Kind: engine.OutcomeRetry}
}

// IsNotFound reports whether err is the driver's not-found / wrong-state error,
// returned by admin operations whose target delivery was absent or in an
// unexpected state.
func IsNotFound(err error) bool {
	return driver.IsNotFound(err)
}
