package event

import (
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

// AsyncOutcome implements engine.OutcomeError, making the sentinel portable
// across runtimes.
func (e *permanentError) AsyncOutcome() engine.Outcome {
	return engine.Outcome{Kind: engine.OutcomeAbort}
}

// Permanent marks a handler error as non-retryable: the delivery goes straight
// to the dead letter instead of consuming its remaining retry budget.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// classify maps a handler error to the engine outcome the executor settles
// by, through the shared cross-runtime classifier: any runtime's sentinel is
// honored (Permanent aborts; a dag.NotReady from shared handler code snoozes
// instead of burning the delivery's budget).
func classify(err error) engine.Outcome {
	return engine.ClassifyOutcome(err)
}

// IsNotFound reports whether err is the driver's not-found / wrong-state error,
// returned by admin operations whose target delivery was absent or in an
// unexpected state.
func IsNotFound(err error) bool {
	return driver.IsNotFound(err)
}
