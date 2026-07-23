package queue

import (
	"errors"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
)

// Handler error taxonomy. A plain error from a handler means Retry.

type jobError struct {
	err        error
	kind       engine.OutcomeKind
	delay      time.Duration
	reportable bool
}

func (e *jobError) Error() string { return e.err.Error() }
func (e *jobError) Unwrap() error { return e.err }

// Abort sends the job straight to the dead letter — the error is permanent.
func Abort(err error) error {
	return &jobError{err: err, kind: engine.OutcomeAbort}
}

// Retry reschedules with exponential backoff (also the default for plain errors).
func Retry(err error) error {
	return &jobError{err: err, kind: engine.OutcomeRetry}
}

// RetryAfter reschedules with a fixed delay — rate limits, resource warm-up.
func RetryAfter(err error, d time.Duration) error {
	return &jobError{err: err, kind: engine.OutcomeRetry, delay: d}
}

// Reportable retries like Retry but flags the error for loud reporting when
// retries are exhausted.
func Reportable(err error) error {
	return &jobError{err: err, kind: engine.OutcomeRetry, reportable: true}
}

// classify maps a handler error to the engine outcome the executor settles by.
func classify(err error) engine.Outcome {
	if je, ok := errors.AsType[*jobError](err); ok {
		return engine.Outcome{Kind: je.kind, Delay: je.delay, Reportable: je.reportable}
	}
	return engine.Outcome{Kind: engine.OutcomeRetry}
}

// IsNotFound reports whether err is the queue's not-found/wrong-state error.
func IsNotFound(err error) bool {
	return driver.IsNotFound(err)
}
