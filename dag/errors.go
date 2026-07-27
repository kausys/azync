package dag

import (
	"errors"
	"fmt"
	"time"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/engine"
)

// Handler error taxonomy. A plain error from a handler means Retry. The
// taxonomy mirrors the queue's on purpose — same names, same semantics — so a
// handler migrating between runtimes keeps its failure behavior; it is
// reimplemented locally because the runtimes never import each other.

// ErrTaskSkipped reports that ResultOf targeted a task that settled as
// skipped: it deliberately did no work, so there is no result to read — and
// silently handing back a zero value would hide exactly the distinction the
// skipped state exists to make. Test with errors.Is and branch.
var ErrTaskSkipped = errors.New("dag: task was skipped")

type taskError struct {
	err        error
	kind       engine.OutcomeKind
	delay      time.Duration
	reportable bool
}

func (e *taskError) Error() string { return e.err.Error() }
func (e *taskError) Unwrap() error { return e.err }

// AsyncOutcome implements engine.OutcomeError, making the sentinel portable
// across runtimes (a NotReady inside a workflow Operation snoozes there too).
func (e *taskError) AsyncOutcome() engine.Outcome {
	return engine.Outcome{Kind: e.kind, Delay: e.delay, Reportable: e.reportable}
}

// Abort sends the task straight to the dead letter — the error is permanent.
// The workflow's failure policy reacts on the next scheduler pass.
func Abort(err error) error {
	return &taskError{err: err, kind: engine.OutcomeAbort}
}

// Retry reschedules with exponential backoff (also the default for plain
// errors).
func Retry(err error) error {
	return &taskError{err: err, kind: engine.OutcomeRetry}
}

// RetryAfter reschedules with a fixed delay — rate limits, resource warm-up.
func RetryAfter(err error, d time.Duration) error {
	return &taskError{err: err, kind: engine.OutcomeRetry, delay: d}
}

// Reportable retries like Retry but flags the error for loud reporting when
// retries are exhausted.
func Reportable(err error) error {
	return &taskError{err: err, kind: engine.OutcomeRetry, reportable: true}
}

// NotReady parks the task for d and re-checks then, WITHOUT consuming the
// retry budget: the polling-wait primitive. Unlike Retry it is not a failure —
// no attempt is recorded, the attempt counter is handed back, and the task
// re-polls indefinitely until it succeeds or returns a different error —
// unless the task declares a Deadline, which bounds the wait: past it, the
// next NotReady dead-letters the task and the workflow's failure policy
// reacts. Use it when the task is waiting on an external condition (e.g. a
// verification still pending on a provider), and pair it with Deadline when
// waiting forever is itself a failure.
func NotReady(d time.Duration) error {
	return &taskError{
		err:   fmt.Errorf("dag: task not ready (re-check in %v)", d),
		kind:  engine.OutcomeSnooze,
		delay: d,
	}
}

// Skip settles the task as skipped: terminal, deliberately-no-work. Use it
// when the handler finds nothing to do (the resource is already in the
// target state) so ops can distinguish "ran and worked" from "ran and had
// nothing to do". A skipped task satisfies its dependents like a succeeded
// one, carries no result — ResultOf on it returns ErrTaskSkipped — and never
// compensates.
func Skip(reason string) error {
	return &taskError{
		err:  fmt.Errorf("dag: task skipped: %s", reason),
		kind: engine.OutcomeSkip,
	}
}

// classify maps a handler error to the engine outcome the executor settles
// by, through the shared cross-runtime classifier: any runtime's sentinel is
// honored, not only this package's.
func classify(err error) engine.Outcome {
	return engine.ClassifyOutcome(err)
}

// IsNotFound reports whether err is the driver's not-found / wrong-state
// error, returned by Manager verbs whose target dag was absent or in an
// unexpected state.
func IsNotFound(err error) bool {
	return driver.IsNotFound(err)
}
