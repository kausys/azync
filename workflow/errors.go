package workflow

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

// ErrNoSignalMatched reports that Client.Signal found nothing to deliver to:
// no waiting signal task and no pending timer carries the given name on the
// target workflow. Test with errors.Is.
var ErrNoSignalMatched = errors.New("workflow: signal matched no waiting task")

type taskError struct {
	err   error
	kind  engine.OutcomeKind
	delay time.Duration
}

func (e *taskError) Error() string { return e.err.Error() }
func (e *taskError) Unwrap() error { return e.err }

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

// NotReady parks the task for d and re-checks then, WITHOUT consuming the
// retry budget: the polling-wait primitive. Unlike Retry it is not a failure —
// no attempt is recorded, the attempt counter is handed back, and the task
// re-polls indefinitely until it succeeds or returns a different error. Use it
// when the task is waiting on an external condition with no deadline of its
// own (e.g. a verification still pending on a provider).
func NotReady(d time.Duration) error {
	return &taskError{
		err:   fmt.Errorf("workflow: task not ready (re-check in %v)", d),
		kind:  engine.OutcomeSnooze,
		delay: d,
	}
}

// classify maps a handler error to the engine outcome the executor settles by.
func classify(err error) engine.Outcome {
	if te, ok := errors.AsType[*taskError](err); ok {
		return engine.Outcome{Kind: te.kind, Delay: te.delay}
	}
	return engine.Outcome{Kind: engine.OutcomeRetry}
}

// IsNotFound reports whether err is the driver's not-found / wrong-state
// error, returned by Manager verbs whose target workflow was absent or in an
// unexpected state.
func IsNotFound(err error) bool {
	return driver.IsNotFound(err)
}
