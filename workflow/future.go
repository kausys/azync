package workflow

import (
	"encoding/json"
	"time"
)

// Future is the result of a workflow primitive (ExecuteOperation, Sleep,
// WaitSignal): either already resolved from history, or not yet — in which
// case only Select can park the workflow task until it resolves. Sleep and
// WaitSignal never block by themselves; a Future they return may be unready.
type Future struct {
	ready  bool
	result json.RawMessage
	err    error

	// wakeAt is set only on an unready Sleep-derived future: the backend time
	// a durable timer will make it ready. Zero means "no timer" — an unready
	// WaitSignal future, resolved only by an external Signal.
	wakeAt time.Time
}

// Ready reports whether the future already resolved (successfully or with
// an error) from history.
func (f Future) Ready() bool { return f.ready }

// Err returns the future's error, if it resolved to one. Calling it on an
// unready future returns nil.
func (f Future) Err() error { return f.err }

// Get decodes the future's result into out (a pointer), or returns its
// error. Calling it on an unready future returns nil without touching out —
// check Ready (or route the future through Select) first.
func (f Future) Get(out any) error {
	if f.err != nil {
		return f.err
	}
	if !f.ready || out == nil || len(f.result) == 0 {
		return nil
	}
	return json.Unmarshal(f.result, out)
}

func readyFuture(result json.RawMessage) Future {
	return Future{ready: true, result: result}
}

func failedFuture(err error) Future {
	return Future{ready: true, err: err}
}

func pendingFuture(wakeAt time.Time) Future {
	return Future{wakeAt: wakeAt}
}
