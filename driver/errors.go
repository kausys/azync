package driver

import "errors"

// ErrNotSupported reports that a driver does not implement an optional
// capability. Core.Migrate wraps it when the driver is not a Migrator, and
// UnimplementedStore returns it for every method.
var ErrNotSupported = errors.New("azync: capability not supported by driver")

// ErrAlreadyExists reports that Publish, Enqueue or CreateDAG (or their Tx
// forms) was handed an ID that is already present. Callers assign IDs, so a
// repeat means the same write was already made: a caller forwarding writes it
// captured elsewhere — an outbox — treats it as done. It is returned only when
// nothing deduplicated the write first: a live idempotency key still answers
// inserted=false (or the live DAG's id) without error. Drivers wrap it, so
// match it with errors.Is.
var ErrAlreadyExists = errors.New("azync: id already exists")

// notFoundError is the contract's wrong-state / missing-row sentinel. Settlement
// and admin operations return it when their target row was not in the expected
// state; the runtime layer maps it to a 404-style outcome.
type notFoundError struct{ op string }

func (e *notFoundError) Error() string {
	return "azync: " + e.op + ": job not found in expected state"
}

// NewNotFound builds the contract's not-found error for the named operation.
// Drivers return it from settlement and admin methods whose target row was
// absent or in an unexpected state (e.g. lease-token fencing failed).
func NewNotFound(op string) error { return &notFoundError{op: op} }

// IsNotFound reports whether err is (or wraps) the contract's not-found error.
func IsNotFound(err error) bool {
	var target *notFoundError
	return errors.As(err, &target)
}
