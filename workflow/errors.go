package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/kausys/azync/driver"
)

// ErrUnknownWorkflow is returned when a workflow-task job names a
// (name, version) pair no RegisterWorkflow call bound on this worker.
var ErrUnknownWorkflow = errors.New("workflow: unknown workflow (name, version)")

// ErrUnknownOperation is returned when replay reaches an ExecuteOperation
// call for a (name, version) pair no RegisterOperation call bound on this
// worker.
var ErrUnknownOperation = errors.New("workflow: unknown operation (name, version)")

// ErrSelectNoFutures is returned by Select when called with no futures.
var ErrSelectNoFutures = errors.New("workflow: Select requires at least one future")

// ErrUncertain is returned by an Operation handler when the outcome of a
// mutation cannot be proven. The executor marks the Operation uncertain and
// suspends the workflow (docs/workflow-v1-spec.md §8).
var ErrUncertain = errors.New("workflow: operation uncertain")

// IsUncertain reports whether err is (or wraps) ErrUncertain.
func IsUncertain(err error) bool {
	return errors.Is(err, ErrUncertain)
}

type executionKeyCtxKey struct{}

// withExecutionKey attaches the automatic Operation ExecutionKey to ctx.
func withExecutionKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, executionKeyCtxKey{}, key)
}

// ExecutionKey returns the automatic per-attempt Operation identity from an
// Operation handler's context, or "" outside an Operation.
func ExecutionKey(ctx context.Context) string {
	s, _ := ctx.Value(executionKeyCtxKey{}).(string)
	return s
}

// unknownWorkflowError renders a specific (name, version) against
// ErrUnknownWorkflow, testable with errors.Is.
func unknownWorkflowError(name, version string) error {
	return fmt.Errorf("workflow: %q version %q: %w", name, version, ErrUnknownWorkflow)
}

// unknownOperationError renders a specific (name, version) against
// ErrUnknownOperation, testable with errors.Is.
func unknownOperationError(name, version string) error {
	return fmt.Errorf("workflow: %q version %q: %w", name, version, ErrUnknownOperation)
}

// IsNotFound reports whether err is the driver's not-found error, returned by
// Manager verbs and Client calls whose target execution was absent or in an
// unexpected state.
func IsNotFound(err error) bool {
	return driver.IsNotFound(err)
}
