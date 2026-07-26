package workflow

import (
	"context"
	"testing"

	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

// TestRegisterWorkflowDuplicatePanics proves re-registering the same
// (name, version) pair panics — a programmer error worth failing loudly on,
// since the two likely causes (a duplicate registration, or workflow code
// that changed without bumping its version) are both bugs, not legitimate
// re-registrations.
func TestRegisterWorkflowDuplicatePanics(t *testing.T) {
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	RegisterWorkflow(r.Worker(), "wf-dup", "1", func(Context, struct{}) (string, error) {
		return "a", nil
	})
	is.Panics(func() {
		RegisterWorkflow(r.Worker(), "wf-dup", "1", func(Context, struct{}) (string, error) {
			return "b", nil
		})
	})

	// A different version is a distinct registration, not a duplicate.
	is.NotPanics(func() {
		RegisterWorkflow(r.Worker(), "wf-dup", "2", func(Context, struct{}) (string, error) {
			return "b", nil
		})
	})
}

// TestRegisterOperationDuplicatePanics mirrors
// TestRegisterWorkflowDuplicatePanics for RegisterOperation.
func TestRegisterOperationDuplicatePanics(t *testing.T) {
	is := require.New(t)
	f := drivertest.NewFake()
	r := newTestRuntime(t, f)

	RegisterOperation(r.Worker(), "op-dup", "1", func(context.Context, struct{}) (string, error) {
		return "a", nil
	})
	is.Panics(func() {
		RegisterOperation(r.Worker(), "op-dup", "1", func(context.Context, struct{}) (string, error) {
			return "b", nil
		})
	})

	is.NotPanics(func() {
		RegisterOperation(r.Worker(), "op-dup", "2", func(context.Context, struct{}) (string, error) {
			return "b", nil
		})
	})
}
