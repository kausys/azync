package drivertest_test

import (
	"testing"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/driver/drivertest"
	fake "github.com/kausys/azync/internal/drivertest"
)

// newFakeStore builds a fresh in-memory fake for the conformance suite.
func newFakeStore(t *testing.T) driver.Store {
	t.Helper()
	return fake.NewFake()
}

// TestConformanceFake runs the public driver conformance suite against the
// in-memory fake, proving the fake honors the same observable contract the
// PostgreSQL driver is held to.
func TestConformanceFake(t *testing.T) {
	drivertest.RunConformance(t, newFakeStore)
}

// TestWorkflowConformanceFake runs the workflow-capability conformance suite
// against the in-memory fake, which implements driver.WorkflowStore in full as
// the behavioral oracle for the workflow runtime.
func TestWorkflowConformanceFake(t *testing.T) {
	drivertest.RunWorkflowConformance(t, newFakeStore)
}

// TestWorkflowConformanceSkipsWithoutCapability proves the workflow suite
// skips cleanly (instead of failing) for a store that does not implement the
// optional driver.WorkflowStore capability.
func TestWorkflowConformanceSkipsWithoutCapability(t *testing.T) {
	drivertest.RunWorkflowConformance(t, func(t *testing.T) driver.Store {
		t.Helper()
		return driver.UnimplementedStore{}
	})
}
