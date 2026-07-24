package integration

import (
	"testing"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/driver/drivertest"
)

// TestWorkflowConformance runs the workflow-capability conformance suite against
// a live PostgreSQL Store on an ephemeral schema, proving the pgx WorkflowStore
// honors the same observable contract as the in-memory fake oracle.
func TestWorkflowConformance(t *testing.T) {
	drivertest.RunWorkflowConformance(t, func(t *testing.T) driver.Store {
		t.Helper()
		return newHarness(t).core.Store()
	})
}
