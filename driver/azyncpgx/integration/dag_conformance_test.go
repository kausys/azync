package integration

import (
	"testing"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/driver/drivertest"
)

// TestDAGConformance runs the workflow-capability conformance suite against
// a live PostgreSQL Store on an ephemeral schema, proving the pgx DAGStore
// honors the same observable contract as the in-memory fake oracle.
func TestDAGConformance(t *testing.T) {
	drivertest.RunDAGConformance(t, func(t *testing.T) driver.Store {
		t.Helper()
		return newHarness(t).core.Store()
	})
}
