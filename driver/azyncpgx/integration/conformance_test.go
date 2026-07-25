package integration

import (
	"testing"

	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/driver/drivertest"
)

// TestConformance runs the public driver conformance suite against a live
// PostgreSQL Store on an ephemeral schema, proving the pgx driver honors the
// same observable contract as the in-memory fake.
func TestConformance(t *testing.T) {
	drivertest.RunConformance(t, func(t *testing.T) driver.Store {
		t.Helper()
		return newHarness(t).core.Store()
	})
}
