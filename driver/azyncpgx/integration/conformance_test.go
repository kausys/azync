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

// TestChangeNotifierConformance runs the change-notifier conformance suite
// against a live PostgreSQL Store, driving the 00011 triggers and the second
// LISTEN connection end to end.
func TestChangeNotifierConformance(t *testing.T) {
	drivertest.RunChangeNotifierConformance(t, func(t *testing.T) driver.Store {
		t.Helper()
		return newHarness(t).core.Store()
	})
}
