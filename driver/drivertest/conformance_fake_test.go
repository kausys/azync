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
