package event

import (
	"context"
	"testing"

	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

func TestSubscribeRejectsEmptyAndNil(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())

	is.Error(r.Worker().Subscribe("", func(context.Context, Envelope) error { return nil }))
	is.Error(r.Worker().Subscribe("billing", nil))
}

func TestSubscribeRejectsDuplicate(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())

	handler := func(context.Context, Envelope) error { return nil }
	is.NoError(r.Worker().Subscribe("billing", handler))
	err := r.Worker().Subscribe("billing", handler)
	is.Error(err)
	is.Contains(err.Error(), `subscriber "billing" already registered`)
}

func TestSubscribeAfterStartFails(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	is.NoError(r.Worker().Subscribe("billing", func(context.Context, Envelope) error { return nil }))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	err := r.Worker().Subscribe("notify", func(context.Context, Envelope) error { return nil })
	is.Error(err)
	is.Contains(err.Error(), "cannot subscribe after Start")
}
