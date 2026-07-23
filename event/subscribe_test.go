package event

import (
	"context"
	"testing"

	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

func TestRegisterRejectsEmptyNameAndNoBindings(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())

	// Empty subscriber name.
	is.Error(r.Worker().Register(namedSubscriber(""), On(func(context.Context, orderCreated) error { return nil })))
	// No bindings.
	err := r.Worker().Register(namedSubscriber("billing"))
	is.Error(err)
	is.Contains(err.Error(), "at least one binding")
	// Nil subscriber.
	is.Error(r.Worker().Register(nil, On(func(context.Context, orderCreated) error { return nil })))
}

func TestRegisterRejectsDuplicateSubscriber(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())

	binding := On(func(context.Context, orderCreated) error { return nil })
	is.NoError(r.Worker().Register(namedSubscriber("billing"), binding))
	err := r.Worker().Register(namedSubscriber("billing"), binding)
	is.Error(err)
	is.Contains(err.Error(), `subscriber "billing" already registered`)
}

func TestRegisterRejectsDuplicateEventTypeBinding(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())

	err := r.Worker().Register(namedSubscriber("billing"),
		On(func(context.Context, orderCreated) error { return nil }),
		On(func(context.Context, orderCreated) error { return nil }))
	is.Error(err)
	is.Contains(err.Error(), "two bindings for event type")
}

func TestRegisterAfterStartFails(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	is.NoError(r.Worker().Register(namedSubscriber("billing"),
		On(func(context.Context, orderCreated) error { return nil })))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	err := r.Worker().Register(namedSubscriber("notify"),
		On(func(context.Context, orderCreated) error { return nil }))
	is.Error(err)
	is.Contains(err.Error(), "cannot register after Start")
}

func TestRegisterFuncAfterStartFails(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	r := newTestRuntime(t, drivertest.NewFake())
	is.NoError(RegisterFunc(r.Worker(), "billing", func(context.Context, orderCreated) error { return nil }))

	startWorker(t, r.Worker())
	awaitReady(t, r.Worker())

	err := RegisterFunc(r.Worker(), "notify", func(context.Context, orderCreated) error { return nil })
	is.Error(err)
	is.Contains(err.Error(), "cannot register after Start")
}
