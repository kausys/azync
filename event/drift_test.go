package event

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/kausys/azync"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/stretchr/testify/require"
)

// capturedRecord is one log record captured by capturingHandler, flattened
// for easy assertions.
type capturedRecord struct {
	level slog.Level
	msg   string
	attrs map[string]string
}

// capturingHandler is a minimal slog.Handler that records every log call, so
// a test can assert on a specific Warn without parsing text output.
type capturingHandler struct {
	mu      sync.Mutex
	records []capturedRecord
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := map[string]string{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, capturedRecord{level: r.Level, msg: r.Message, attrs: attrs})
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) hasWarnWithAttr(key, value string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.level == slog.LevelWarn && r.attrs[key] == value {
			return true
		}
	}
	return false
}

// TestSubscriberDriftWarnsForUnregisteredDurableSubscription proves Start
// logs a Warn naming a durably registered subscriber that has no local
// registration on this worker — the signal an operator needs to catch a
// subscriber retired in code but never deleted (Worker.Register's
// durability caveat), whose deliveries would otherwise pile up forever and
// block Manager.Retain.
func TestSubscriberDriftWarnsForUnregisteredDurableSubscription(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	handler := &capturingHandler{}

	core, err := azync.New(f, azync.WithLogger(slog.New(handler)))
	is.NoError(err)
	r, err := New(core, fastOptions()...)
	is.NoError(err)

	// A durable subscription exists for "orphan_sub" (as if a since-retired
	// process registered it), but nothing on this worker registers it.
	is.NoError(r.Publisher().Register(context.Background(),
		Subscription{Name: "orphan_sub", EventType: "orders.created.v1", MaxAttempts: 3}))
	is.NoError(r.Worker().Register(namedSubscriber("billing"),
		On(func(context.Context, orderCreated) error { return nil })))

	stop := startWorker(t, r.Worker())
	awaitReady(t, r.Worker())
	is.NoError(stop())

	is.True(handler.hasWarnWithAttr("subscriber", "orphan_sub"),
		"expected a drift warning naming the orphaned subscriber")
	is.False(handler.hasWarnWithAttr("subscriber", "billing"),
		"a registered subscriber must not be warned about")
}

// TestSubscriberDriftSilentWhenEveryDurableSubscriptionIsRegistered proves no
// drift warning fires when every durable subscription has a local
// registration.
func TestSubscriberDriftSilentWhenEveryDurableSubscriptionIsRegistered(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	f := drivertest.NewFake()
	handler := &capturingHandler{}

	core, err := azync.New(f, azync.WithLogger(slog.New(handler)))
	is.NoError(err)
	r, err := New(core, fastOptions()...)
	is.NoError(err)

	is.NoError(r.Worker().Register(namedSubscriber("billing"),
		On(func(context.Context, orderCreated) error { return nil })))

	stop := startWorker(t, r.Worker())
	awaitReady(t, r.Worker())
	is.NoError(stop())

	is.False(handler.hasWarnWithAttr("subscriber", "billing"))
}
