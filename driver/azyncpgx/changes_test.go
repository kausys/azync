package azyncpgx

import (
	"log/slog"
	"testing"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseChange(t *testing.T) {
	jobID := uuid.New()
	dagID := uuid.New()

	cases := []struct {
		name       string
		payload    string
		want       driver.Change
		wantSchema string
		ok         bool
	}{
		{
			"job change",
			`{"schema":"public","entity":"job","source":"queue","id":"` + jobID.String() + `","kind":"email.send","state":"pending","atMs":1753900000000}`,
			driver.Change{
				Entity: driver.ChangeJob, Source: driver.SourceQueue, ID: jobID,
				Kind: "email.send", State: "pending", At: time.UnixMilli(1753900000000),
			},
			"public", true,
		},
		{
			"dag task change carries its owner",
			`{"schema":"azync","entity":"job","source":"dag","id":"` + jobID.String() + `","dagId":"` + dagID.String() + `","kind":"kyc.create","taskKey":"create","state":"active","atMs":1}`,
			driver.Change{
				Entity: driver.ChangeJob, Source: driver.SourceDAG, ID: jobID, DAGID: dagID,
				Kind: "kyc.create", TaskKey: "create", State: "active", At: time.UnixMilli(1),
			},
			"azync", true,
		},
		{
			"dag header change",
			`{"schema":"azync","entity":"dag","id":"` + dagID.String() + `","kind":"onboard","state":"succeeded","atMs":1}`,
			driver.Change{
				Entity: driver.ChangeDAG, ID: dagID, Kind: "onboard",
				State: "succeeded", At: time.UnixMilli(1),
			},
			"azync", true,
		},
		{
			"event append has no state",
			`{"schema":"public","entity":"event","id":"` + jobID.String() + `","kind":"user.signed_up","atMs":1}`,
			driver.Change{
				Entity: driver.ChangeEvent, ID: jobID, Kind: "user.signed_up", At: time.UnixMilli(1),
			},
			"public", true,
		},
		{
			"bulk carries no id",
			`{"schema":"public","entity":"job","source":"dag","bulk":true,"count":417,"atMs":1}`,
			driver.Change{
				Entity: driver.ChangeJob, Source: driver.SourceDAG,
				Bulk: true, Count: 417, At: time.UnixMilli(1),
			},
			"public", true,
		},
		{"not json", "queue:email.send", driver.Change{}, "", false},
		{"missing schema", `{"entity":"job","id":"` + jobID.String() + `"}`, driver.Change{}, "", false},
		{"unknown entity", `{"schema":"public","entity":"mystery","id":"` + jobID.String() + `"}`, driver.Change{}, "", false},
		{"reset never travels on the wire", `{"schema":"public","entity":"reset"}`, driver.Change{}, "", false},
		{"non-bulk without id", `{"schema":"public","entity":"job","source":"queue","kind":"x"}`, driver.Change{}, "", false},
		{"malformed id", `{"schema":"public","entity":"job","id":"nope"}`, driver.Change{}, "", false},
		{"malformed dagId", `{"schema":"public","entity":"job","id":"` + jobID.String() + `","dagId":"nope"}`, driver.Change{}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, schema, ok := parseChange(tc.payload)
			assert.Equal(t, tc.ok, ok)
			if !tc.ok {
				return
			}
			assert.Equal(t, tc.wantSchema, schema)
			assert.True(t, got.At.Equal(tc.want.At), "At mismatch: got %v want %v", got.At, tc.want.At)
			got.At = tc.want.At // normalize monotonic/location for the struct compare
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestDeliverOverflowAnnouncesReset exercises the listener's drop-to-reset
// path without a database: a subscriber whose buffer fills gets one in-band
// reset before its next delivery instead of a silent gap.
func TestDeliverOverflowAnnouncesReset(t *testing.T) {
	is := require.New(t)
	l := newListener(nil, "azync_changes", false, slog.New(slog.DiscardHandler),
		2, func(string) (driver.Change, bool) { return driver.Change{}, false }, resetChange)
	sub := &subscriber[driver.Change]{ch: make(chan driver.Change, 2)}

	hint := func(kind string) driver.Change {
		return driver.Change{Entity: driver.ChangeJob, Source: driver.SourceQueue, ID: uuid.New(), Kind: kind}
	}

	l.mu.Lock()
	l.deliverLocked(sub, hint("a"))
	l.deliverLocked(sub, hint("b"))
	l.deliverLocked(sub, hint("dropped")) // buffer full: marks overflow
	l.mu.Unlock()
	is.Equal("a", (<-sub.ch).Kind)

	// Drain the second buffered hint, then deliver again: the reset must
	// precede the new hint.
	is.Equal("b", (<-sub.ch).Kind)
	l.mu.Lock()
	l.deliverLocked(sub, hint("after-gap"))
	l.mu.Unlock()

	first := <-sub.ch
	is.Equal(driver.ChangeReset, first.Entity, "the gap must surface as a reset")
	second := <-sub.ch
	is.Equal("after-gap", second.Kind)
}

// TestSubscribeDefersResetUntilConnected pins the liveness meaning of the
// opening reset: it must never arrive before the LISTEN is established, or a
// consumer could refetch and then silently miss changes committed before the
// stream could observe them.
func TestSubscribeDefersResetUntilConnected(t *testing.T) {
	is := require.New(t)
	l := newListener(nil, "azync_changes", false, slog.New(slog.DiscardHandler),
		2, func(string) (driver.Change, bool) { return driver.Change{}, false }, resetChange)
	// Pretend the listen loop already runs, so subscribe does not start one
	// against the nil pool.
	l.mu.Lock()
	l.started = true
	l.mu.Unlock()

	ctx := t.Context()
	ch, err := l.subscribe(ctx)
	is.NoError(err)
	select {
	case <-ch:
		t.Fatal("no reset may arrive before the stream is live")
	default:
	}

	l.markConnectedAndReset()
	is.Equal(driver.ChangeReset, (<-ch).Entity, "the connect broadcast delivers the deferred opening reset")

	// A subscription made while connected opens with an immediate reset.
	ch2, err := l.subscribe(ctx)
	is.NoError(err)
	is.Equal(driver.ChangeReset, (<-ch2).Entity)

	// After a disconnect, new subscriptions defer again until the reconnect.
	l.markDisconnected()
	ch3, err := l.subscribe(ctx)
	is.NoError(err)
	select {
	case <-ch3:
		t.Fatal("no reset may arrive while disconnected")
	default:
	}
	l.markConnectedAndReset()
	is.Equal(driver.ChangeReset, (<-ch3).Entity)
}

// TestDeliverWithoutResetKeepsPlainDrop pins the wakeup contract: a listener
// with no reset func drops on overflow and never injects anything.
func TestDeliverWithoutResetKeepsPlainDrop(t *testing.T) {
	is := require.New(t)
	l := newListener(nil, "azync", false, slog.New(slog.DiscardHandler),
		1, parseWake, nil)
	sub := &subscriber[driver.Wake]{ch: make(chan driver.Wake, 1)}

	l.mu.Lock()
	l.deliverLocked(sub, driver.Wake{Source: driver.SourceQueue, Kind: "a"})
	l.deliverLocked(sub, driver.Wake{Source: driver.SourceQueue, Kind: "dropped"})
	l.mu.Unlock()
	is.Equal("a", (<-sub.ch).Kind)

	l.mu.Lock()
	l.deliverLocked(sub, driver.Wake{Source: driver.SourceQueue, Kind: "b"})
	l.mu.Unlock()
	is.Equal("b", (<-sub.ch).Kind, "no reset is ever injected for wakeups")
}
