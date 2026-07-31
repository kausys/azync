package watch

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/internal/drivertest"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// newTestWatcher composes a Watcher over a fresh fake store.
func newTestWatcher(t *testing.T, opts ...Option) (*Watcher, *drivertest.Fake) {
	t.Helper()
	is := require.New(t)
	f := drivertest.NewFake()
	core, err := azync.New(f, azync.WithLogger(discardLogger()))
	is.NoError(err)
	w, err := New(core, opts...)
	is.NoError(err)
	return w, f
}

// awaitChange drains ch until pred matches or a second passes.
func awaitChange(t *testing.T, ch <-chan Change, pred func(Change) bool) Change {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case c, open := <-ch:
			if !open {
				t.Fatal("watch channel closed before the expected change arrived")
			}
			if pred(c) {
				return c
			}
		case <-deadline:
			t.Fatal("expected change did not arrive")
		}
	}
}

func enqueue(t *testing.T, f *drivertest.Fake, kind string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := f.Enqueue(context.Background(), driver.EnqueueParams{
		ID: id, Kind: kind, Payload: json.RawMessage(`{}`),
	})
	require.NoError(t, err)
	return id
}

func TestNewRejectsNilCore(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	_, err := New(nil)
	is.Error(err)
	is.Contains(err.Error(), "core is nil")
}

// storeOnly masks every optional capability of the fake (ChangeNotifier
// included), leaving the bare driver.Store.
type storeOnly struct {
	driver.Store
}

func TestNewRejectsDriverWithoutChangeNotifications(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	core, err := azync.New(storeOnly{Store: drivertest.NewFake()}, azync.WithLogger(discardLogger()))
	is.NoError(err)

	_, err = New(core)
	is.Error(err)
	is.Contains(err.Error(), "does not support change notifications")
}

// pollOnlyStore reports the ChangeNotifier capability but cannot push: the
// nil-channel contract a poll-only backend uses.
type pollOnlyStore struct {
	driver.Store
}

func (pollOnlyStore) Changes(context.Context) (<-chan driver.Change, error) {
	//nolint:nilnil // poll-only backend: a nil channel with nil error is the contract signal
	return nil, nil
}

func TestWatchReportsPollOnlyDriver(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	core, err := azync.New(pollOnlyStore{Store: drivertest.NewFake()}, azync.WithLogger(discardLogger()))
	is.NoError(err)
	w, err := New(core)
	is.NoError(err)

	_, err = w.Watch(t.Context(), Filter{})
	is.Error(err)
	is.Contains(err.Error(), "poll-only")
}

func TestWatchDeliversInitialResetThenChanges(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	w, f := newTestWatcher(t)

	ch, err := w.Watch(t.Context(), Filter{})
	is.NoError(err)

	first := <-ch
	is.Equal(EntityReset, first.Entity, "the first delivery is always a reset")

	id := enqueue(t, f, "send")
	c := awaitChange(t, ch, func(c Change) bool { return c.ID == id })
	is.Equal(EntityJob, c.Entity)
	is.Equal("send", c.Kind)
}

func TestWatchFilterExcludesNonMatching(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	w, f := newTestWatcher(t)

	ch, err := w.Watch(t.Context(), Filter{Kinds: []string{"wanted"}})
	is.NoError(err)

	enqueue(t, f, "noise")
	want := enqueue(t, f, "wanted")

	// The first matching change after the reset must be the wanted kind: the
	// noise enqueue that preceded it was filtered out, so seeing "wanted"
	// first proves the exclusion.
	c := awaitChange(t, ch, func(c Change) bool { return c.Entity == EntityJob })
	is.Equal(want, c.ID)
	is.Equal("wanted", c.Kind)
}

func TestWatchChannelClosesOnCtxCancel(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	w, _ := newTestWatcher(t)

	ctx, cancel := context.WithCancel(t.Context())
	ch, err := w.Watch(ctx, Filter{})
	is.NoError(err)
	cancel()

	deadline := time.After(time.Second)
	for {
		select {
		case _, open := <-ch:
			if !open {
				return
			}
		case <-deadline:
			t.Fatal("watch channel did not close after ctx cancellation")
		}
	}
}

func TestWatchOverflowCollapsesIntoReset(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	w, f := newTestWatcher(t, WithBuffer(2))

	ch, err := w.Watch(t.Context(), Filter{})
	is.NoError(err)

	// Flood more changes than every buffer in the path can hold, consuming
	// nothing: by pigeonhole something must drop, and the contract requires
	// the drop to surface as an in-band reset before any later delivery. A
	// dropped hint is only replaced by a reset on the NEXT delivery attempt,
	// so the drain loop keeps nudging with fresh changes until one lands.
	for range 400 {
		enqueue(t, f, "flood")
	}

	nudges := map[uuid.UUID]bool{enqueue(t, f, "nudge"): true}
	sawGapReset := false
	initialSeen := false
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case c := <-ch:
			if c.Entity == EntityReset {
				if initialSeen {
					sawGapReset = true
				}
				initialSeen = true
			}
			if nudges[c.ID] {
				is.True(sawGapReset, "a dropped hint must be announced by a reset before later deliveries")
				return
			}
		case <-tick.C:
			nudges[enqueue(t, f, "nudge")] = true
		case <-deadline:
			t.Fatal("no nudge change ever arrived after the overflow")
		}
	}
}

func TestFilterMatches(t *testing.T) {
	t.Parallel()
	dagID := uuid.New()
	jobChange := Change{Entity: EntityJob, Source: driver.SourceQueue, ID: uuid.New(), Kind: "send", State: "pending"}
	taskChange := Change{Entity: EntityJob, Source: driver.SourceDAG, ID: uuid.New(), DAGID: dagID, Kind: "task.kind", TaskKey: "step"}
	dagChange := Change{Entity: EntityDAG, ID: dagID, Kind: "onboard", State: "running"}
	eventChange := Change{Entity: EntityEvent, ID: uuid.New(), Kind: "user.signed_up"}
	reset := Change{Entity: EntityReset}
	jobBulk := Change{Entity: EntityJob, Source: driver.SourceDAG, Bulk: true, Count: 120}

	cases := []struct {
		name   string
		filter Filter
		change Change
		want   bool
	}{
		{"zero filter admits everything", Filter{}, jobChange, true},
		{"reset bypasses every bound", Filter{Entities: []Entity{EntityEvent}, Kinds: []string{"x"}, DAGID: uuid.New()}, reset, true},
		{"entity match", Filter{Entities: []Entity{EntityDAG}}, dagChange, true},
		{"entity mismatch", Filter{Entities: []Entity{EntityDAG}}, jobChange, false},
		{"source match", Filter{Sources: []Source{driver.SourceQueue}}, jobChange, true},
		{"source mismatch", Filter{Sources: []Source{driver.SourceQueue}}, taskChange, false},
		{"source bound never excludes non-jobs", Filter{Sources: []Source{driver.SourceQueue}}, eventChange, true},
		{"kind match", Filter{Kinds: []string{"user.signed_up"}}, eventChange, true},
		{"kind mismatch", Filter{Kinds: []string{"other"}}, jobChange, false},
		{"dag id matches its header", Filter{DAGID: dagID}, dagChange, true},
		{"dag id matches its tasks", Filter{DAGID: dagID}, taskChange, true},
		{"dag id excludes other jobs", Filter{DAGID: dagID}, jobChange, false},
		{"dag id excludes events", Filter{DAGID: dagID}, eventChange, false},
		{"bulk passes kind and dag bounds", Filter{Kinds: []string{"x"}, DAGID: dagID}, jobBulk, true},
		{"bulk still honors entity bound", Filter{Entities: []Entity{EntityEvent}}, jobBulk, false},
		{"bulk still honors source bound", Filter{Sources: []Source{driver.SourceQueue}}, jobBulk, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.filter.matches(tc.change))
		})
	}
}
