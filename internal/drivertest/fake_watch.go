package drivertest

// The ChangeNotifier capability of the Fake: an in-memory mirror of the
// row-change hint stream the SQL driver emits from its change triggers. The
// same contract applies — best-effort, at-most-once, first delivery is a
// ChangeReset, and a subscription that overflows receives a reset before its
// next delivery instead of dropping silently. Hints fire on the same
// transitions the SQL triggers observe: job inserts and state changes, DAG
// header inserts and state/cancel_requested changes, and ledger appends.
// Workflow-as-code execution headers emit nothing, matching the driver (only
// their source=workflow jobs are covered).

import (
	"context"
	"slices"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// changeBuffer bounds each subscription's channel; a full buffer flips the
// subscription into overflow-reset mode rather than blocking a store method.
const changeBuffer = 256

// changeSub is one Changes subscription: its delivery channel plus the
// overflow flag that turns drops into an in-band ChangeReset.
type changeSub struct {
	ch         chan driver.Change
	overflowed bool
}

// Changes returns a stream of row-change hints; the channel is closed when
// ctx ends. Per the capability contract, the first delivery is a ChangeReset.
func (f *Fake) Changes(ctx context.Context) (<-chan driver.Change, error) {
	sub := &changeSub{ch: make(chan driver.Change, changeBuffer)}
	sub.ch <- driver.Change{Entity: driver.ChangeReset, At: f.now()}
	f.changeMu.Lock()
	f.changeSubs = append(f.changeSubs, sub)
	f.changeMu.Unlock()
	go func() {
		<-ctx.Done()
		f.changeMu.Lock()
		defer f.changeMu.Unlock()
		for i, s := range f.changeSubs {
			if s == sub {
				f.changeSubs = slices.Delete(f.changeSubs, i, i+1)
				break
			}
		}
		close(sub.ch)
	}()
	return sub.ch, nil
}

// changeJob broadcasts a hint for j's current state. Callers hold f.mu; like
// wake, the broadcast takes only changeMu so it can never re-enter f.mu.
func (f *Fake) changeJob(j *fakeJob) {
	f.notifyChange(driver.Change{
		Entity:  driver.ChangeJob,
		Source:  j.Source,
		ID:      j.ID,
		DAGID:   j.DAGID,
		Kind:    j.Kind,
		TaskKey: j.TaskKey,
		State:   string(j.State),
		At:      f.now(),
	})
}

// changeDAG broadcasts a hint for the DAG header's current state. Callers
// hold f.mu.
func (f *Fake) changeDAG(w *fakeDAG) {
	f.notifyChange(driver.Change{
		Entity: driver.ChangeDAG,
		ID:     w.ID,
		Kind:   w.Name,
		State:  string(w.State),
		At:     f.now(),
	})
}

// changeEvent broadcasts a hint for a ledger append. Callers hold f.mu.
func (f *Fake) changeEvent(id uuid.UUID, eventType string) {
	f.notifyChange(driver.Change{
		Entity: driver.ChangeEvent,
		ID:     id,
		Kind:   eventType,
		At:     f.now(),
	})
}

// notifyChange delivers a hint to every live subscription without blocking.
// A full buffer marks the subscription overflowed; its next delivery is then
// preceded by a ChangeReset, so a gap is announced in-band, never silent.
func (f *Fake) notifyChange(c driver.Change) {
	f.changeMu.Lock()
	defer f.changeMu.Unlock()
	for _, s := range f.changeSubs {
		if s.overflowed {
			select {
			case s.ch <- driver.Change{Entity: driver.ChangeReset, At: c.At}:
				s.overflowed = false
			default:
				continue // still full; the pending reset stands in for every drop
			}
		}
		select {
		case s.ch <- c:
		default:
			s.overflowed = true
		}
	}
}
