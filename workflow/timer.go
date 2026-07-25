package workflow

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kausys/azync/workflow/kernel"
)

// timerPayload is the durable encoding of a started timer's deadline.
type timerPayload struct {
	FireAt time.Time `json:"fireAt"`
}

// Sleep starts (or, on replay, recovers) a durable timer for d against the
// backend clock and returns its Future immediately — ready when the timer
// already fired, unready otherwise. Sleep never blocks by itself: route the
// result through Select (even alone, e.g. Select(ctx, Sleep(ctx, d))) to
// actually park the workflow task until it fires.
func Sleep(ctx Context, d time.Duration) Future {
	c := asInternal(ctx)
	f, _ := c.probeSleep(d)
	return f
}

// probeSleep is Sleep's non-parking half, shared with Select so a Sleep
// future considered but not chosen still gets its TimerStarted event
// recorded exactly once (the timer is real and running regardless of which
// branch of a Select eventually wins).
func (c *wfContext) probeSleep(d time.Duration) (Future, parkInfo) {
	var tp timerPayload
	ev, ok, err := c.cursor.TakeOrCommand(kernel.EventTimerStarted)
	if err != nil {
		panic(err)
	}
	if !ok {
		tp = timerPayload{FireAt: c.now.Add(d)}
		c.record(kernel.EventTimerStarted, marshalOrPanic(tp))
	} else if err := json.Unmarshal(ev.Payload, &tp); err != nil {
		panic(fmt.Errorf("workflow: decode recorded timer: %w", err))
	}

	if _, ok, err := c.cursor.TakeOrCommand(kernel.EventTimerFired); err != nil {
		panic(err)
	} else if ok {
		return readyFuture(nil), parkInfo{}
	}

	if !c.now.Before(tp.FireAt) {
		c.record(kernel.EventTimerFired, nil)
		return readyFuture(nil), parkInfo{}
	}
	return pendingFuture(tp.FireAt), parkInfo{wakeAt: tp.FireAt}
}
