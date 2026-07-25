package workflow

import (
	"encoding/json"
	"fmt"
	"time"
)

// signalEventPayload is the durable encoding of one delivered signal, as
// recorded directly into the workflow's history by Client.Signal.
type signalEventPayload struct {
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// WaitSignal looks for the named signal in history and returns its Future
// immediately — ready with the signal's payload when one has already
// arrived (including "early", before this call ever ran; see
// docs/workflow-v1-spec.md §9), unready otherwise. WaitSignal never blocks
// by itself: route the result through Select to actually park the workflow
// task until a matching signal arrives.
func WaitSignal(ctx Context, name string) Future {
	c := asInternal(ctx)
	f, _ := c.probeSignal(name)
	return f
}

// probeSignal is WaitSignal's non-parking half, shared with Select. See
// wfContext.signals for why signals are matched by an out-of-band scan
// instead of the sequential cursor. consumedSignals tracks, within this
// pass, which entries already matched an earlier WaitSignal/Select call so
// two calls for the same name never both claim one arrival.
func (c *wfContext) probeSignal(name string) (Future, parkInfo) {
	for i, ev := range c.signals {
		if c.consumedSignals[i] {
			continue
		}
		var sp signalEventPayload
		if err := json.Unmarshal(ev.Payload, &sp); err != nil {
			panic(fmt.Errorf("workflow: decode recorded signal at seq %d: %w", ev.Seq, err))
		}
		if sp.Name != name {
			continue
		}
		c.consumedSignals[i] = true
		return readyFuture(sp.Payload), parkInfo{}
	}
	return pendingFuture(time.Time{}), parkInfo{}
}
