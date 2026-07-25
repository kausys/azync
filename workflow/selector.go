package workflow

// Select deterministically resolves the first ready future in futures,
// returning its index. Readiness is checked in registration (argument)
// order, so replaying the same history always picks the same branch
// regardless of any real-world race between the futures becoming ready.
//
// When none are ready, Select parks the workflow task (see doc.go): the
// worker schedules a follow-up workflow-task job for the earliest wakeAt
// among any Sleep-derived futures in the set, and otherwise (a pure
// WaitSignal wait) leaves waking it to Client.Signal. Select is the only
// primitive that parks — Sleep and WaitSignal alone never do.
func Select(ctx Context, futures ...Future) (int, error) {
	if len(futures) == 0 {
		return -1, ErrSelectNoFutures
	}
	for i, f := range futures {
		if f.ready {
			return i, nil
		}
	}

	c := asInternal(ctx)
	var info parkInfo
	for _, f := range futures {
		if f.wakeAt.IsZero() {
			continue
		}
		if info.wakeAt.IsZero() || f.wakeAt.Before(info.wakeAt) {
			info.wakeAt = f.wakeAt
		}
	}
	c.park(info)
	panic("workflow: unreachable: park always unwinds via panic")
}
