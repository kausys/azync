package kernel_test

import (
	"encoding/json"
	"testing"

	"github.com/kausys/azync/workflow/kernel"

	"github.com/stretchr/testify/require"
)

// TestReplayIsPrefixClosed verifies that replaying any prefix of a history and
// then continuing produces the same EventSeq order — a basic determinism /
// crash-recovery property of the append-only log.
func TestReplayIsPrefixClosed(t *testing.T) {
	t.Parallel()
	is := require.New(t)

	var full kernel.History
	full.Append(kernel.EventWorkflowStarted, json.RawMessage(`{"n":"w"}`))
	full.Append(kernel.EventOperationScheduled, json.RawMessage(`{"op":1}`))
	full.Append(kernel.EventOperationCompleted, json.RawMessage(`{"op":1,"ok":true}`))
	full.Append(kernel.EventWorkflowCompleted, json.RawMessage(`{}`))

	for prefix := 0; prefix <= len(full.Events); prefix++ {
		partial := kernel.History{Events: append([]kernel.Event(nil), full.Events[:prefix]...)}
		c := kernel.NewCursor(&partial)
		var seqs []int64
		for {
			ev, ok := c.Next()
			if !ok {
				break
			}
			seqs = append(seqs, ev.Seq)
		}
		for i, seq := range seqs {
			is.Equal(int64(i+1), seq)
		}
		if prefix < len(full.Events) {
			is.Equal(int64(prefix+1), partial.NextSeq())
		}
	}
}
