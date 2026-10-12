package azync_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/kausys/azync"
	"github.com/kausys/azync/dag"
	"github.com/kausys/azync/event"
	"github.com/kausys/azync/queue"

	"github.com/stretchr/testify/require"
)

// Any runtime's permanent sentinel answers IsPermanent, wherever it was
// minted: code outside a handler asks one question, not one per runtime.
func TestIsPermanentAcrossRuntimes(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	cause := errors.New("the address does not exist")

	is.True(azync.IsPermanent(event.Permanent(cause)))
	is.True(azync.IsPermanent(queue.Abort(cause)))
	is.True(azync.IsPermanent(dag.Abort(cause)))
	is.True(azync.IsPermanent(fmt.Errorf("mail: %w", queue.Abort(cause))), "wrapped")

	is.False(azync.IsPermanent(cause))
	is.False(azync.IsPermanent(queue.Retry(cause)))
	is.False(azync.IsPermanent(queue.RetryAfter(cause, time.Second)))
	is.False(azync.IsPermanent(dag.NotReady(time.Second)))

	is.True(event.IsPermanent(queue.Abort(cause)), "each runtime's own name agrees")
	is.True(queue.IsAbort(event.Permanent(cause)))
	is.True(dag.IsAbort(event.Permanent(cause)))
}

func TestEachRuntimeKnowsItsLastAttempt(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	plain := errors.New("the provider timed out")
	ctx := t.Context()

	first := event.NewContext(ctx, event.Delivery{Attempt: 1, MaxAttempts: 3})
	last := event.NewContext(ctx, event.Delivery{Attempt: 3, MaxAttempts: 3})
	is.False(event.LastAttempt(first))
	is.True(event.WillRetry(first, plain))
	is.True(event.LastAttempt(last))
	is.False(event.WillRetry(last, plain), "the last failure goes to the dead letter")
	is.False(event.WillRetry(first, event.Permanent(plain)))
	is.False(event.LastAttempt(ctx), "outside a delivery")
	is.False(event.WillRetry(ctx, plain), "outside a delivery")

	job := queue.NewContext(ctx, queue.JobInfo{Attempt: 5, MaxAttempts: 5})
	is.True(queue.LastAttempt(job))
	is.False(queue.WillRetry(job, plain))
	is.True(queue.WillRetry(queue.NewContext(ctx, queue.JobInfo{Attempt: 1, MaxAttempts: 5}), plain))

	task := dag.NewContext(ctx, dag.TaskInfo{Attempt: 2, MaxAttempts: 2})
	is.True(dag.LastAttempt(task))
	is.False(dag.WillRetry(task, plain))
	is.True(dag.WillRetry(task, dag.NotReady(time.Second)), "waiting spends no attempt")
	is.False(dag.WillRetry(task, dag.Skip("nothing to do")))
}
