package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestAccessorsReadTaskInfoFromContext(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	id := uuid.New()
	enqueued := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	ctx := NewContext(context.Background(), TaskInfo{
		WorkflowID:  id,
		TaskKey:     "verify",
		Kind:        "kyc.verify",
		Attempt:     2,
		MaxAttempts: 5,
		EnqueuedAt:  enqueued,
		Meta:        map[string]string{"tenant": "t1"},
	})

	is.Equal(id, WorkflowID(ctx))
	is.Equal("verify", TaskKey(ctx))
	is.Equal(2, Attempt(ctx))
	is.Equal(5, MaxAttempts(ctx))
	is.True(IsRetry(ctx))
	is.Equal(map[string]string{"tenant": "t1"}, Metadata(ctx))

	info, ok := TaskFromContext(ctx)
	is.True(ok)
	is.Equal("kyc.verify", info.Kind)
	is.True(enqueued.Equal(info.EnqueuedAt))
}

func TestAccessorsAreZeroValueSafeOutsideATask(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()

	is.Equal(uuid.Nil, WorkflowID(ctx))
	is.Empty(TaskKey(ctx))
	is.Zero(Attempt(ctx))
	is.Zero(MaxAttempts(ctx))
	is.False(IsRetry(ctx))
	is.Nil(Metadata(ctx))
	_, ok := TaskFromContext(ctx)
	is.False(ok)
}

func TestResultOfWithoutResolverReturnsClearError(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	// NewContext is the test seam for handlers: it carries TaskInfo but no
	// resolver, so ResultOf must fail with a message that says exactly that.
	ctx := NewContext(context.Background(), TaskInfo{TaskKey: "b"})

	_, err := ResultOf[string](ctx, "a")
	is.Error(err)
	is.Contains(err.Error(), "no result resolver in context")
	is.Contains(err.Error(), `"a"`, "the error must name the requested key")
}
