package queue

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNewContextExposesJobInfoToAccessors(t *testing.T) {
	t.Parallel()
	is := require.New(t)

	id := uuid.New()
	enqueued := time.Date(2026, 7, 23, 10, 0, 0, 0, time.UTC)
	info := JobInfo{
		ID:          id,
		Kind:        "queue.test",
		Attempt:     2,
		MaxAttempts: 5,
		EnqueuedAt:  enqueued,
		Meta:        map[string]string{"k": "v"},
	}
	ctx := NewContext(context.Background(), info)

	got, ok := JobFromContext(ctx)
	is.True(ok)
	is.Equal(info, got)

	is.Equal(id, JobID(ctx))
	is.Equal("queue.test", Kind(ctx))
	is.Equal(2, Attempt(ctx))
	is.Equal(5, MaxAttempts(ctx))
	is.True(IsRetry(ctx), "attempt 2 is a retry")
	is.Equal(enqueued, EnqueuedAt(ctx))
	is.Equal(map[string]string{"k": "v"}, Metadata(ctx))
}

func TestAccessorsAreZeroValueSafeOutsideAJob(t *testing.T) {
	t.Parallel()
	is := require.New(t)
	ctx := context.Background()

	_, ok := JobFromContext(ctx)
	is.False(ok)

	is.Equal(uuid.Nil, JobID(ctx))
	is.Empty(Kind(ctx))
	is.Zero(Attempt(ctx))
	is.Zero(MaxAttempts(ctx))
	is.False(IsRetry(ctx))
	is.True(EnqueuedAt(ctx).IsZero())
	is.Nil(Metadata(ctx))
}
