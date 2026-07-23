package azyncpgx

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestSmokeIntegration is an optional end-to-end smoke against a real
// PostgreSQL: it proves migrate + the core queue/event paths wire up against a
// live backend (the full e2e coverage lives in the integration package). It
// runs only when AZYNC_INTEGRATION_DATABASE_URL is set and the database
// answers a ping within one second; otherwise it skips. Each run uses an
// ephemeral schema dropped on cleanup.
func TestSmokeIntegration(t *testing.T) {
	dsn := os.Getenv("AZYNC_INTEGRATION_DATABASE_URL")
	if dsn == "" {
		t.Skip("AZYNC_INTEGRATION_DATABASE_URL not set; skipping integration smoke")
	}

	adminPool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Skipf("cannot build admin pool: %v", err)
	}
	defer adminPool.Close()
	pingCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := adminPool.Ping(pingCtx); err != nil {
		t.Skipf("database not reachable within 1s: %v", err)
	}

	schema := fmt.Sprintf("azync_it_%d", time.Now().UnixNano())
	opened, err := open(dsn, driver.Config{Schema: schema})
	require.NoError(t, err)
	store, ok := opened.(*Store)
	require.True(t, ok)

	ctx := context.Background()
	t.Cleanup(func() {
		_, _ = adminPool.Exec(context.Background(),
			"DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		_ = store.Close(context.Background())
	})

	require.NoError(t, store.Migrate(ctx))

	// enqueue -> dequeue -> ack
	jobID := uuid.New()
	inserted, err := store.Enqueue(ctx, driver.EnqueueParams{
		ID: jobID, Kind: "email", Payload: json.RawMessage(`{"to":"a@b.c"}`),
		MaxAttempts: 3, MaxAttemptsExplicit: true,
	})
	require.NoError(t, err)
	require.True(t, inserted)

	leased, err := store.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{
		Kind: "email", Limit: 10, Lease: 30 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, leased, 1)
	require.Equal(t, jobID, leased[0].ID)
	require.Equal(t, 1, leased[0].Attempt)
	require.NoError(t, store.Ack(ctx, leased[0].ID, leased[0].LeaseToken))

	acked, err := store.GetJob(ctx, driver.SourceQueue, jobID)
	require.NoError(t, err)
	require.Equal(t, driver.StateSucceeded, acked.State)

	// publish -> dequeue(event) with a rehydrated Envelope
	require.NoError(t, store.RegisterSubscriber(ctx, driver.Subscriber{
		Name: "projector", EventType: "order.created", MaxAttempts: 5,
	}))
	eventID := uuid.New()
	delivered, err := store.Publish(ctx, driver.PublishParams{
		ID: eventID, Type: "order.created", OccurredAt: time.Now(),
		Payload: json.RawMessage(`{"order":1}`),
	})
	require.NoError(t, err)
	require.Equal(t, 1, delivered)

	deliveries, err := store.DequeueBatch(ctx, driver.SourceEvent, driver.DequeueParams{
		Kind: "projector", Limit: 10, Lease: 30 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, deliveries, 1)
	require.NotNil(t, deliveries[0].Event)
	require.Equal(t, eventID, deliveries[0].Event.ID)
	require.Equal(t, "order.created", deliveries[0].Event.Type)
	require.JSONEq(t, `{"order":1}`, string(deliveries[0].Event.Payload))

	// reap: a leased job whose short lease expired is reclaimed and, at the reap
	// budget, killed.
	reapID := uuid.New()
	_, err = store.Enqueue(ctx, driver.EnqueueParams{
		ID: reapID, Kind: "reapme", Payload: json.RawMessage(`{}`),
		MaxAttempts: 3, MaxAttemptsExplicit: true,
	})
	require.NoError(t, err)
	reapLeased, err := store.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{
		Kind: "reapme", Limit: 1, Lease: time.Millisecond,
	})
	require.NoError(t, err)
	require.Len(t, reapLeased, 1)

	time.Sleep(50 * time.Millisecond) // let the 1ms lease expire on the DB clock
	reaped, killed, err := store.ReapExpired(ctx, driver.SourceQueue, []string{"reapme"}, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), reaped)
	require.Equal(t, int64(1), killed)

	dead, err := store.GetJob(ctx, driver.SourceQueue, reapID)
	require.NoError(t, err)
	require.Equal(t, driver.StateDead, dead.State)
}
