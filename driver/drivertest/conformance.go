// Package drivertest provides a public conformance suite that any azync storage
// driver can run against its own [driver.Store] to prove it honors the
// backend-agnostic contract. Third-party driver authors wire their backend into
// [RunConformance] and get the same behavioral coverage the first-party
// PostgreSQL driver is held to.
//
// The suite is black-box: it drives a Store only through the exported contract,
// never reaching into backend internals. It uses real wall-clock durations (a
// short lease plus a sleep for reaper tests, a far-future run_at for scheduling)
// so it is correct against any backend clock without a controllable clock.
package drivertest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Timing budgets for the reaper subtests: a lease short enough to expire within
// the test, and a wait comfortably longer than it so the lease is past-due on
// any backend clock.
const (
	reapLease = 40 * time.Millisecond
	reapWait  = 200 * time.Millisecond
	// tick spaces consecutive enqueues so their backend-stamped timestamps
	// (enqueued_at, completed_at) are strictly ordered for the listing tests.
	tick = 4 * time.Millisecond
)

// RunConformance exercises the observable [driver.Store] contract against the
// Store returned by newStore. newStore is called once; every subtest shares that
// Store and stays independent by using unique kind and subscriber names, so a
// backend need not reset between subtests (they must not assume an empty store).
func RunConformance(t *testing.T, newStore func(t *testing.T) driver.Store) {
	t.Helper()
	store := newStore(t)

	t.Run("Enqueue", func(t *testing.T) { runEnqueue(t, store) })
	t.Run("Dequeue", func(t *testing.T) { runDequeue(t, store) })
	t.Run("Settlement", func(t *testing.T) { runSettlement(t, store) })
	t.Run("ReapExpired", func(t *testing.T) { runReap(t, store) })
	t.Run("PromoteDue", func(t *testing.T) { runPromoteDue(t, store) })
	t.Run("Publish", func(t *testing.T) { runPublish(t, store) })
	t.Run("Replay", func(t *testing.T) { runReplay(t, store) })
	t.Run("Retain", func(t *testing.T) { runRetain(t, store) })
	t.Run("Admin", func(t *testing.T) { runAdmin(t, store) })
	t.Run("ListJobsOrdering", func(t *testing.T) { runListJobsOrdering(t, store) })
	t.Run("Vacuums", func(t *testing.T) { runVacuums(t, store) })
	t.Run("NukeAll", func(t *testing.T) { runNukeAll(t, store) })
}

// ---- shared helpers -------------------------------------------------------

func enqueueDue(ctx context.Context, t *testing.T, store driver.Store, kind string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	inserted, err := store.Enqueue(ctx, driver.EnqueueParams{
		ID: id, Kind: kind, Payload: json.RawMessage(`{}`),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	return id
}

func dequeueN(ctx context.Context, t *testing.T, store driver.Store, source driver.Source, kind string, limit int, lease time.Duration) []driver.Job {
	t.Helper()
	jobs, err := store.DequeueBatch(ctx, source, driver.DequeueParams{
		Kind: kind, Limit: limit, Lease: lease,
	})
	require.NoError(t, err)
	return jobs
}

func getJob(ctx context.Context, t *testing.T, store driver.Store, source driver.Source, id uuid.UUID) driver.Job {
	t.Helper()
	j, err := store.GetJob(ctx, source, id)
	require.NoError(t, err)
	return *j
}

func jobIDs(jobs []driver.Job) []uuid.UUID {
	ids := make([]uuid.UUID, len(jobs))
	for i, j := range jobs {
		ids[i] = j.ID
	}
	return ids
}

// ---- Enqueue --------------------------------------------------------------

func runEnqueue(t *testing.T, store driver.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("pending vs scheduled", func(t *testing.T) {
		is := require.New(t)
		pendingID := enqueueDue(ctx, t, store, "enq_ps")
		scheduledID := uuid.New()
		inserted, err := store.Enqueue(ctx, driver.EnqueueParams{
			ID: scheduledID, Kind: "enq_ps", Payload: json.RawMessage(`{}`),
			RunAt: time.Now().Add(time.Hour),
		})
		is.NoError(err)
		is.True(inserted)

		is.Equal(driver.StatePending, getJob(ctx, t, store, driver.SourceQueue, pendingID).State)
		is.Equal(driver.StateScheduled, getJob(ctx, t, store, driver.SourceQueue, scheduledID).State)
	})

	t.Run("dedupe live rejects duplicate", func(t *testing.T) {
		is := require.New(t)
		first, err := store.Enqueue(ctx, driver.EnqueueParams{
			ID: uuid.New(), Kind: "enq_live", Payload: json.RawMessage(`{}`), IdempotencyKey: "k_live",
		})
		is.NoError(err)
		is.True(first)
		second, err := store.Enqueue(ctx, driver.EnqueueParams{
			ID: uuid.New(), Kind: "enq_live", Payload: json.RawMessage(`{}`), IdempotencyKey: "k_live",
		})
		is.NoError(err)
		is.False(second, "a live idempotency key rejects the duplicate")
	})

	t.Run("dedupe TTL window survives Ack", func(t *testing.T) {
		is := require.New(t)
		id := uuid.New()
		first, err := store.Enqueue(ctx, driver.EnqueueParams{
			ID: id, Kind: "enq_ttl", Payload: json.RawMessage(`{}`),
			IdempotencyKey: "k_ttl", IdempotencyTTL: 30 * time.Second,
		})
		is.NoError(err)
		is.True(first)

		// Complete the job: the live-job key is freed, but the TTL reservation
		// keeps rejecting the key until it expires.
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "enq_ttl", 1, time.Minute)
		is.Len(leased, 1)
		is.NoError(store.Ack(ctx, leased[0].ID, leased[0].LeaseToken))

		second, err := store.Enqueue(ctx, driver.EnqueueParams{
			ID: uuid.New(), Kind: "enq_ttl", Payload: json.RawMessage(`{}`),
			IdempotencyKey: "k_ttl", IdempotencyTTL: 30 * time.Second,
		})
		is.NoError(err)
		is.False(second, "the TTL window still holds the key after the job succeeded")
	})

	t.Run("RunAt wins over Delay", func(t *testing.T) {
		is := require.New(t)
		id := uuid.New()
		inserted, err := store.Enqueue(ctx, driver.EnqueueParams{
			ID: id, Kind: "enq_runat", Payload: json.RawMessage(`{}`),
			RunAt: time.Now().Add(time.Hour), Delay: time.Millisecond,
		})
		is.NoError(err)
		is.True(inserted)

		got := getJob(ctx, t, store, driver.SourceQueue, id)
		is.Equal(driver.StateScheduled, got.State, "RunAt (far future) wins; Delay would have made it pending")
		is.True(got.RunAt.After(time.Now().Add(30*time.Minute)), "run_at reflects RunAt, not now()+Delay")
	})
}

// ---- Dequeue --------------------------------------------------------------

func runDequeue(t *testing.T, store driver.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("order run_at then id", func(t *testing.T) {
		is := require.New(t)
		base := time.Now()
		// Enqueue with explicit past run_at out of order to prove the sort key.
		// The offsets are hours so every job is unambiguously due-and-pending on
		// the backend clock regardless of any host/DB clock skew.
		late := uuid.New()
		early := uuid.New()
		mid := uuid.New()
		for _, e := range []struct {
			id    uuid.UUID
			runAt time.Time
		}{
			{late, base.Add(-1 * time.Hour)},
			{early, base.Add(-3 * time.Hour)},
			{mid, base.Add(-2 * time.Hour)},
		} {
			_, err := store.Enqueue(ctx, driver.EnqueueParams{
				ID: e.id, Kind: "deq_order", Payload: json.RawMessage(`{}`), RunAt: e.runAt,
			})
			is.NoError(err)
		}
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "deq_order", 10, time.Minute)
		is.Equal([]uuid.UUID{early, mid, late}, jobIDs(leased), "ordered by run_at ascending")
	})

	t.Run("only its source and kind", func(t *testing.T) {
		is := require.New(t)
		aID := enqueueDue(ctx, t, store, "deq_iso_a")
		enqueueDue(ctx, t, store, "deq_iso_b")
		leasedA := dequeueN(ctx, t, store, driver.SourceQueue, "deq_iso_a", 10, time.Minute)
		is.Len(leasedA, 1)
		is.Equal(aID, leasedA[0].ID)
		is.Equal("deq_iso_a", leasedA[0].Kind)

		// Source isolation: an event delivery is never leased by a queue dequeue.
		is.NoError(store.RegisterSubscriber(ctx, driver.Subscriber{Name: "iso_s", EventType: "evt.iso", MaxAttempts: 3}))
		delivered, err := store.Publish(ctx, driver.PublishParams{ID: uuid.New(), Type: "evt.iso", OccurredAt: time.Now(), Payload: json.RawMessage(`{}`)})
		is.NoError(err)
		is.Equal(1, delivered)
		is.Empty(dequeueN(ctx, t, store, driver.SourceQueue, "iso_s", 10, time.Minute), "queue dequeue never leases an event delivery")
		is.Len(dequeueN(ctx, t, store, driver.SourceEvent, "iso_s", 10, time.Minute), 1, "event dequeue leases the delivery")
	})

	t.Run("attempt is 1-based", func(t *testing.T) {
		is := require.New(t)
		enqueueDue(ctx, t, store, "deq_attempt")
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "deq_attempt", 1, time.Minute)
		is.Len(leased, 1)
		is.Equal(1, leased[0].Attempt, "first lease is attempt 1")
	})

	t.Run("lease token is unique and non-nil", func(t *testing.T) {
		is := require.New(t)
		enqueueDue(ctx, t, store, "deq_token")
		enqueueDue(ctx, t, store, "deq_token")
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "deq_token", 2, time.Minute)
		is.Len(leased, 2)
		is.NotEqual(uuid.Nil, leased[0].LeaseToken)
		is.NotEqual(uuid.Nil, leased[1].LeaseToken)
		is.NotEqual(leased[0].LeaseToken, leased[1].LeaseToken, "each lease mints a distinct token")
	})

	t.Run("explicit budget survives runtime override", func(t *testing.T) {
		is := require.New(t)
		id := uuid.New()
		_, err := store.Enqueue(ctx, driver.EnqueueParams{
			ID: id, Kind: "deq_bexp", Payload: json.RawMessage(`{}`),
			MaxAttempts: 7, MaxAttemptsExplicit: true,
		})
		is.NoError(err)
		leased, err := store.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{
			Kind: "deq_bexp", Limit: 1, Lease: time.Minute, DefaultMaxAttempts: 99, OverrideDefault: true,
		})
		is.NoError(err)
		is.Len(leased, 1)
		is.Equal(7, leased[0].MaxAttempts, "an explicit budget is pinned against the runtime default")
	})

	t.Run("runtime default resolves durably on first lease", func(t *testing.T) {
		is := require.New(t)
		id := uuid.New()
		_, err := store.Enqueue(ctx, driver.EnqueueParams{
			ID: id, Kind: "deq_bovr", Payload: json.RawMessage(`{}`), MaxAttempts: 3,
		})
		is.NoError(err)
		first, err := store.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{
			Kind: "deq_bovr", Limit: 1, Lease: time.Minute, DefaultMaxAttempts: 42, OverrideDefault: true,
		})
		is.NoError(err)
		is.Len(first, 1)
		is.Equal(42, first[0].MaxAttempts, "the first lease applies the runtime default")

		is.NoError(store.Release(ctx, first[0].ID, first[0].LeaseToken))
		second, err := store.DequeueBatch(ctx, driver.SourceQueue, driver.DequeueParams{
			Kind: "deq_bovr", Limit: 1, Lease: time.Minute, DefaultMaxAttempts: 7, OverrideDefault: true,
		})
		is.NoError(err)
		is.Len(second, 1)
		is.Equal(42, second[0].MaxAttempts, "the resolved budget is durable; a later, divergent default cannot overwrite it")
	})
}

// ---- Settlement -----------------------------------------------------------

func runSettlement(t *testing.T, store driver.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("Ack retains succeeded", func(t *testing.T) {
		is := require.New(t)
		id := enqueueDue(ctx, t, store, "set_ack")
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "set_ack", 1, time.Minute)
		is.Len(leased, 1)
		is.NoError(store.Ack(ctx, leased[0].ID, leased[0].LeaseToken))
		got := getJob(ctx, t, store, driver.SourceQueue, id)
		is.Equal(driver.StateSucceeded, got.State, "an acked job is retained, not deleted")
		is.False(got.CompletedAt.IsZero())
	})

	t.Run("Reschedule parks scheduled and records history", func(t *testing.T) {
		is := require.New(t)
		id := enqueueDue(ctx, t, store, "set_resched")
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "set_resched", 1, time.Minute)
		is.Len(leased, 1)
		is.NoError(store.Reschedule(ctx, leased[0].ID, leased[0].LeaseToken, time.Hour, "transient"))
		got := getJob(ctx, t, store, driver.SourceQueue, id)
		is.Equal(driver.StateScheduled, got.State)
		is.Equal("transient", got.LastError)
		attempts, err := store.JobAttempts(ctx, driver.SourceQueue, id)
		is.NoError(err)
		is.Len(attempts, 1)
		is.Equal(1, attempts[0].Attempt)
		is.Equal("transient", attempts[0].Error)
	})

	t.Run("Dead records final attempt", func(t *testing.T) {
		is := require.New(t)
		id := enqueueDue(ctx, t, store, "set_dead")
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "set_dead", 1, time.Minute)
		is.Len(leased, 1)
		is.NoError(store.Dead(ctx, leased[0].ID, leased[0].LeaseToken, "fatal"))
		got := getJob(ctx, t, store, driver.SourceQueue, id)
		is.Equal(driver.StateDead, got.State)
		attempts, err := store.JobAttempts(ctx, driver.SourceQueue, id)
		is.NoError(err)
		is.Len(attempts, 1)
		is.Equal("fatal", attempts[0].Error)
	})

	t.Run("Release returns to pending attempt-1 without history", func(t *testing.T) {
		is := require.New(t)
		id := enqueueDue(ctx, t, store, "set_release")
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "set_release", 1, time.Minute)
		is.Len(leased, 1)
		is.Equal(1, leased[0].Attempt)
		is.NoError(store.Release(ctx, leased[0].ID, leased[0].LeaseToken))
		got := getJob(ctx, t, store, driver.SourceQueue, id)
		is.Equal(driver.StatePending, got.State)
		is.Equal(0, got.Attempt, "Release decrements the attempt it did not really spend")
		attempts, err := store.JobAttempts(ctx, driver.SourceQueue, id)
		is.NoError(err)
		is.Empty(attempts, "Release records no attempt history")
	})

	t.Run("stale token is fenced on all five transitions", func(t *testing.T) {
		is := require.New(t)
		id := enqueueDue(ctx, t, store, "set_stale")
		first := dequeueN(ctx, t, store, driver.SourceQueue, "set_stale", 1, reapLease)
		is.Len(first, 1)
		staleToken := first[0].LeaseToken

		time.Sleep(reapWait)
		reaped, _, err := store.ReapExpired(ctx, driver.SourceQueue, []string{"set_stale"}, 5)
		is.NoError(err)
		is.Equal(int64(1), reaped)

		second := dequeueN(ctx, t, store, driver.SourceQueue, "set_stale", 1, time.Minute)
		is.Len(second, 1)
		is.NotEqual(staleToken, second[0].LeaseToken)

		is.True(driver.IsNotFound(store.Ack(ctx, id, staleToken)), "stale Ack is fenced")
		is.True(driver.IsNotFound(store.Reschedule(ctx, id, staleToken, time.Second, "x")), "stale Reschedule is fenced")
		is.True(driver.IsNotFound(store.Dead(ctx, id, staleToken, "x")), "stale Dead is fenced")
		is.True(driver.IsNotFound(store.Release(ctx, id, staleToken)), "stale Release is fenced")
		is.True(driver.IsNotFound(store.ExtendLease(ctx, id, staleToken, time.Second)), "stale ExtendLease is fenced")

		is.NoError(store.Ack(ctx, id, second[0].LeaseToken), "the current token still owns the job")
	})

	t.Run("ExtendLease renews the deadline", func(t *testing.T) {
		is := require.New(t)
		id := enqueueDue(ctx, t, store, "set_extend")
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "set_extend", 1, 100*time.Millisecond)
		is.Len(leased, 1)
		is.NoError(store.ExtendLease(ctx, leased[0].ID, leased[0].LeaseToken, 30*time.Second))
		got := getJob(ctx, t, store, driver.SourceQueue, id)
		is.True(got.LeaseUntil.After(leased[0].LeaseUntil), "the lease deadline moved further out")
	})
}

// ---- ReapExpired ----------------------------------------------------------

func runReap(t *testing.T, store driver.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("expired lease returns to pending with reap_count+1", func(t *testing.T) {
		is := require.New(t)
		id := enqueueDue(ctx, t, store, "reap_pending")
		is.Len(dequeueN(ctx, t, store, driver.SourceQueue, "reap_pending", 1, reapLease), 1)
		time.Sleep(reapWait)
		reaped, killed, err := store.ReapExpired(ctx, driver.SourceQueue, []string{"reap_pending"}, 5)
		is.NoError(err)
		is.Equal(int64(1), reaped)
		is.Equal(int64(0), killed)
		got := getJob(ctx, t, store, driver.SourceQueue, id)
		is.Equal(driver.StatePending, got.State)
		is.Equal(1, got.ReapCount)
	})

	t.Run("reaching maxReaps kills to dead with history", func(t *testing.T) {
		is := require.New(t)
		id := enqueueDue(ctx, t, store, "reap_dead")
		is.Len(dequeueN(ctx, t, store, driver.SourceQueue, "reap_dead", 1, reapLease), 1)
		time.Sleep(reapWait)
		reaped, killed, err := store.ReapExpired(ctx, driver.SourceQueue, []string{"reap_dead"}, 1)
		is.NoError(err)
		is.Equal(int64(1), reaped)
		is.Equal(int64(1), killed)
		got := getJob(ctx, t, store, driver.SourceQueue, id)
		is.Equal(driver.StateDead, got.State)
		attempts, err := store.JobAttempts(ctx, driver.SourceQueue, id)
		is.NoError(err)
		is.Len(attempts, 1, "the reap-to-dead transition records an attempt")
	})

	t.Run("a re-leased job is not reaped (fencing)", func(t *testing.T) {
		is := require.New(t)
		id := enqueueDue(ctx, t, store, "reap_fence")
		is.Len(dequeueN(ctx, t, store, driver.SourceQueue, "reap_fence", 1, reapLease), 1)
		time.Sleep(reapWait)
		reaped, _, err := store.ReapExpired(ctx, driver.SourceQueue, []string{"reap_fence"}, 5)
		is.NoError(err)
		is.Equal(int64(1), reaped)

		// Re-lease with a long lease: the job is active again with a fresh token.
		second := dequeueN(ctx, t, store, driver.SourceQueue, "reap_fence", 1, time.Minute)
		is.Len(second, 1)

		again, _, err := store.ReapExpired(ctx, driver.SourceQueue, []string{"reap_fence"}, 5)
		is.NoError(err)
		is.Equal(int64(0), again, "a freshly re-leased job whose lease is not expired is left alone")
		got := getJob(ctx, t, store, driver.SourceQueue, id)
		is.Equal(driver.StateActive, got.State)
		is.Equal(second[0].LeaseToken, got.LeaseToken, "the live lease token is untouched")
	})
}

// ---- PromoteDue -----------------------------------------------------------

func runPromoteDue(t *testing.T, store driver.Store) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	// Build a scheduled-and-due job by rescheduling with zero delay: run_at is
	// stamped at the backend clock's now(), so the job is immediately due and
	// the test is immune to any host/DB clock skew an absolute host timestamp
	// would suffer.
	makeDueScheduled := func(kind string) uuid.UUID {
		id := enqueueDue(ctx, t, store, kind)
		leased := dequeueN(ctx, t, store, driver.SourceQueue, kind, 1, time.Minute)
		is.Len(leased, 1)
		is.NoError(store.Reschedule(ctx, leased[0].ID, leased[0].LeaseToken, 0, "park as scheduled"))
		is.Equal(driver.StateScheduled, getJob(ctx, t, store, driver.SourceQueue, id).State)
		return id
	}
	aID := makeDueScheduled("promo_a")
	bID := makeDueScheduled("promo_b")

	promoted, err := store.PromoteDue(ctx, driver.SourceQueue, []string{"promo_a"})
	is.NoError(err)
	is.Equal(int64(1), promoted, "only the requested kind is promoted")
	is.Equal(driver.StatePending, getJob(ctx, t, store, driver.SourceQueue, aID).State)
	is.Equal(driver.StateScheduled, getJob(ctx, t, store, driver.SourceQueue, bID).State, "an unrequested kind is left scheduled")
}

// ---- Publish --------------------------------------------------------------

func runPublish(t *testing.T, store driver.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("fan-out one delivery per subscriber", func(t *testing.T) {
		is := require.New(t)
		for _, name := range []string{"fo_a", "fo_b", "fo_c"} {
			is.NoError(store.RegisterSubscriber(ctx, driver.Subscriber{Name: name, EventType: "evt.fanout", MaxAttempts: 3}))
		}
		delivered, err := store.Publish(ctx, driver.PublishParams{ID: uuid.New(), Type: "evt.fanout", OccurredAt: time.Now(), Payload: json.RawMessage(`{}`)})
		is.NoError(err)
		is.Equal(3, delivered, "one delivery per registered subscriber")
	})

	t.Run("subscriber registered after publish receives nothing", func(t *testing.T) {
		is := require.New(t)
		is.NoError(store.RegisterSubscriber(ctx, driver.Subscriber{Name: "snap_a", EventType: "evt.snap", MaxAttempts: 3}))
		delivered, err := store.Publish(ctx, driver.PublishParams{ID: uuid.New(), Type: "evt.snap", OccurredAt: time.Now(), Payload: json.RawMessage(`{}`)})
		is.NoError(err)
		is.Equal(1, delivered, "only the snapshot at publish time is fanned out")

		is.NoError(store.RegisterSubscriber(ctx, driver.Subscriber{Name: "snap_b", EventType: "evt.snap", MaxAttempts: 3}))
		is.Empty(dequeueN(ctx, t, store, driver.SourceEvent, "snap_b", 10, time.Minute), "a later subscriber gets no delivery for the earlier event")
	})

	t.Run("delivery job carries the subscriber kind and a nil payload", func(t *testing.T) {
		is := require.New(t)
		is.NoError(store.RegisterSubscriber(ctx, driver.Subscriber{Name: "pnil_s", EventType: "evt.pnil", MaxAttempts: 3}))
		eventID := uuid.New()
		_, err := store.Publish(ctx, driver.PublishParams{ID: eventID, Type: "evt.pnil", OccurredAt: time.Now(), Payload: json.RawMessage(`{"a":1}`)})
		is.NoError(err)
		leased := dequeueN(ctx, t, store, driver.SourceEvent, "pnil_s", 1, time.Minute)
		is.Len(leased, 1)
		is.Equal(driver.SourceEvent, leased[0].Source)
		is.Equal("pnil_s", leased[0].Kind)
		is.Equal(eventID, leased[0].EventID)
		is.Nil(leased[0].Payload, "the delivery job carries no payload; the body lives in the ledger")
	})

	t.Run("dequeue rehydrates the full event record", func(t *testing.T) {
		is := require.New(t)
		is.NoError(store.RegisterSubscriber(ctx, driver.Subscriber{Name: "cmpl_s", EventType: "evt.complete", MaxAttempts: 3}))
		occurred := time.Now().Truncate(time.Microsecond)
		tenant := uuid.New()
		eventID := uuid.New()
		_, err := store.Publish(ctx, driver.PublishParams{
			ID: eventID, Type: "evt.complete", TenantID: tenant,
			AggregateType: "order", AggregateID: "agg-123", Version: 7,
			OccurredAt: occurred, Payload: json.RawMessage(`{"k":"v"}`),
			Meta: map[string]string{"m1": "v1"}, TraceID: "trace-abc", SpanID: "span-def", TraceFlags: 1,
		})
		is.NoError(err)

		leased := dequeueN(ctx, t, store, driver.SourceEvent, "cmpl_s", 1, time.Minute)
		is.Len(leased, 1)
		rec := leased[0].Event
		is.NotNil(rec, "the event is rehydrated on dequeue")
		is.Equal(eventID, rec.ID)
		is.Equal("evt.complete", rec.Type)
		is.Equal(tenant, rec.TenantID)
		is.Equal("order", rec.AggregateType)
		is.Equal("agg-123", rec.AggregateID)
		is.Equal(int64(7), rec.Version)
		is.True(occurred.Equal(rec.OccurredAt), "OccurredAt round-trips")
		is.JSONEq(`{"k":"v"}`, string(rec.Payload))
		is.Equal(map[string]string{"m1": "v1"}, rec.Meta)
		is.Equal("trace-abc", rec.TraceID)
		is.Equal("span-def", rec.SpanID)
		is.Equal(int16(1), rec.TraceFlags)
	})

	t.Run("rehydrated meta is never nil", func(t *testing.T) {
		is := require.New(t)
		is.NoError(store.RegisterSubscriber(ctx, driver.Subscriber{Name: "metanil_s", EventType: "evt.metanil", MaxAttempts: 3}))
		_, err := store.Publish(ctx, driver.PublishParams{ID: uuid.New(), Type: "evt.metanil", OccurredAt: time.Now(), Payload: json.RawMessage(`{}`)})
		is.NoError(err)
		leased := dequeueN(ctx, t, store, driver.SourceEvent, "metanil_s", 1, time.Minute)
		is.Len(leased, 1)
		is.NotNil(leased[0].Event.Meta, "an event with no meta rehydrates as an empty, non-nil map")
		is.Empty(leased[0].Event.Meta)
	})
}

// ---- Replay ---------------------------------------------------------------

func runReplay(t *testing.T, store driver.Store) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	is.NoError(store.RegisterSubscriber(ctx, driver.Subscriber{Name: "rp_a", EventType: "evt.replay", MaxAttempts: 3}))
	is.NoError(store.RegisterSubscriber(ctx, driver.Subscriber{Name: "rp_b", EventType: "evt.replay", MaxAttempts: 3}))

	base := time.Now().Add(-2 * time.Hour)
	e1 := uuid.New()
	e2 := uuid.New()
	_, err := store.Publish(ctx, driver.PublishParams{ID: e1, Type: "evt.replay", OccurredAt: base, Payload: json.RawMessage(`{}`)})
	is.NoError(err)
	_, err = store.Publish(ctx, driver.PublishParams{ID: e2, Type: "evt.replay", OccurredAt: base.Add(time.Hour), Payload: json.RawMessage(`{}`)})
	is.NoError(err)

	replay := func(f driver.ReplayFilter) int64 {
		n, rerr := store.Replay(ctx, f)
		is.NoError(rerr)
		return n
	}
	is.Equal(int64(2), replay(driver.ReplayFilter{Subscriber: "rp_a"}), "subscriber filter: one replay per matching event")
	is.Equal(int64(2), replay(driver.ReplayFilter{EventType: "evt.replay", Limit: 1}), "limit bounds the ledger events, fanning out to both subscribers")
	is.Equal(int64(2), replay(driver.ReplayFilter{EventType: "evt.replay", Since: base.Add(30 * time.Minute)}), "since selects only the later event")
	is.Equal(int64(2), replay(driver.ReplayFilter{EventType: "evt.replay", Until: base.Add(30 * time.Minute)}), "until selects only the earlier event")
	is.Equal(int64(2), replay(driver.ReplayFilter{EventID: e1}), "event id selects a single event")

	all, _, err := store.ListJobs(ctx, driver.SourceEvent, driver.JobFilter{Kind: "rp_a"}, 0, 0)
	is.NoError(err)
	var originals int
	var sawReplay bool
	for _, j := range all {
		if j.Replay {
			sawReplay = true
		} else {
			originals++
		}
	}
	is.Equal(2, originals, "the original publish deliveries are left intact by replay")
	is.True(sawReplay, "replayed deliveries are flagged Replay=true")
}

// ---- Retain ---------------------------------------------------------------

func runRetain(t *testing.T, store driver.Store) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	is.NoError(store.RegisterSubscriber(ctx, driver.Subscriber{Name: "ret_s", EventType: "evt.retain", MaxAttempts: 3}))
	eventID := uuid.New()
	_, err := store.Publish(ctx, driver.PublishParams{ID: eventID, Type: "evt.retain", OccurredAt: time.Now().Add(-time.Hour), Payload: json.RawMessage(`{}`)})
	is.NoError(err)
	cutoff := time.Now()

	// A pending (in-flight) delivery blocks retention.
	removed, err := store.Retain(ctx, cutoff, 10)
	is.NoError(err)
	is.Equal(int64(0), removed, "an event with an in-flight delivery is skipped")

	leased := dequeueN(ctx, t, store, driver.SourceEvent, "ret_s", 1, time.Minute)
	is.Len(leased, 1)
	is.NoError(store.Ack(ctx, leased[0].ID, leased[0].LeaseToken))

	// All deliveries terminal → the event is retainable and cascades.
	removed, err = store.Retain(ctx, cutoff, 10)
	is.NoError(err)
	is.Equal(int64(1), removed, "an event whose deliveries are all terminal is trimmed")
	_, err = store.GetEvent(ctx, eventID)
	is.True(driver.IsNotFound(err), "the ledger event is gone")
	_, err = store.GetJob(ctx, driver.SourceEvent, leased[0].ID)
	is.True(driver.IsNotFound(err), "the terminal delivery cascades with its event")
}

// ---- Admin ----------------------------------------------------------------

func runAdmin(t *testing.T, store driver.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("KindDepths counts per state", func(t *testing.T) {
		is := require.New(t)
		enqueueDue(ctx, t, store, "adm_depth")
		enqueueDue(ctx, t, store, "adm_depth")
		_, err := store.Enqueue(ctx, driver.EnqueueParams{ID: uuid.New(), Kind: "adm_depth", Payload: json.RawMessage(`{}`), RunAt: time.Now().Add(time.Hour)})
		is.NoError(err)
		depths, err := store.KindDepths(ctx, driver.SourceQueue)
		is.NoError(err)
		is.Equal(int64(2), depths["adm_depth"].Pending)
		is.Equal(int64(1), depths["adm_depth"].Scheduled)
	})

	t.Run("GetJob returns not-found for a missing id", func(t *testing.T) {
		is := require.New(t)
		_, err := store.GetJob(ctx, driver.SourceQueue, uuid.New())
		is.True(driver.IsNotFound(err))
	})

	t.Run("RetryJob clears attempt and reap_count", func(t *testing.T) {
		is := require.New(t)
		id := enqueueDue(ctx, t, store, "adm_retry")
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "adm_retry", 1, time.Minute)
		is.Len(leased, 1)
		is.NoError(store.Dead(ctx, leased[0].ID, leased[0].LeaseToken, "boom"))
		is.NoError(store.RetryJob(ctx, driver.SourceQueue, id))
		got := getJob(ctx, t, store, driver.SourceQueue, id)
		is.Equal(driver.StatePending, got.State)
		is.Equal(0, got.Attempt)
		is.Equal(0, got.ReapCount)
	})

	t.Run("Pause and Resume honor run_at", func(t *testing.T) {
		is := require.New(t)
		pastID := enqueueDue(ctx, t, store, "adm_pause")
		futureID := uuid.New()
		_, err := store.Enqueue(ctx, driver.EnqueueParams{ID: futureID, Kind: "adm_pause", Payload: json.RawMessage(`{}`), RunAt: time.Now().Add(time.Hour)})
		is.NoError(err)

		is.NoError(store.PauseJob(ctx, driver.SourceQueue, pastID))
		is.NoError(store.PauseJob(ctx, driver.SourceQueue, futureID))
		is.Equal(driver.StatePaused, getJob(ctx, t, store, driver.SourceQueue, pastID).State)
		is.Equal(driver.StatePaused, getJob(ctx, t, store, driver.SourceQueue, futureID).State)

		is.NoError(store.ResumeJob(ctx, driver.SourceQueue, pastID))
		is.NoError(store.ResumeJob(ctx, driver.SourceQueue, futureID))
		is.Equal(driver.StatePending, getJob(ctx, t, store, driver.SourceQueue, pastID).State, "a due job resumes to pending")
		is.Equal(driver.StateScheduled, getJob(ctx, t, store, driver.SourceQueue, futureID).State, "a future job resumes to scheduled")
	})

	t.Run("DeleteJob never removes an active job via a wrong state", func(t *testing.T) {
		is := require.New(t)
		id := enqueueDue(ctx, t, store, "adm_del")
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "adm_del", 1, time.Minute)
		is.Len(leased, 1)
		err := store.DeleteJob(ctx, driver.SourceQueue, id, driver.StatePending)
		is.True(driver.IsNotFound(err), "an active job is not deletable through its non-active state")
		is.Equal(driver.StateActive, getJob(ctx, t, store, driver.SourceQueue, id).State)
	})
}

// ---- ListJobs ordering ----------------------------------------------------

func runListJobsOrdering(t *testing.T, store driver.Store) {
	t.Helper()
	ctx := context.Background()

	list := func(t *testing.T, kind string, state driver.JobState) []uuid.UUID {
		t.Helper()
		jobs, _, err := store.ListJobs(ctx, driver.SourceQueue, driver.JobFilter{Kind: kind, State: state}, 0, 0)
		require.NoError(t, err)
		return jobIDs(jobs)
	}

	enqueueSpaced := func(t *testing.T, kind string, n int) []uuid.UUID {
		t.Helper()
		ids := make([]uuid.UUID, n)
		for i := range ids {
			ids[i] = enqueueDue(ctx, t, store, kind)
			time.Sleep(tick)
		}
		return ids
	}

	t.Run("no state filter orders by enqueued_at descending", func(t *testing.T) {
		is := require.New(t)
		ids := enqueueSpaced(t, "ord_none", 3)
		is.Equal([]uuid.UUID{ids[2], ids[1], ids[0]}, list(t, "ord_none", ""), "newest first")
	})

	t.Run("pending orders by enqueued_at ascending", func(t *testing.T) {
		is := require.New(t)
		ids := enqueueSpaced(t, "ord_pending", 3)
		is.Equal(ids, list(t, "ord_pending", driver.StatePending), "oldest first")
	})

	t.Run("dead orders by enqueued_at ascending regardless of death order", func(t *testing.T) {
		is := require.New(t)
		ids := enqueueSpaced(t, "ord_dead", 3)
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "ord_dead", 3, time.Minute)
		is.Len(leased, 3)
		tokens := make(map[uuid.UUID]uuid.UUID, 3)
		for _, j := range leased {
			tokens[j.ID] = j.LeaseToken
		}
		for _, i := range []int{2, 0, 1} {
			is.NoError(store.Dead(ctx, ids[i], tokens[ids[i]], "boom"))
		}
		is.Equal(ids, list(t, "ord_dead", driver.StateDead), "oldest enqueued first, not death order")
	})

	t.Run("scheduled orders by run_at ascending", func(t *testing.T) {
		is := require.New(t)
		base := time.Now()
		id0, id1, id2 := uuid.New(), uuid.New(), uuid.New()
		for _, e := range []struct {
			id    uuid.UUID
			delay time.Duration
		}{{id0, 3 * time.Hour}, {id1, time.Hour}, {id2, 2 * time.Hour}} {
			_, err := store.Enqueue(ctx, driver.EnqueueParams{ID: e.id, Kind: "ord_scheduled", Payload: json.RawMessage(`{}`), RunAt: base.Add(e.delay)})
			is.NoError(err)
		}
		is.Equal([]uuid.UUID{id1, id2, id0}, list(t, "ord_scheduled", driver.StateScheduled), "soonest run_at first")
	})

	t.Run("paused orders by run_at ascending", func(t *testing.T) {
		is := require.New(t)
		base := time.Now()
		id0, id1, id2 := uuid.New(), uuid.New(), uuid.New()
		for _, e := range []struct {
			id    uuid.UUID
			delay time.Duration
		}{{id0, 3 * time.Hour}, {id1, time.Hour}, {id2, 2 * time.Hour}} {
			_, err := store.Enqueue(ctx, driver.EnqueueParams{ID: e.id, Kind: "ord_paused", Payload: json.RawMessage(`{}`), RunAt: base.Add(e.delay)})
			is.NoError(err)
			is.NoError(store.PauseJob(ctx, driver.SourceQueue, e.id))
		}
		is.Equal([]uuid.UUID{id1, id2, id0}, list(t, "ord_paused", driver.StatePaused), "soonest run_at first")
	})

	t.Run("active orders by lease_until ascending", func(t *testing.T) {
		is := require.New(t)
		for range 3 {
			enqueueDue(ctx, t, store, "ord_active")
		}
		var short, mid, long uuid.UUID
		for _, step := range []struct {
			lease time.Duration
			dst   *uuid.UUID
		}{{30 * time.Second, &long}, {10 * time.Second, &short}, {20 * time.Second, &mid}} {
			leased := dequeueN(ctx, t, store, driver.SourceQueue, "ord_active", 1, step.lease)
			is.Len(leased, 1)
			*step.dst = leased[0].ID
		}
		is.Equal([]uuid.UUID{short, mid, long}, list(t, "ord_active", driver.StateActive), "soonest lease_until first")
	})

	t.Run("succeeded orders by completed_at descending", func(t *testing.T) {
		is := require.New(t)
		for range 3 {
			enqueueDue(ctx, t, store, "ord_succeeded")
		}
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "ord_succeeded", 3, time.Minute)
		is.Len(leased, 3)
		for _, j := range leased {
			is.NoError(store.Ack(ctx, j.ID, j.LeaseToken))
			time.Sleep(tick)
		}
		is.Equal([]uuid.UUID{leased[2].ID, leased[1].ID, leased[0].ID}, list(t, "ord_succeeded", driver.StateSucceeded), "most recently completed first")
	})
}

// ---- Vacuums --------------------------------------------------------------

func runVacuums(t *testing.T, store driver.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("stats retention zero is a no-op", func(t *testing.T) {
		is := require.New(t)
		enqueueDue(ctx, t, store, "vac_stats") // writes a daily stat row
		removed, err := store.VacuumStats(ctx, driver.SourceQueue, 0)
		is.NoError(err)
		is.Equal(int64(0), removed, "a non-positive retention retains everything")
		removed, err = store.VacuumStats(ctx, driver.SourceQueue, 30*24*time.Hour)
		is.NoError(err)
		is.Equal(int64(0), removed, "today's counters are within any positive retention")
	})

	t.Run("expired idempotency keys are trimmed", func(t *testing.T) {
		is := require.New(t)
		_, err := store.Enqueue(ctx, driver.EnqueueParams{
			ID: uuid.New(), Kind: "vac_idem", Payload: json.RawMessage(`{}`),
			IdempotencyKey: "k_vac", IdempotencyTTL: 30 * time.Millisecond,
		})
		is.NoError(err)
		time.Sleep(120 * time.Millisecond)
		removed, err := store.VacuumIdempotency(ctx, driver.SourceQueue)
		is.NoError(err)
		is.GreaterOrEqual(removed, int64(1), "the expired reservation is removed")
	})

	t.Run("completed retention zero is a no-op; a positive one trims", func(t *testing.T) {
		is := require.New(t)
		id := enqueueDue(ctx, t, store, "vac_done")
		leased := dequeueN(ctx, t, store, driver.SourceQueue, "vac_done", 1, time.Minute)
		is.Len(leased, 1)
		is.NoError(store.Ack(ctx, leased[0].ID, leased[0].LeaseToken))

		removed, err := store.VacuumCompleted(ctx, driver.SourceQueue, 0)
		is.NoError(err)
		is.Equal(int64(0), removed, "a non-positive retention keeps succeeded jobs forever")

		time.Sleep(50 * time.Millisecond)
		removed, err = store.VacuumCompleted(ctx, driver.SourceQueue, time.Millisecond)
		is.NoError(err)
		is.GreaterOrEqual(removed, int64(1), "a job completed before the retention is trimmed")
		_, err = store.GetJob(ctx, driver.SourceQueue, id)
		is.True(driver.IsNotFound(err))
	})
}

// ---- NukeAll --------------------------------------------------------------

func runNukeAll(t *testing.T, store driver.Store) {
	t.Helper()
	ctx := context.Background()
	is := require.New(t)

	queueID := enqueueDue(ctx, t, store, "adm_nuke_q")
	is.NoError(store.RegisterSubscriber(ctx, driver.Subscriber{Name: "nuke_s", EventType: "evt.nuke", MaxAttempts: 3}))
	eventID := uuid.New()
	delivered, err := store.Publish(ctx, driver.PublishParams{ID: eventID, Type: "evt.nuke", OccurredAt: time.Now(), Payload: json.RawMessage(`{}`)})
	is.NoError(err)
	is.Equal(1, delivered)

	report, err := store.NukeAll(ctx, driver.SourceQueue)
	is.NoError(err)
	is.GreaterOrEqual(report.Jobs, int64(1))

	_, err = store.GetJob(ctx, driver.SourceQueue, queueID)
	is.True(driver.IsNotFound(err), "queue jobs are wiped")
	_, err = store.GetEvent(ctx, eventID)
	is.NoError(err, "NukeAll(queue) leaves the event ledger intact")
	is.Len(dequeueN(ctx, t, store, driver.SourceEvent, "nuke_s", 10, time.Minute), 1, "NukeAll(queue) leaves event deliveries intact")
}
