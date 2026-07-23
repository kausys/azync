package event

import (
	"context"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// Delivery is the cross-cutting metadata of one event delivery. Handlers receive
// the decoded domain event as their argument; everything about the delivery
// itself — which subscriber it targets, which attempt this is, the ledger
// identifiers and the publish-time annotations — travels on the context and is
// read through the package accessors (Attempt, IsRetry, EventID, Metadata, ...).
//
// It deliberately carries no payload: the payload is already decoded into the
// handler's typed argument. Delivery is at-least-once and deliberately
// unordered, so handlers must deduplicate their effects by (EventID,
// Subscriber). AggregateID and Version are exposed for consumers that reject
// stale work.
type Delivery struct {
	// ID is the ledger event id.
	ID uuid.UUID
	// Type is the event type.
	Type string
	// TenantID scopes the event to a tenant; uuid.Nil means global/unset.
	TenantID uuid.UUID
	// AggregateType and AggregateID identify the source aggregate, if any.
	AggregateType string
	AggregateID   string
	// Version is the aggregate version this event advanced to.
	Version int64
	// OccurredAt is the domain time the event happened.
	OccurredAt time.Time
	// Meta carries the string-valued annotations attached at publish time.
	Meta map[string]string
	// Subscriber is the name of the consumer this delivery targets.
	Subscriber string
	// Attempt is the 1-based delivery attempt (first delivery is attempt 1).
	Attempt int
	// MaxAttempts is the resolved retry budget for this delivery.
	MaxAttempts int
	// Replay is true for deliveries created by Manager.Replay rather than the
	// original publish fan-out.
	Replay bool
}

// deliveryKey is the private context key carrying the Delivery of the delivery
// in flight. A single value per package holds the whole struct; the accessors
// read their field from it.
type deliveryKey struct{}

// NewContext returns a copy of parent carrying d, so a handler can be exercised
// in isolation in a test without a running worker: build a Delivery, attach it,
// and the accessors below read from it exactly as they do in production.
func NewContext(parent context.Context, d Delivery) context.Context {
	return context.WithValue(parent, deliveryKey{}, d)
}

// DeliveryFromContext returns the Delivery carried by ctx and whether one was
// present. Outside a delivery (a ctx that never passed through a worker) it
// returns the zero Delivery and false.
func DeliveryFromContext(ctx context.Context) (Delivery, bool) {
	d, ok := ctx.Value(deliveryKey{}).(Delivery)
	return d, ok
}

func deliveryFromContext(ctx context.Context) Delivery {
	d, _ := DeliveryFromContext(ctx)
	return d
}

// deliveryFrom rehydrates the Delivery metadata from a leased delivery: every
// ledger field from job.Event, plus the delivery's own Subscriber, Attempt,
// MaxAttempts and Replay.
func deliveryFrom(job driver.Job) Delivery {
	e := job.Event
	return Delivery{
		ID:            e.ID,
		Type:          e.Type,
		TenantID:      e.TenantID,
		AggregateType: e.AggregateType,
		AggregateID:   e.AggregateID,
		Version:       e.Version,
		OccurredAt:    e.OccurredAt,
		Meta:          e.Meta,
		Subscriber:    job.Kind,
		Attempt:       job.Attempt,
		MaxAttempts:   job.MaxAttempts,
		Replay:        job.Replay,
	}
}

// The accessors below read the delivery metadata a handler is running under.
// Each is zero-value-safe: called on a context that did not come from a delivery
// (for example in a unit test that forgot NewContext, or from unrelated code),
// it returns the zero value of its result rather than panicking.

// Attempt is the 1-based delivery attempt; the first delivery is attempt 1.
// Zero outside a delivery.
func Attempt(ctx context.Context) int { return deliveryFromContext(ctx).Attempt }

// MaxAttempts is the resolved retry budget for the delivery. Zero outside a
// delivery.
func MaxAttempts(ctx context.Context) int { return deliveryFromContext(ctx).MaxAttempts }

// IsRetry reports whether this is a re-delivery (Attempt > 1). False outside a
// delivery.
func IsRetry(ctx context.Context) bool { return deliveryFromContext(ctx).Attempt > 1 }

// Metadata returns the string-valued annotations attached at publish time. Nil
// outside a delivery.
func Metadata(ctx context.Context) map[string]string { return deliveryFromContext(ctx).Meta }

// EventID is the ledger event id of the delivery. uuid.Nil outside a delivery.
func EventID(ctx context.Context) uuid.UUID { return deliveryFromContext(ctx).ID }

// Type is the event type of the delivery. Empty outside a delivery.
func Type(ctx context.Context) string { return deliveryFromContext(ctx).Type }

// OccurredAt is the domain time the event happened. Zero outside a delivery.
func OccurredAt(ctx context.Context) time.Time { return deliveryFromContext(ctx).OccurredAt }

// SubscriberName is the name of the subscriber processing the delivery. Empty
// outside a delivery.
func SubscriberName(ctx context.Context) string { return deliveryFromContext(ctx).Subscriber }

// IsReplay reports whether the delivery was created by Manager.Replay rather
// than the original publish fan-out. False outside a delivery.
func IsReplay(ctx context.Context) bool { return deliveryFromContext(ctx).Replay }

// AggregateType is the source aggregate type, if any. Empty outside a delivery.
func AggregateType(ctx context.Context) string { return deliveryFromContext(ctx).AggregateType }

// AggregateID is the source aggregate id, if any. Empty outside a delivery.
func AggregateID(ctx context.Context) string { return deliveryFromContext(ctx).AggregateID }

// Version is the aggregate version this event advanced to. Zero outside a
// delivery.
func Version(ctx context.Context) int64 { return deliveryFromContext(ctx).Version }

// TenantID scopes the event to a tenant; uuid.Nil means global/unset or a
// context outside a delivery.
func TenantID(ctx context.Context) uuid.UUID { return deliveryFromContext(ctx).TenantID }
