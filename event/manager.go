package event

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// Manager is the event administration surface: inspection, retry, replay,
// retention and ops projections. Pure library — no auth, no HTTP; embed it
// behind your own ops endpoints. It operates the event delivery source only and
// deliberately has no pause, deletion or purge operations.
type Manager struct {
	store driver.Store
}

// JobState is the wire state of a delivery — the same values the driver
// persists for every job source.
type JobState = driver.JobState

// Delivery lifecycle states, re-exported from the driver contract.
const (
	StatePending   = driver.StatePending
	StateScheduled = driver.StateScheduled
	StateActive    = driver.StateActive
	StateDead      = driver.StateDead
	StatePaused    = driver.StatePaused
	StateSucceeded = driver.StateSucceeded
)

// EventFilter selects ledger events for the admin list.
type EventFilter = driver.EventFilter

// ReplayFilter selects ledger events to re-fan-out into fresh deliveries.
type ReplayFilter = driver.ReplayFilter

// SubscriberView is one subscriber registration projected for the ops surface.
type SubscriberView = driver.SubscriberView

// OpsStats is the event ledger admin summary.
type OpsStats = driver.OpsStats

// AttemptError is one recorded failure in a delivery's retry history.
type AttemptError = driver.AttemptError

// DeadFilter scopes a bulk dead-delivery retry to a single subscriber. An empty
// Subscriber targets every subscriber.
type DeadFilter struct {
	Subscriber string
}

// DeliveryFilter selects delivery jobs for the admin list. Zero fields are
// unbounded.
type DeliveryFilter struct {
	EventID    uuid.UUID
	Subscriber string
	State      JobState
}

// Stats summarizes the durable event and delivery ledger: the ledger event
// count, the instantaneous delivery depths summed across every subscriber, and
// the registration count.
type Stats struct {
	Events      int64
	Pending     int64
	Scheduled   int64
	Active      int64
	Paused      int64
	Succeeded   int64
	Dead        int64
	Subscribers int64
}

// ReplayReport summarizes a Replay: how many fresh deliveries were created.
type ReplayReport struct {
	Created int64
}

// EventView is the admin projection of one ledger event.
type EventView struct {
	ID            uuid.UUID
	Type          string
	TenantID      uuid.UUID
	AggregateType string
	AggregateID   string
	Version       int64
	OccurredAt    time.Time
	// DispatchedAt is zero when the event has no deliveries. Otherwise it equals
	// OccurredAt — publish creates deliveries atomically, so "dispatched" means
	// "has at least one delivery snapshot".
	DispatchedAt time.Time
	TraceID      string
	SpanID       string
	TraceFlags   int16
	Meta         map[string]string
	Payload      json.RawMessage
}

// EventListPage is one page of events for the admin list.
type EventListPage struct {
	Items []EventView
	Page  int
	Size  int
	Total int64
}

// DeliveryView is the admin projection of one delivery job. It deliberately
// carries no event type: a listed delivery exposes only the job row, and the
// event type lives in the ledger, joined only at dequeue — look the event up
// by EventID when the type is needed.
type DeliveryView struct {
	ID          uuid.UUID
	EventID     uuid.UUID
	Subscriber  string
	State       JobState
	Attempt     int
	MaxAttempts int
	Replay      bool
	AvailableAt time.Time
	LastError   string
	CreatedAt   time.Time
	FailedAt    time.Time
	CompletedAt time.Time
}

// DeliveryListPage is one page of deliveries for the admin list.
type DeliveryListPage struct {
	Items []DeliveryView
	Page  int
	Size  int
	Total int64
}

// Stats returns the ledger event count, the delivery depths summed across every
// subscriber, and the subscriber registration count.
//
// The result is not an atomic snapshot: it is assembled from three separate
// reads (delivery depths, ops counts, and the ledger event total), so a publish
// or delivery that lands between them can leave the returned counters mutually
// inconsistent by a small margin. It is intended for dashboards and ops views,
// not for exact accounting.
func (m *Manager) Stats(ctx context.Context) (Stats, error) {
	depths, err := m.store.KindDepths(ctx, driver.SourceEvent)
	if err != nil {
		return Stats{}, err
	}
	ops, err := m.store.OpsStats(ctx)
	if err != nil {
		return Stats{}, err
	}
	// KindDepths counts deliveries and OpsStats counts registrations; neither
	// exposes the ledger event count, so it comes from the ListEvents total.
	_, events, err := m.store.ListEvents(ctx, driver.EventFilter{}, 0, 1)
	if err != nil {
		return Stats{}, err
	}
	out := Stats{Events: events, Subscribers: ops.Subscribers}
	for _, d := range depths {
		out.Pending += d.Pending
		out.Scheduled += d.Scheduled
		out.Active += d.Active
		out.Paused += d.Paused
		out.Succeeded += d.Succeeded
		out.Dead += d.Dead
	}
	return out, nil
}

// Retry re-enqueues one dead delivery with a fresh attempt budget.
func (m *Manager) Retry(ctx context.Context, deliveryID uuid.UUID) error {
	return m.store.RetryJob(ctx, driver.SourceEvent, deliveryID)
}

// RetryDead re-enqueues every dead delivery matching the filter and returns the
// count. An empty Subscriber targets every subscriber.
func (m *Manager) RetryDead(ctx context.Context, filter DeadFilter) (int64, error) {
	return m.store.RetryAllDead(ctx, driver.SourceEvent, filter.Subscriber)
}

// Replay re-fans-out ledger events matching the filter into fresh deliveries
// flagged Replay, without overwriting the original delivery history.
func (m *Manager) Replay(ctx context.Context, filter ReplayFilter) (ReplayReport, error) {
	created, err := m.store.Replay(ctx, filter)
	if err != nil {
		return ReplayReport{}, err
	}
	return ReplayReport{Created: created}, nil
}

// Retain deletes ledger events occurring before the cutoff whose deliveries
// have all reached a terminal state (succeeded or dead), cascading to those
// deliveries, and returns the number of events removed. Events with any
// in-flight delivery (pending, scheduled, active or paused) are skipped.
func (m *Manager) Retain(ctx context.Context, before time.Time, limit int) (int64, error) {
	return m.store.Retain(ctx, before, limit)
}

// List returns one page of events (page is 0-based, default size 50).
func (m *Manager) List(ctx context.Context, filter EventFilter, page, size int) (EventListPage, error) {
	if size <= 0 {
		size = 50
	}
	if page < 0 {
		page = 0
	}
	rows, total, err := m.store.ListEvents(ctx, filter, page*size, size)
	if err != nil {
		return EventListPage{}, err
	}
	items := make([]EventView, 0, len(rows))
	for _, row := range rows {
		items = append(items, toEventView(row))
	}
	return EventListPage{Items: items, Page: page, Size: size, Total: total}, nil
}

// Get returns one event or nil when it does not exist.
func (m *Manager) Get(ctx context.Context, id uuid.UUID) (*EventView, error) {
	row, err := m.store.GetEvent(ctx, id)
	if err != nil {
		if driver.IsNotFound(err) {
			return nil, nil //nolint:nilnil // absence is not an error for the admin surface
		}
		return nil, err
	}
	view := toEventView(*row)
	return &view, nil
}

// OpsStats returns aggregate counts for the ops surface.
func (m *Manager) OpsStats(ctx context.Context) (OpsStats, error) {
	return m.store.OpsStats(ctx)
}

// ListSubscribers returns the subscriber catalog, optionally filtered by event
// type.
func (m *Manager) ListSubscribers(ctx context.Context, eventType string) ([]SubscriberView, error) {
	return m.store.ListSubscriberViews(ctx, eventType)
}

// ListDeliveries returns one page of deliveries (page is 0-based, default size
// 50).
//
// The unified JobFilter has no EventID predicate, so a filter that scopes by
// EventID loads the full (subscriber, state) match set from the driver and
// filters, then paginates, in memory — the documented cost of scoping by event.
// Without an EventID, pagination and totals are delegated straight to the
// driver.
func (m *Manager) ListDeliveries(ctx context.Context, filter DeliveryFilter, page, size int) (DeliveryListPage, error) {
	if size <= 0 {
		size = 50
	}
	if page < 0 {
		page = 0
	}
	jf := driver.JobFilter{Kind: filter.Subscriber, State: filter.State}

	if filter.EventID == uuid.Nil {
		rows, total, err := m.store.ListJobs(ctx, driver.SourceEvent, jf, page*size, size)
		if err != nil {
			return DeliveryListPage{}, err
		}
		return DeliveryListPage{Items: toDeliveryViews(rows), Page: page, Size: size, Total: total}, nil
	}

	all, _, err := m.store.ListJobs(ctx, driver.SourceEvent, jf, 0, 0)
	if err != nil {
		return DeliveryListPage{}, err
	}
	matched := make([]driver.Job, 0, len(all))
	for _, j := range all {
		if j.EventID == filter.EventID {
			matched = append(matched, j)
		}
	}
	total := int64(len(matched))
	offset := min(page*size, len(matched))
	matched = matched[offset:]
	if size < len(matched) {
		matched = matched[:size]
	}
	return DeliveryListPage{Items: toDeliveryViews(matched), Page: page, Size: size, Total: total}, nil
}

// DeliveryAttempts returns one delivery's failure history, oldest attempt first.
func (m *Manager) DeliveryAttempts(ctx context.Context, deliveryID uuid.UUID) ([]AttemptError, error) {
	return m.store.JobAttempts(ctx, driver.SourceEvent, deliveryID)
}

func toDeliveryViews(rows []driver.Job) []DeliveryView {
	out := make([]DeliveryView, 0, len(rows))
	for _, j := range rows {
		out = append(out, toDeliveryView(j))
	}
	return out
}

func toDeliveryView(j driver.Job) DeliveryView {
	return DeliveryView{
		ID:          j.ID,
		EventID:     j.EventID,
		Subscriber:  j.Kind,
		State:       j.State,
		Attempt:     j.Attempt,
		MaxAttempts: j.MaxAttempts,
		Replay:      j.Replay,
		AvailableAt: j.RunAt,
		LastError:   j.LastError,
		CreatedAt:   j.EnqueuedAt,
		FailedAt:    j.FailedAt,
		CompletedAt: j.CompletedAt,
	}
}

func toEventView(row driver.EventAdminRow) EventView {
	payload := row.Payload
	if payload == nil {
		payload = json.RawMessage(`{}`)
	}
	return EventView{
		ID:            row.ID,
		Type:          row.Type,
		TenantID:      row.TenantID,
		AggregateType: row.AggregateType,
		AggregateID:   row.AggregateID,
		Version:       row.Version,
		OccurredAt:    row.OccurredAt,
		DispatchedAt:  row.DispatchedAt,
		TraceID:       row.TraceID,
		SpanID:        row.SpanID,
		TraceFlags:    row.TraceFlags,
		Meta:          row.Meta,
		Payload:       payload,
	}
}
