package driver

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// UnimplementedStore is an embeddable no-op [Store]: every method returns zero
// values and ErrNotSupported. Embed it in a third-party driver so that adding a
// method to the Store contract in a later version does not break compilation —
// the driver keeps satisfying Store, with new methods reporting ErrNotSupported
// until implemented.
//
// A partial driver overrides the methods it supports and inherits the rest.
type UnimplementedStore struct{}

var _ Store = UnimplementedStore{}

// Enqueue reports ErrNotSupported.
func (UnimplementedStore) Enqueue(context.Context, EnqueueParams) (bool, error) {
	return false, ErrNotSupported
}

// Publish reports ErrNotSupported.
func (UnimplementedStore) Publish(context.Context, PublishParams) (int, error) {
	return 0, ErrNotSupported
}

// RegisterSubscriber reports ErrNotSupported.
func (UnimplementedStore) RegisterSubscriber(context.Context, Subscriber) error {
	return ErrNotSupported
}

// Subscribers reports ErrNotSupported.
func (UnimplementedStore) Subscribers(context.Context, string) ([]Subscriber, error) {
	return nil, ErrNotSupported
}

// DequeueBatch reports ErrNotSupported.
func (UnimplementedStore) DequeueBatch(context.Context, Source, DequeueParams) ([]Job, error) {
	return nil, ErrNotSupported
}

// Ack reports ErrNotSupported.
func (UnimplementedStore) Ack(context.Context, uuid.UUID, uuid.UUID) error {
	return ErrNotSupported
}

// Reschedule reports ErrNotSupported.
func (UnimplementedStore) Reschedule(context.Context, uuid.UUID, uuid.UUID, time.Duration, string) error {
	return ErrNotSupported
}

// Dead reports ErrNotSupported.
func (UnimplementedStore) Dead(context.Context, uuid.UUID, uuid.UUID, string) error {
	return ErrNotSupported
}

// Release reports ErrNotSupported.
func (UnimplementedStore) Release(context.Context, uuid.UUID, uuid.UUID) error {
	return ErrNotSupported
}

// ExtendLease reports ErrNotSupported.
func (UnimplementedStore) ExtendLease(context.Context, uuid.UUID, uuid.UUID, time.Duration) error {
	return ErrNotSupported
}

// PromoteDue reports ErrNotSupported.
func (UnimplementedStore) PromoteDue(context.Context, Source, []string) (int64, error) {
	return 0, ErrNotSupported
}

// ReapExpired reports ErrNotSupported.
func (UnimplementedStore) ReapExpired(context.Context, Source, []string, int) (int64, int64, error) {
	return 0, 0, ErrNotSupported
}

// VacuumStats reports ErrNotSupported.
func (UnimplementedStore) VacuumStats(context.Context, Source, time.Duration) (int64, error) {
	return 0, ErrNotSupported
}

// VacuumIdempotency reports ErrNotSupported.
func (UnimplementedStore) VacuumIdempotency(context.Context, Source) (int64, error) {
	return 0, ErrNotSupported
}

// VacuumCompleted reports ErrNotSupported.
func (UnimplementedStore) VacuumCompleted(context.Context, Source, time.Duration) (int64, error) {
	return 0, ErrNotSupported
}

// ListKinds reports ErrNotSupported.
func (UnimplementedStore) ListKinds(context.Context, Source) ([]string, error) {
	return nil, ErrNotSupported
}

// KindDepths reports ErrNotSupported.
func (UnimplementedStore) KindDepths(context.Context, Source) (map[string]Depths, error) {
	return nil, ErrNotSupported
}

// Stats reports ErrNotSupported.
func (UnimplementedStore) Stats(context.Context, Source, string) (Depths, []DailyCount, error) {
	return Depths{}, nil, ErrNotSupported
}

// AllDaily reports ErrNotSupported.
func (UnimplementedStore) AllDaily(context.Context, Source) ([]DailyCount, error) {
	return nil, ErrNotSupported
}

// ListJobs reports ErrNotSupported.
func (UnimplementedStore) ListJobs(context.Context, Source, JobFilter, int, int) ([]Job, int64, error) {
	return nil, 0, ErrNotSupported
}

// GetJob reports ErrNotSupported.
func (UnimplementedStore) GetJob(context.Context, Source, uuid.UUID) (*Job, error) {
	return nil, ErrNotSupported
}

// JobAttempts reports ErrNotSupported.
func (UnimplementedStore) JobAttempts(context.Context, Source, uuid.UUID) ([]AttemptError, error) {
	return nil, ErrNotSupported
}

// RetryJob reports ErrNotSupported.
func (UnimplementedStore) RetryJob(context.Context, Source, uuid.UUID) error {
	return ErrNotSupported
}

// RetryAllDead reports ErrNotSupported.
func (UnimplementedStore) RetryAllDead(context.Context, Source, string) (int64, error) {
	return 0, ErrNotSupported
}

// ArchiveJob reports ErrNotSupported.
func (UnimplementedStore) ArchiveJob(context.Context, Source, uuid.UUID) error {
	return ErrNotSupported
}

// PauseJob reports ErrNotSupported.
func (UnimplementedStore) PauseJob(context.Context, Source, uuid.UUID) error {
	return ErrNotSupported
}

// ResumeJob reports ErrNotSupported.
func (UnimplementedStore) ResumeJob(context.Context, Source, uuid.UUID) error {
	return ErrNotSupported
}

// DeleteJob reports ErrNotSupported.
func (UnimplementedStore) DeleteJob(context.Context, Source, uuid.UUID, JobState) error {
	return ErrNotSupported
}

// DeleteAll reports ErrNotSupported.
func (UnimplementedStore) DeleteAll(context.Context, Source, string, JobState) (int64, error) {
	return 0, ErrNotSupported
}

// VacuumDead reports ErrNotSupported.
func (UnimplementedStore) VacuumDead(context.Context, Source, string, time.Duration) (int64, error) {
	return 0, ErrNotSupported
}

// NukeAll reports ErrNotSupported.
func (UnimplementedStore) NukeAll(context.Context, Source) (NukeReport, error) {
	return NukeReport{}, ErrNotSupported
}

// ListEvents reports ErrNotSupported.
func (UnimplementedStore) ListEvents(context.Context, EventFilter, int, int) ([]EventAdminRow, int64, error) {
	return nil, 0, ErrNotSupported
}

// GetEvent reports ErrNotSupported.
func (UnimplementedStore) GetEvent(context.Context, uuid.UUID) (*EventAdminRow, error) {
	return nil, ErrNotSupported
}

// ListSubscriberViews reports ErrNotSupported.
func (UnimplementedStore) ListSubscriberViews(context.Context, string) ([]SubscriberView, error) {
	return nil, ErrNotSupported
}

// OpsStats reports ErrNotSupported.
func (UnimplementedStore) OpsStats(context.Context) (OpsStats, error) {
	return OpsStats{}, ErrNotSupported
}

// Replay reports ErrNotSupported.
func (UnimplementedStore) Replay(context.Context, ReplayFilter) (int64, error) {
	return 0, ErrNotSupported
}

// Retain reports ErrNotSupported.
func (UnimplementedStore) Retain(context.Context, time.Time, int) (int64, error) {
	return 0, ErrNotSupported
}

// Close reports ErrNotSupported.
func (UnimplementedStore) Close(context.Context) error {
	return ErrNotSupported
}
