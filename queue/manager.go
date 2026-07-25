package queue

import (
	"context"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

// statsWindowDays is the daily-series window the admin surface charts.
const statsWindowDays = 30

// Manager is the queue administration surface: inspection, retry, archive,
// pause/resume, purge and vacuum. Pure library — no auth, no HTTP; embed it
// behind your own ops endpoints. It operates the queue source only.
type Manager struct {
	store driver.Store
}

// DailyCount is one day of throughput counters for a kind.
type DailyCount = driver.DailyCount

// AttemptError is one recorded failure in a job's retry history.
type AttemptError = driver.AttemptError

// NukeReport summarizes a NukeAll (dev reset).
type NukeReport = driver.NukeReport

// QueueInfo identifies one queue (= job kind) for the selector, with its
// instantaneous per-state counters.
type QueueInfo struct {
	Name      string
	Namespace string // the kind, display-only
	Pending   int64
	Scheduled int64
	Active    int64
	Paused    int64
	Dead      int64
	Succeeded int64
}

// QueueStats is the admin stats payload: instantaneous depths plus the daily
// throughput window (zero-filled, oldest first) and its totals.
type QueueStats struct {
	Queue      string
	Pending    int64
	Scheduled  int64
	Active     int64
	Dead       int64
	Paused     int64
	Succeeded  int64
	Enqueued   int64 // window totals
	Processed  int64
	Failed     int64
	Reaped     int64
	WindowDays int
	Daily      []DailyCount
}

// JobView is the admin projection of one job. Optional timestamps are zero
// when absent (IsZero reports absence).
type JobView struct {
	ID            uuid.UUID
	Kind          string
	State         JobState
	Attempt       int
	MaxAttempts   int
	EnqueuedAt    time.Time
	RunAt         time.Time
	LeaseDeadline time.Time
	FailedAt      time.Time
	CompletedAt   time.Time
	Payload       []byte
	Meta          map[string]string
	TraceID       string
	LastError     string
}

// JobListPage is one page of jobs for a queue+state.
type JobListPage struct {
	Items []JobView
	Page  int
	Size  int
	Total int64
}

// PurgeReport counts what Purge removed per state, plus the active jobs it
// deliberately left running.
type PurgeReport struct {
	Pending         int64
	Scheduled       int64
	Dead            int64
	ActiveRemaining int64
}

// ListQueues returns every known kind (live jobs or recent stats).
func (m *Manager) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	kinds, err := m.store.ListKinds(ctx, driver.SourceQueue)
	if err != nil {
		return nil, err
	}
	depths, err := m.store.KindDepths(ctx, driver.SourceQueue)
	if err != nil {
		return nil, err
	}
	queues := make([]QueueInfo, 0, len(kinds))
	for _, kind := range kinds {
		d := depths[kind]
		queues = append(queues, QueueInfo{
			Name: kind, Namespace: kind,
			Pending: d.Pending, Scheduled: d.Scheduled, Active: d.Active,
			Paused: d.Paused, Dead: d.Dead, Succeeded: d.Succeeded,
		})
	}
	return queues, nil
}

// Stats returns depths plus the zero-filled daily window for one queue.
func (m *Manager) Stats(ctx context.Context, queue string) (QueueStats, error) {
	depths, daily, err := m.store.Stats(ctx, driver.SourceQueue, queue)
	if err != nil {
		return QueueStats{}, err
	}
	out := QueueStats{
		Queue:   queue,
		Pending: depths.Pending, Scheduled: depths.Scheduled, Active: depths.Active,
		Dead: depths.Dead, Paused: depths.Paused, Succeeded: depths.Succeeded,
	}
	fillDailyWindow(&out, daily)
	return out, nil
}

// AllStats returns system-wide stats: depths summed across every kind plus the
// cross-queue daily throughput window (zero-filled, oldest first). Queue is
// empty to signal the aggregate scope.
func (m *Manager) AllStats(ctx context.Context) (QueueStats, error) {
	depths, err := m.store.KindDepths(ctx, driver.SourceQueue)
	if err != nil {
		return QueueStats{}, err
	}
	daily, err := m.store.AllDaily(ctx, driver.SourceQueue)
	if err != nil {
		return QueueStats{}, err
	}
	var out QueueStats
	for _, d := range depths {
		out.Pending += d.Pending
		out.Scheduled += d.Scheduled
		out.Active += d.Active
		out.Dead += d.Dead
		out.Paused += d.Paused
		out.Succeeded += d.Succeeded
	}
	fillDailyWindow(&out, daily)
	return out, nil
}

// fillDailyWindow zero-fills the last statsWindowDays of daily counters
// (oldest first) into out and accumulates the window totals.
func fillDailyWindow(out *QueueStats, daily []DailyCount) {
	byDate := make(map[string]DailyCount, len(daily))
	for _, d := range daily {
		byDate[d.Date.UTC().Format(time.DateOnly)] = d
	}
	out.WindowDays = statsWindowDays
	out.Daily = make([]DailyCount, 0, statsWindowDays)
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	for i := statsWindowDays - 1; i >= 0; i-- {
		date := today.AddDate(0, 0, -i)
		d, ok := byDate[date.Format(time.DateOnly)]
		if !ok {
			d = DailyCount{Date: date}
		}
		out.Daily = append(out.Daily, d)
		out.Enqueued += d.Enqueued
		out.Processed += d.Processed
		out.Failed += d.Failed
		out.Reaped += d.Reaped
	}
}

// List returns one page of jobs (page is 0-based).
func (m *Manager) List(ctx context.Context, queue string, state JobState, page, size int) (JobListPage, error) {
	return m.list(ctx, driver.JobFilter{Kind: queue, State: state}, page, size)
}

// ListAllJobs returns one page of jobs of a state across every queue (each
// JobView carries its own Kind so the caller can show which queue a job
// belongs to).
func (m *Manager) ListAllJobs(ctx context.Context, state JobState, page, size int) (JobListPage, error) {
	return m.list(ctx, driver.JobFilter{State: state}, page, size)
}

func (m *Manager) list(ctx context.Context, filter driver.JobFilter, page, size int) (JobListPage, error) {
	if size <= 0 {
		size = 50
	}
	if page < 0 {
		page = 0
	}
	rows, total, err := m.store.ListJobs(ctx, driver.SourceQueue, filter, page*size, size)
	if err != nil {
		return JobListPage{}, err
	}
	items := make([]JobView, 0, len(rows))
	for _, r := range rows {
		items = append(items, toJobView(r))
	}
	return JobListPage{Items: items, Page: page, Size: size, Total: total}, nil
}

// Get returns one job or nil when it does not exist.
func (m *Manager) Get(ctx context.Context, id uuid.UUID) (*JobView, error) {
	row, err := m.store.GetJob(ctx, driver.SourceQueue, id)
	if err != nil {
		if driver.IsNotFound(err) {
			return nil, nil //nolint:nilnil // absence is not an error for the admin surface
		}
		return nil, err
	}
	view := toJobView(*row)
	return &view, nil
}

// Retry re-enqueues a dead job with a fresh attempt budget.
func (m *Manager) Retry(ctx context.Context, id uuid.UUID) error {
	return m.store.RetryJob(ctx, driver.SourceQueue, id)
}

// RetryAll re-enqueues every dead job of the queue (single statement). An
// empty queue targets every kind.
func (m *Manager) RetryAll(ctx context.Context, queue string) (int64, error) {
	return m.store.RetryAllDead(ctx, driver.SourceQueue, queue)
}

// Archive parks a pending/scheduled job in the dead letter without running it.
func (m *Manager) Archive(ctx context.Context, id uuid.UUID) error {
	return m.store.ArchiveJob(ctx, driver.SourceQueue, id)
}

// Pause parks a pending/scheduled job; Resume restores it by run_at.
func (m *Manager) Pause(ctx context.Context, id uuid.UUID) error {
	return m.store.PauseJob(ctx, driver.SourceQueue, id)
}

// Resume returns a paused job to pending or scheduled depending on run_at.
func (m *Manager) Resume(ctx context.Context, id uuid.UUID) error {
	return m.store.ResumeJob(ctx, driver.SourceQueue, id)
}

// Delete removes one job in the given state (active jobs cannot be deleted —
// pass their real state; a running job's row is owned by its worker).
func (m *Manager) Delete(ctx context.Context, id uuid.UUID, state JobState) error {
	return m.store.DeleteJob(ctx, driver.SourceQueue, id, state)
}

// Purge empties the queue's pending, scheduled and dead jobs. Active jobs are
// owned by their workers and paused jobs were parked deliberately by an
// operator — both survive.
func (m *Manager) Purge(ctx context.Context, queue string) (PurgeReport, error) {
	var report PurgeReport
	for _, target := range []struct {
		state JobState
		out   *int64
	}{
		{StatePending, &report.Pending},
		{StateScheduled, &report.Scheduled},
		{StateDead, &report.Dead},
	} {
		n, err := m.store.DeleteAll(ctx, driver.SourceQueue, queue, target.state)
		if err != nil {
			return report, err
		}
		*target.out = n
	}
	depths, _, err := m.store.Stats(ctx, driver.SourceQueue, queue)
	if err != nil {
		return report, err
	}
	report.ActiveRemaining = depths.Active
	return report, nil
}

// VacuumDead removes dead jobs older than the given age. An empty queue
// targets every kind.
func (m *Manager) VacuumDead(ctx context.Context, queue string, olderThan time.Duration) (int64, error) {
	return m.store.VacuumDead(ctx, driver.SourceQueue, queue, olderThan)
}

// NukeAll wipes every queue job, stat and idempotency key — dev reset only.
func (m *Manager) NukeAll(ctx context.Context) (NukeReport, error) {
	return m.store.NukeAll(ctx, driver.SourceQueue)
}

// JobAttempts returns one job's failure history, oldest attempt first.
func (m *Manager) JobAttempts(ctx context.Context, id uuid.UUID) ([]AttemptError, error) {
	return m.store.JobAttempts(ctx, driver.SourceQueue, id)
}

func toJobView(r driver.Job) JobView {
	return JobView{
		ID:            r.ID,
		Kind:          r.Kind,
		State:         r.State,
		Attempt:       r.Attempt,
		MaxAttempts:   r.MaxAttempts,
		EnqueuedAt:    r.EnqueuedAt,
		RunAt:         r.RunAt,
		LeaseDeadline: r.LeaseUntil,
		FailedAt:      r.FailedAt,
		CompletedAt:   r.CompletedAt,
		Payload:       r.Payload,
		Meta:          r.Meta,
		TraceID:       r.TraceID,
		LastError:     r.LastError,
	}
}
