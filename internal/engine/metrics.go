package engine

import (
	"context"
	"sync"
	"time"

	"github.com/kausys/azync/driver"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// metricsDepthInterval is how often the maintenance loop refreshes the depth
// cache the queue.depth / queue.oldest_pending_age gauges read (see
// engineMetrics.updateDepths). A fixed interval, not exposed as a Settings
// knob: metrics freshness is a much looser requirement than the
// promote/reap/vacuum cadences.
const metricsDepthInterval = 15 * time.Second

// engineMetrics holds one Engine's lazily-built OpenTelemetry instruments.
// Every instrument is created against whatever MeterProvider is globally
// configured at construction time (otel.Meter): with none configured, the
// API's no-op implementation makes every call below a cheap no-op, so an
// application that never wires up OTel pays only this struct's allocation.
type engineMetrics struct {
	source driver.Source

	settled  metric.Int64Counter
	duration metric.Float64Histogram

	depthMu    sync.Mutex
	depthCache map[string]driver.Depths // kind -> depths, refreshed by maintenanceLoop
}

func newEngineMetrics(source driver.Source) *engineMetrics {
	meter := otel.Meter(instrumentationName)
	m := &engineMetrics{source: source, depthCache: map[string]driver.Depths{}}

	m.settled, _ = meter.Int64Counter("azync.jobs.settled",
		metric.WithDescription("Count of settled job attempts, by outcome."),
		metric.WithUnit("{job}"))
	m.duration, _ = meter.Float64Histogram("azync.handler.duration",
		metric.WithDescription("Handler execution time from dispatch to outcome."),
		metric.WithUnit("s"))

	depthGauge, _ := meter.Int64ObservableGauge("azync.queue.depth",
		metric.WithDescription("Instantaneous pending job count, by kind."),
		metric.WithUnit("{job}"))
	ageGauge, _ := meter.Float64ObservableGauge("azync.queue.oldest_pending_age",
		metric.WithDescription("Age of the oldest pending job, by kind; 0 when none is pending."),
		metric.WithUnit("s"))
	// Registration failure (e.g. no valid instruments — can't happen with the
	// fixed names above) leaves the gauges simply never observed; not worth
	// failing Engine construction over.
	_, _ = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		m.depthMu.Lock()
		defer m.depthMu.Unlock()
		for kind, d := range m.depthCache {
			attrs := metric.WithAttributes(
				attribute.String("source", string(source)), attribute.String("kind", kind))
			o.ObserveInt64(depthGauge, d.Pending, attrs)
			o.ObserveFloat64(ageGauge, d.OldestPendingAge.Seconds(), attrs)
		}
		return nil
	}, depthGauge, ageGauge)

	return m
}

// recordSettled records one settled job attempt: a count and a duration
// observation sharing the same (source, kind, outcome) attributes. ctx is
// used only to carry an active span/baggage into the metric export pipeline
// (per the otel/metric API); it is never checked for cancellation, so
// passing an already-settling context (e.g. WithoutCancel-derived) here is
// always safe.
func (m *engineMetrics) recordSettled(ctx context.Context, kind, outcome string, elapsed time.Duration) {
	attrs := metric.WithAttributes(
		attribute.String("source", string(m.source)),
		attribute.String("kind", kind),
		attribute.String("outcome", outcome))
	m.settled.Add(ctx, 1, attrs)
	m.duration.Record(ctx, elapsed.Seconds(), attrs)
}

// recordAbandoned counts handlers abandoned past the hard drain timeout
// (engine.go's finalDrainGrace). There is no single kind or duration to
// attach — n handlers across however many kinds were still running — so only
// the counter is bumped, without a kind attribute.
func (m *engineMetrics) recordAbandoned(ctx context.Context, n int64) {
	if n <= 0 {
		return
	}
	m.settled.Add(ctx, n, metric.WithAttributes(
		attribute.String("source", string(m.source)), attribute.String("outcome", "abandoned")))
}

// updateDepths replaces the cache the queue.depth / queue.oldest_pending_age
// gauge callback reads. Called only by maintenanceLoop's own ticker — never
// from the callback itself, which must never touch the database.
func (m *engineMetrics) updateDepths(depths map[string]driver.Depths) {
	m.depthMu.Lock()
	defer m.depthMu.Unlock()
	m.depthCache = depths
}
