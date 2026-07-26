package engine

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracePropagator is the fixed W3C Trace Context propagator carrying a span
// across the job boundary via Meta. It is explicit rather than
// otel.GetTextMapPropagator() — the global default is a no-op propagator
// unless the host application wires one up — so trace propagation works
// whether or not the application configures OpenTelemetry itself.
var tracePropagator = propagation.TraceContext{}

// InjectTraceContext writes ctx's current span, if valid, into meta as
// traceparent/tracestate, allocating meta when nil, and returns it. Without a
// valid span (no sampled trace in ctx) meta is returned unchanged, so a
// producer with tracing off never allocates a Meta map on ctx's account.
func InjectTraceContext(ctx context.Context, meta map[string]string) map[string]string {
	if !trace.SpanContextFromContext(ctx).IsValid() {
		return meta
	}
	if meta == nil {
		meta = map[string]string{}
	}
	tracePropagator.Inject(ctx, propagation.MapCarrier(meta))
	return meta
}

// ExtractTraceContext returns a context carrying the remote span described by
// meta's traceparent/tracestate, or ctx unchanged when meta carries none (or
// is nil).
func ExtractTraceContext(ctx context.Context, meta map[string]string) context.Context {
	if len(meta) == 0 {
		return ctx
	}
	return tracePropagator.Extract(ctx, propagation.MapCarrier(meta))
}
