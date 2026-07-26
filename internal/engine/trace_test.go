package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

// withValidSpanContext returns ctx carrying a valid, sampled span context
// built directly from the W3C spec's example trace/span IDs — no SDK
// TracerProvider needed, just the otel/trace API this module already depends
// on.
func withValidSpanContext(ctx context.Context) context.Context {
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	return trace.ContextWithSpanContext(ctx, sc)
}

func TestInjectTraceContextNoopWithoutValidSpan(t *testing.T) {
	t.Parallel()
	is := require.New(t)

	meta := InjectTraceContext(context.Background(), nil)
	is.Nil(meta, "no valid span means no meta allocation and no injected keys")
}

func TestInjectAndExtractTraceContextRoundTrip(t *testing.T) {
	t.Parallel()
	is := require.New(t)

	ctx := withValidSpanContext(context.Background())
	meta := InjectTraceContext(ctx, nil)
	is.NotEmpty(meta)
	is.Contains(meta, "traceparent")

	extracted := ExtractTraceContext(context.Background(), meta)
	sc := trace.SpanContextFromContext(extracted)
	is.True(sc.IsValid(), "the extracted context must carry a valid remote span context")
	is.True(sc.IsRemote())
	is.Equal("4bf92f3577b34da6a3ce929d0e0e4736", sc.TraceID().String(),
		"the extracted trace ID must match what was injected")
}

func TestInjectTraceContextPreservesExistingMeta(t *testing.T) {
	t.Parallel()
	is := require.New(t)

	ctx := withValidSpanContext(context.Background())
	meta := InjectTraceContext(ctx, map[string]string{"tenant": "acme"})
	is.Equal("acme", meta["tenant"], "injection must not clobber unrelated existing meta")
	is.Contains(meta, "traceparent")
}

func TestExtractTraceContextNoopWithoutMeta(t *testing.T) {
	t.Parallel()
	is := require.New(t)

	base := context.Background()
	is.Equal(base, ExtractTraceContext(base, nil))
	is.Equal(base, ExtractTraceContext(base, map[string]string{}))
}
