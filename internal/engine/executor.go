package engine

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/kausys/azync/internal/engine"

// execute runs one leased job: consumer span joined to the enqueue trace,
// per-job timeout, lease renewal, and outcome handling (Ack / Reschedule with
// backoff / dead letter).
func (e *Engine) execute(jobsCtx context.Context, k Kind, job driver.Job, release func()) {
	defer release()

	ctx := withRemoteTrace(jobsCtx, job)

	tracer := otel.Tracer(instrumentationName)
	ctx, span := tracer.Start(ctx, string(e.source)+".job "+job.Kind,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String(string(e.source)+".kind", job.Kind),
			attribute.String(string(e.source)+".job_id", job.ID.String()),
			attribute.Int(string(e.source)+".attempt", job.Attempt),
		))
	defer span.End()

	if k.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, k.Timeout)
		defer cancel()
	}
	ctx, cancelLease := context.WithCancel(ctx)
	defer cancelLease()
	go e.renewLease(ctx, job.ID, job.LeaseToken, cancelLease)

	err := e.runHandler(ctx, k, job)
	if err == nil {
		if ackErr := e.store.Ack(context.WithoutCancel(ctx), job.ID, job.LeaseToken); ackErr != nil {
			logSettleError(e.logger, ackErr, "ack failed", "job", job.ID.String(), "error", ackErr)
		}
		return
	}

	span.SetStatus(codes.Error, err.Error())
	e.settleFailure(context.WithoutCancel(ctx), k, job, err)
}

// settleFailure lands a failed execution: Abort or an exhausted budget goes to
// the dead letter; everything else reschedules after the classified delay (or
// the exponential backoff). Settle errors are logged, never fatal: a stale
// lease token means another worker already owns the job.
func (e *Engine) settleFailure(ctx context.Context, k Kind, job driver.Job, err error) {
	logger := e.logger.With("job", job.ID.String(), "kind", job.Kind, "attempt", job.Attempt)

	o := k.Classify(err)
	exhausted := job.Attempt >= job.MaxAttempts

	switch {
	case o.Kind == OutcomeAbort:
		if dErr := e.store.Dead(ctx, job.ID, job.LeaseToken, err.Error()); dErr != nil {
			logSettleError(logger, dErr, "dead-letter failed", "error", dErr)
		}
		logger.Warn("job aborted to dead letter", "error", err)

	case exhausted:
		if dErr := e.store.Dead(ctx, job.ID, job.LeaseToken, err.Error()); dErr != nil {
			logSettleError(logger, dErr, "dead-letter failed", "error", dErr)
		}
		if o.Reportable {
			logger.Error("job exhausted retries (reportable)", "error", err)
		} else {
			logger.Warn("job exhausted retries", "error", err)
		}

	default:
		delay := o.Delay
		if delay <= 0 {
			delay = Backoff(job.Attempt)
		}
		if rErr := e.store.Reschedule(ctx, job.ID, job.LeaseToken, delay, err.Error()); rErr != nil {
			logSettleError(logger, rErr, "reschedule failed", "error", rErr)
		}
		logger.Debug("job rescheduled", "delay", delay, "error", err)
	}
}

// logSettleError logs a failed settlement. A not-found error is the expected
// fencing outcome — a reaper or a re-leasing worker already owns the row — so
// it logs at Debug; anything else is a real storage error and logs at Error.
func logSettleError(logger *slog.Logger, err error, msg string, args ...any) {
	if driver.IsNotFound(err) {
		logger.Debug(msg+": lease no longer owned", args...)
		return
	}
	logger.Error(msg, args...)
}

// runHandler invokes the handler with a panic guard: a panicking handler must
// take down only its job, not the whole consumer process. The recovered panic
// becomes an ordinary handler error (stack attached) and settles through the
// normal failure path, so it is retried and eventually dead-lettered like any
// other failure.
func (e *Engine) runHandler(ctx context.Context, k Kind, job driver.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
			e.logger.Error("handler panicked",
				"job", job.ID.String(), "kind", job.Kind, "panic", r)
		}
	}()
	return k.Handler(ctx, job)
}

// renewLease extends the job lease every LeaseTTL/2. Losing the lease cancels
// the handler ctx: another worker may already own the job.
func (e *Engine) renewLease(ctx context.Context, id, leaseToken uuid.UUID, cancel context.CancelFunc) {
	ticker := time.NewTicker(e.settings.LeaseTTL / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.store.ExtendLease(ctx, id, leaseToken, e.settings.LeaseTTL); err != nil {
				e.logger.Warn("lease lost, cancelling handler", "job", id.String(), "error", err)
				cancel()
				return
			}
		}
	}
}

// withRemoteTrace joins the handler context to the trace propagated at enqueue
// time, if any.
func withRemoteTrace(ctx context.Context, job driver.Job) context.Context {
	if job.TraceID == "" || job.SpanID == "" {
		return ctx
	}
	traceID, err := trace.TraceIDFromHex(job.TraceID)
	if err != nil {
		return ctx
	}
	spanID, err := trace.SpanIDFromHex(job.SpanID)
	if err != nil {
		return ctx
	}
	flags := trace.TraceFlags(job.TraceFlags) //nolint:gosec // stored from a uint8 to begin with
	return trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: flags,
		Remote:     true,
	}))
}
