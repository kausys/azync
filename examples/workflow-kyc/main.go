// Command workflow-kyc shows a workflow-as-code (package workflow) KYC
// onboarding flow: an Operation polls a verification provider until it is
// done, a durable Timer paces the polling without burning retries, and a
// Select waits for whichever comes first between a human approval Signal and
// a review deadline Timer.
//
// The provider is an in-process stand-in — this example is deliberately
// vendor-neutral, with no dependency on any specific KYC/verification
// vendor's SDK. The program starts one workflow, simulates the approval
// webhook concurrently, drives the flow to completion and exits.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	// Blank import registers the "postgres"/"postgresql" DSN schemes with
	// azync.Open.
	_ "github.com/kausys/azync/driver/azyncpgx"

	"github.com/kausys/azync"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/workflow"

	"github.com/google/uuid"
)

// defaultDSN matches the repo's compose.yml; override with DATABASE_URL.
//
//nolint:gosec // not a credential leak: matches compose.yml's dev-only DB
const defaultDSN = "postgres://azync:azync@localhost:5432/azync?sslmode=disable"

// ref identifies the KYC subject; it seeds the workflow's business
// idempotency key, so re-running main against a live execution is a no-op.
const ref = "applicant_4200"

const (
	pollInterval  = 2 * time.Second  // paces re-checking a still-pending provider
	reviewTimeout = 30 * time.Second // bounds how long an approval may take
)

// --- workflow & Operation payloads -------------------------------------------

// kycInput is the workflow's input: the subject to onboard.
type kycInput struct {
	Ref string `json:"ref"`
}

// kycResult is the workflow's terminal result.
type kycResult struct {
	Ref      string `json:"ref"`
	Status   string `json:"status"` // "approved", "rejected" or "expired"
	Decision string `json:"decision,omitempty"`
}

// checkStatusInput is the check-status Operation's argument.
type checkStatusInput struct {
	Ref string `json:"ref"`
}

// checkStatusOutput is the check-status Operation's result: the provider's
// current verification state for Ref.
type checkStatusOutput struct {
	Status string `json:"status"` // "pending" or "ready_for_review"
}

// approvalSignal is the payload Client.Signal delivers once a human reviewer
// has decided.
type approvalSignal struct {
	Decision string `json:"decision"` // "approved" or "rejected"
	By       string `json:"by"`
}

// provider is an in-process stand-in for an external verification provider:
// it reports "pending" until the third check, then "ready_for_review" — the
// shape of a real provider whose verification takes a few polls to settle.
type provider struct {
	checks atomic.Int32
}

func (p *provider) status(ref string) string {
	if p.checks.Add(1) >= 3 {
		return "ready_for_review"
	}
	return "pending"
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = defaultDSN
	}

	core, err := azync.Open(dsn)
	if err != nil {
		return fmt.Errorf("open core: %w", err)
	}
	defer func() {
		if err := core.Close(context.Background()); err != nil {
			log.Printf("close core: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Migrate is always explicit; Open never touches the schema.
	if err := core.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	r, err := workflow.New(core)
	if err != nil {
		return fmt.Errorf("new workflow runtime: %w", err)
	}

	prov := &provider{}
	registerHandlers(r.Worker(), prov)

	// Run the worker in the background; it replays the workflow function
	// and executes Operations until ctx is cancelled.
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := r.Worker().Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("worker: %v", err)
		}
	})

	// WithBusinessIdempotencyKey makes the start idempotent: a re-run with
	// the same key while this one is live returns the existing id instead
	// of a duplicate onboarding.
	started, err := r.Client().Start(ctx, "kyc-onboarding", "1", kycInput{Ref: ref},
		workflow.WithBusinessIdempotencyKey("kyc-"+ref))
	if err != nil {
		return fmt.Errorf("start kyc-onboarding: %w", err)
	}
	slog.Info("kyc onboarding started", "workflow_id", started.WorkflowID.String(), "deduplicated", started.Deduplicated)

	// Simulate the reviewer's decision arriving over a webhook, concurrently
	// with the workflow polling the provider.
	wg.Go(func() {
		deliverApproval(ctx, r.Client(), started.WorkflowID)
	})

	view, err := awaitTerminal(ctx, r.Manager(), started.WorkflowID)
	if err != nil {
		stop()
		wg.Wait()
		return err
	}
	reportOutcome(view)

	stop()
	wg.Wait()
	return nil
}

// registerHandlers binds the workflow function and its one Operation before
// the worker starts.
func registerHandlers(w *workflow.Worker, prov *provider) {
	workflow.RegisterOperation(w, "check-status", "1", func(_ context.Context, in checkStatusInput) (checkStatusOutput, error) {
		status := prov.status(in.Ref)
		slog.Info("checked provider status", "ref", in.Ref, "status", status)
		return checkStatusOutput{Status: status}, nil
	})

	workflow.RegisterWorkflow(w, "kyc-onboarding", "1", kycWorkflow)
}

// kycWorkflow is the durable workflow function: Operation check-status →
// while pending, a Timer paces the next check → once ready for review,
// Select races a human approval Signal against a review-deadline Timer.
//
// Every primitive call replays deterministically against the execution's
// history (see docs/workflow-v1-spec.md): on a fresh pass with nothing new
// to decide, ExecuteOperation/Sleep/WaitSignal recover their prior outcome
// instead of re-running real work, so this function is safe to invoke again
// from scratch on every workflow-task job.
func kycWorkflow(ctx workflow.Context, in kycInput) (kycResult, error) {
	for {
		var status checkStatusOutput
		if err := workflow.ExecuteOperation(ctx, "check-status", "1", checkStatusInput(in)).Get(&status); err != nil {
			return kycResult{}, fmt.Errorf("check status: %w", err)
		}
		if status.Status != "pending" {
			break
		}
		if _, err := workflow.Select(ctx, workflow.Sleep(ctx, pollInterval)); err != nil {
			return kycResult{}, err
		}
	}

	signal := workflow.WaitSignal(ctx, "approval")
	deadline := workflow.Sleep(ctx, reviewTimeout)
	idx, err := workflow.Select(ctx, signal, deadline)
	if err != nil {
		return kycResult{}, err
	}
	if idx == 1 {
		return kycResult{Ref: in.Ref, Status: "expired"}, nil
	}

	var appr approvalSignal
	if err := signal.Get(&appr); err != nil {
		return kycResult{}, fmt.Errorf("decode approval signal: %w", err)
	}
	return kycResult{Ref: in.Ref, Status: appr.Decision, Decision: appr.By}, nil
}

// deliverApproval simulates the reviewer's decision arriving over a
// webhook a few seconds in. A real webhook handler would call Signal once,
// as soon as the decision is made.
func deliverApproval(ctx context.Context, client *workflow.Client, id uuid.UUID) {
	select {
	case <-time.After(7 * time.Second):
	case <-ctx.Done():
		return
	}
	delivered, err := client.Signal(ctx, id, "approval", approvalSignal{Decision: "approved", By: "compliance-officer"})
	if err != nil {
		log.Printf("deliver approval: %v", err)
		return
	}
	slog.Info("approval webhook delivered", "workflow_id", id.String(), "delivered", delivered)
}

// awaitTerminal polls the workflow until it reaches a terminal state.
func awaitTerminal(ctx context.Context, m *workflow.Manager, id uuid.UUID) (workflow.View, error) {
	deadline := time.After(90 * time.Second)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return workflow.View{}, ctx.Err()
		case <-deadline:
			return workflow.View{}, errors.New("kyc onboarding did not complete within 90s")
		case <-tick.C:
			view, err := m.Get(ctx, id)
			if err != nil {
				return workflow.View{}, fmt.Errorf("get workflow: %w", err)
			}
			switch view.State {
			case driver.WorkflowSucceeded, driver.WorkflowFailed, driver.WorkflowCancelled:
				return view, nil
			default:
				// running or suspended: keep waiting.
			}
		}
	}
}

// reportOutcome logs the workflow's terminal result.
func reportOutcome(view workflow.View) {
	var result kycResult
	if len(view.Result) > 0 {
		_ = json.Unmarshal(view.Result, &result)
	}
	slog.Info("kyc onboarding settled",
		"state", string(view.State), "status", result.Status, "decision", result.Decision, "failure_reason", view.FailureReason)
}
