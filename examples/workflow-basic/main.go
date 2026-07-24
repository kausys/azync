// Command workflow-basic shows a durable DAG workflow shaped like a real
// onboarding saga: a typed chain whose outputs flow downstream, a polling-wait
// against an external provider, a signal delivered by a simulated webhook, a
// fan-out, and a final idempotent barrier that launches a second workflow.
//
// It is the migration template for a braid-style KYC flow: a linear chain
// enriched step by step through ResultOf, a NotReady poll that waits on an
// external condition without burning retries, and a Run(WithIdempotencyKey)
// barrier that replaces a Redis SetNX lock with the engine's own live-execution
// dedupe. The program runs the flow to completion and exits.
package main

import (
	"context"
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
	"github.com/kausys/azync/workflow"

	"github.com/google/uuid"
)

// defaultDSN matches the repo's compose.yml; override with DATABASE_URL.
//
//nolint:gosec // not a credential leak: matches compose.yml's dev-only DB
const defaultDSN = "postgres://azync:azync@localhost:5432/azync?sslmode=disable"

// businessID identifies the onboarding subject; it seeds every idempotency key,
// so re-running the barrier is a no-op instead of a duplicate.
const businessID = "kyb_9001"

// --- task arguments (each Kind is the wire-stable identity) -----------------

// createAccount opens the provider account. Its handler returns an account,
// which the rest of the chain reads through ResultOf.
type createAccount struct {
	Email string `json:"email"`
}

func (createAccount) Kind() string { return "onboard.create_account" }

// account is create-account's durable result, read downstream by verify-status.
type account struct {
	ID string `json:"id"`
}

// deleteAccount is create-account's compensation. It only runs if a later task
// dies under the workflow's (default) Cancel policy, unwinding the account in
// reverse completion order. This happy-path run never triggers it.
type deleteAccount struct {
	Email string `json:"email"`
}

func (deleteAccount) Kind() string { return "onboard.delete_account" }

// verifyStatus polls the provider until the account is verified.
type verifyStatus struct{}

func (verifyStatus) Kind() string { return "onboard.verify_status" }

// approval is the payload the approval signal carries; provision and notify
// read it through ResultOf.
type approval struct {
	By string `json:"by"`
}

// provision and notify fan out after the approval lands.
type provision struct{}

func (provision) Kind() string { return "onboard.provision" }

type notify struct{}

func (notify) Kind() string { return "onboard.notify" }

// finalize is the fan-in barrier: it depends on both provision and notify and
// launches the downstream celebrate workflow exactly once.
type finalize struct{}

func (finalize) Kind() string { return "onboard.finalize" }

// celebrate is the single task of the downstream workflow the barrier starts.
type celebrate struct {
	Business string `json:"business"`
}

func (celebrate) Kind() string { return "onboard.celebrate" }

// provider is an in-process stand-in for an external verification provider: it
// reports "pending" until the third check, then "verified" — the shape of a KYC
// provider whose status is not yet available.
type provider struct {
	checks atomic.Int32
}

func (p *provider) status(id string) string {
	if p.checks.Add(1) >= 3 {
		return "verified"
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
	if err := registerHandlers(r, prov); err != nil {
		return err
	}

	// Run the worker in the background; it drives the DAG scheduler and executes
	// task handlers until ctx is cancelled.
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := r.Worker().Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("worker: %v", err)
		}
	})

	// The onboarding DAG:
	//
	//   create-account -> verify-status -> await-approval -> provision -> finalize
	//                                                     \-> notify    -/
	//
	// create-account declares a compensation; verify-status polls; await-approval
	// waits for a signal; provision and notify fan out; finalize fans them back in
	// and launches the celebrate workflow.
	onboard := workflow.Define("onboard-business").
		Task("create-account", createAccount{Email: "founder@example.com"},
			workflow.Compensate(deleteAccount{Email: "founder@example.com"})).
		Task("verify-status", verifyStatus{}, workflow.After("create-account")).
		WaitSignal("await-approval", workflow.After("verify-status")).
		Task("provision", provision{}, workflow.After("await-approval")).
		Task("notify", notify{}, workflow.After("await-approval")).
		Task("finalize", finalize{}, workflow.After("provision", "notify"))

	// WithIdempotencyKey makes the whole run idempotent: a re-run with the same
	// key while this one is live returns the existing id instead of a duplicate —
	// the braid dedupe-on-start, replacing an asynq TaskID conflict.
	started, err := r.Client().Run(ctx, onboard, workflow.WithIdempotencyKey("onboard-"+businessID))
	if err != nil {
		return fmt.Errorf("run onboard-business: %w", err)
	}
	slog.Info("onboarding started", "workflow_id", started.ID.String(), "deduplicated", started.Deduplicated)

	// Simulate the approval webhook: after a short delay a real handler would call
	// Signal once. We retry on ErrNoSignalMatched so an early "webhook" that races
	// ahead of the DAG reaching the wait still lands.
	wg.Go(func() {
		deliverApproval(ctx, r.Client(), started.ID)
	})

	// Drive the flow to completion, then report the terminal state.
	if err := awaitSuccess(ctx, r.Manager(), started.ID); err != nil {
		stop()
		wg.Wait()
		return err
	}
	reportOutcome(ctx, r.Manager(), started.ID)

	stop()
	wg.Wait()
	return nil
}

// registerHandlers binds every task kind before the worker starts. The internal
// WaitSignal kind ($signal) needs no handler — the scheduler resolves it.
func registerHandlers(r *workflow.Runtime, prov *provider) error {
	w := r.Worker()

	if err := workflow.Register(w, func(ctx context.Context, c createAccount) (account, error) {
		id := "acct_" + businessID
		logTask(ctx, "creating provider account", "email", c.Email, "account_id", id)
		return account{ID: id}, nil
	}); err != nil {
		return fmt.Errorf("register create-account: %w", err)
	}

	// The compensation handler — declared for completeness; unused on the happy
	// path. It would run in reverse completion order if a later task died.
	if err := workflow.Register(w, func(ctx context.Context, d deleteAccount) (workflow.None, error) {
		logTask(ctx, "compensating: deleting provider account", "email", d.Email)
		return workflow.None{}, nil
	}); err != nil {
		return fmt.Errorf("register delete-account: %w", err)
	}

	if err := workflow.Register(w, func(ctx context.Context, _ verifyStatus) (workflow.None, error) {
		acct, err := workflow.ResultOf[account](ctx, "create-account")
		if err != nil {
			return workflow.None{}, err
		}
		switch st := prov.status(acct.ID); st {
		case "verified":
			logTask(ctx, "provider verification complete", "account_id", acct.ID, "status", st)
			return workflow.None{}, nil
		default:
			// NotReady re-checks after the delay WITHOUT consuming a retry — the
			// polling-wait primitive. The attempt counter stays at 1 throughout.
			logTask(ctx, "provider still verifying; will re-check", "account_id", acct.ID, "status", st)
			return workflow.None{}, workflow.NotReady(2 * time.Second)
		}
	}); err != nil {
		return fmt.Errorf("register verify-status: %w", err)
	}

	if err := workflow.Register(w, func(ctx context.Context, _ provision) (workflow.None, error) {
		appr, err := workflow.ResultOf[approval](ctx, "await-approval")
		if err != nil {
			return workflow.None{}, err
		}
		logTask(ctx, "provisioning resources", "approved_by", appr.By)
		return workflow.None{}, nil
	}); err != nil {
		return fmt.Errorf("register provision: %w", err)
	}

	if err := workflow.Register(w, func(ctx context.Context, _ notify) (workflow.None, error) {
		appr, err := workflow.ResultOf[approval](ctx, "await-approval")
		if err != nil {
			return workflow.None{}, err
		}
		logTask(ctx, "notifying stakeholders", "approved_by", appr.By)
		return workflow.None{}, nil
	}); err != nil {
		return fmt.Errorf("register notify: %w", err)
	}

	// finalize is the fan-in barrier. Run inside a handler, combined with
	// WithIdempotencyKey, starts the downstream workflow exactly once — even
	// though the task itself runs at-least-once, and even if several upstream
	// workflows raced to this point sharing the same key.
	celebrateDef := workflow.Define("celebrate").Task("cheer", celebrate{Business: businessID})
	if err := workflow.Register(w, func(ctx context.Context, _ finalize) (workflow.None, error) {
		res, err := r.Client().Run(ctx, celebrateDef, workflow.WithIdempotencyKey("celebrate-"+businessID))
		if err != nil {
			return workflow.None{}, err
		}
		if res.Deduplicated {
			logTask(ctx, "celebrate already launched; barrier absorbed the duplicate", "celebrate_id", res.ID.String())
		} else {
			logTask(ctx, "launched downstream celebrate workflow", "celebrate_id", res.ID.String())
		}
		return workflow.None{}, nil
	}); err != nil {
		return fmt.Errorf("register finalize: %w", err)
	}

	if err := workflow.Register(w, func(ctx context.Context, c celebrate) (workflow.None, error) {
		logTask(ctx, "celebrating onboarding", "business", c.Business)
		return workflow.None{}, nil
	}); err != nil {
		return fmt.Errorf("register celebrate: %w", err)
	}

	return nil
}

// deliverApproval simulates the approval webhook. A real webhook handler would
// call Signal once; retrying on ErrNoSignalMatched covers the case where the
// signal arrives before the DAG has reached the WaitSignal task.
func deliverApproval(ctx context.Context, client *workflow.Client, id uuid.UUID) {
	select {
	case <-time.After(3 * time.Second):
	case <-ctx.Done():
		return
	}
	for {
		err := client.Signal(ctx, id, "await-approval", approval{By: "compliance-officer"})
		switch {
		case err == nil:
			slog.Info("approval webhook delivered", "workflow_id", id.String())
			return
		case errors.Is(err, workflow.ErrNoSignalMatched):
			// The flow has not parked on the wait yet; try again shortly.
			select {
			case <-time.After(500 * time.Millisecond):
			case <-ctx.Done():
				return
			}
		default:
			log.Printf("deliver approval: %v", err)
			return
		}
	}
}

// awaitSuccess polls the workflow until it succeeds, or fails loudly if it
// reaches any other terminal state.
func awaitSuccess(ctx context.Context, m *workflow.Manager, id uuid.UUID) error {
	deadline := time.After(90 * time.Second)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return errors.New("onboarding did not complete within 90s")
		case <-tick.C:
			view, err := m.Get(ctx, id)
			if err != nil {
				return fmt.Errorf("get workflow: %w", err)
			}
			if view == nil {
				continue
			}
			switch view.State {
			case workflow.StateSucceeded:
				return nil
			case workflow.StateFailed, workflow.StateCancelled:
				return fmt.Errorf("onboarding ended %s: %s", view.State, view.FailureReason)
			default:
				// running, suspended or compensating: keep waiting.
			}
		}
	}
}

// reportOutcome logs the terminal task states and confirms the downstream
// workflow was started by the barrier.
func reportOutcome(ctx context.Context, m *workflow.Manager, id uuid.UUID) {
	tasks, err := m.Tasks(ctx, id)
	if err != nil {
		log.Printf("list tasks: %v", err)
		return
	}
	for _, t := range tasks {
		slog.Info("task settled", "key", t.Key, "kind", t.Kind, "state", t.State, "attempt", t.Attempt)
	}
	page, err := m.List(ctx, workflow.Filter{Name: "celebrate"}, 0, 10)
	if err != nil {
		log.Printf("list celebrate: %v", err)
		return
	}
	slog.Info("onboarding succeeded", "celebrate_workflows", page.Total)
}

// logTask logs a handler line stamped with the task's identity and attempt, read
// from the context accessors.
func logTask(ctx context.Context, msg string, args ...any) {
	slog.Info(msg, append([]any{
		"task", workflow.TaskKey(ctx),
		"attempt", workflow.Attempt(ctx),
	}, args...)...)
}
