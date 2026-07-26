// Command shared-core shows one Core powering all four runtimes at once:
// queue, event, dag, and workflow-as-code — one pool, one schema. A projector
// bridges event → queue; a one-task DAG and a one-Operation workflow each run
// once to prove coexistence, then the process waits for Ctrl-C like before.
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
	"syscall"
	"time"

	_ "github.com/kausys/azync/driver/azyncpgx"

	"github.com/kausys/azync"
	"github.com/kausys/azync/dag"
	"github.com/kausys/azync/driver"
	"github.com/kausys/azync/event"
	"github.com/kausys/azync/queue"
	"github.com/kausys/azync/workflow"

	"github.com/google/uuid"
)

//nolint:gosec // matches compose.yml's dev-only DB
const defaultDSN = "postgres://azync:azync@localhost:5433/azync?sslmode=disable"

type orderPlaced struct {
	OrderID string `json:"orderId"`
}

func (orderPlaced) EventType() string { return "examples.order.placed" }

type sendReceipt struct {
	OrderID string `json:"orderId"`
}

func (sendReceipt) Kind() string { return "examples.receipt.send" }

type stampTask struct {
	Label string `json:"label"`
}

func (stampTask) Kind() string { return "examples.shared.stamp" }

const projector = "examples.receipt-projector"

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

	if err := core.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	q, err := queue.New(core)
	if err != nil {
		return fmt.Errorf("new queue runtime: %w", err)
	}
	ev, err := event.New(core)
	if err != nil {
		return fmt.Errorf("new event runtime: %w", err)
	}
	d, err := dag.New(core)
	if err != nil {
		return fmt.Errorf("new dag runtime: %w", err)
	}
	wf, err := workflow.New(core)
	if err != nil {
		return fmt.Errorf("new workflow runtime: %w", err)
	}

	if err := queue.Register(q.Worker(), func(_ context.Context, job sendReceipt) error {
		slog.Info("sending receipt", "order_id", job.OrderID)
		return nil
	}); err != nil {
		return fmt.Errorf("register queue: %w", err)
	}

	if err := event.RegisterFunc(ev.Worker(), projector, func(ctx context.Context, order orderPlaced) error {
		_, err := q.Producer().Enqueue(ctx, sendReceipt(order))
		return err
	}); err != nil {
		return fmt.Errorf("register projector: %w", err)
	}

	if err := dag.Register(d.Worker(), func(ctx context.Context, t stampTask) (dag.None, error) {
		slog.Info("dag stamp", "label", t.Label, "dag_id", dag.ID(ctx))
		return dag.None{}, nil
	}); err != nil {
		return fmt.Errorf("register dag: %w", err)
	}

	workflow.RegisterOperation(wf.Worker(), "ping", "1", func(_ context.Context, _ struct{}) (string, error) {
		return "pong", nil
	})
	workflow.RegisterWorkflow(wf.Worker(), "shared-ping", "1", func(ctx workflow.Context, _ struct{}) (string, error) {
		var out string
		if err := workflow.ExecuteOperation(ctx, "ping", "1", struct{}{}).Get(&out); err != nil {
			return "", err
		}
		return out, nil
	})

	slog.Info("workers starting; press ctrl-C to stop")
	var wg sync.WaitGroup
	start := func(name string, fn func(context.Context) error) {
		wg.Go(func() {
			if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("%s worker: %v", name, err)
			}
		})
	}
	start("queue", q.Worker().Start)
	start("event", ev.Worker().Start)
	start("dag", d.Worker().Start)
	start("workflow", wf.Worker().Start)

	select {
	case <-ev.Worker().Ready():
		if _, err := ev.Publisher().Publish(ctx, orderPlaced{OrderID: "ord_9001"}); err != nil {
			log.Printf("publish: %v", err)
		}
	case <-ctx.Done():
		wg.Wait()
		return nil
	}

	<-d.Worker().Ready()
	dagRes, err := d.Client().Run(ctx,
		dag.Define("shared-stamp").Task("stamp", stampTask{Label: "coexist"}),
		dag.WithIdempotencyKey("shared-core-stamp"))
	if err != nil {
		log.Printf("dag run: %v", err)
	} else {
		slog.Info("dag started", "id", dagRes.ID, "deduplicated", dagRes.Deduplicated)
	}

	// workflow.Worker has no Ready channel; Start is already running above.
	wfRes, err := wf.Client().Start(ctx, "shared-ping", "1", struct{}{},
		workflow.WithBusinessIdempotencyKey("shared-core-ping"))
	if err != nil {
		log.Printf("workflow start: %v", err)
	} else {
		slog.Info("workflow started", "id", wfRes.WorkflowID, "deduplicated", wfRes.Deduplicated)
	}

	awaitSettled(ctx, d, wf, dagRes.ID, wfRes.WorkflowID)
	if ctx.Err() == nil {
		slog.Info("dag+workflow coexistence ok; queue/event still running — ctrl-C to stop")
		<-ctx.Done()
	}
	stop()
	wg.Wait()

	qStats, err := q.Manager().AllStats(context.Background())
	if err != nil {
		return fmt.Errorf("queue stats: %w", err)
	}
	evStats, err := ev.Manager().Stats(context.Background())
	if err != nil {
		return fmt.Errorf("event stats: %w", err)
	}
	slog.Info("queue stats", "pending", qStats.Pending, "succeeded", qStats.Succeeded, "dead", qStats.Dead)
	slog.Info("event stats", "events", evStats.Events, "succeeded", evStats.Succeeded, "dead", evStats.Dead)
	return nil
}

func awaitSettled(ctx context.Context, d *dag.Runtime, wf *workflow.Runtime, dagID, wfID uuid.UUID) {
	deadline := time.After(15 * time.Second)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			slog.Warn("timed out waiting for dag/workflow terminal state")
			return
		case <-tick.C:
			dagOK, wfOK := dagID == uuid.Nil, wfID == uuid.Nil
			if !dagOK {
				if v, err := d.Manager().Get(ctx, dagID); err == nil && v != nil {
					switch v.State {
					case dag.StateSucceeded, dag.StateFailed, dag.StateCancelled:
						dagOK = true
					default:
						// still running / suspended / compensating
					}
				}
			}
			if !wfOK {
				if v, err := wf.Manager().Get(ctx, wfID); err == nil {
					switch v.State {
					case driver.WorkflowSucceeded, driver.WorkflowFailed, driver.WorkflowCancelled:
						wfOK = true
						slog.Info("workflow settled", "state", string(v.State))
					default:
						// still running / suspended
					}
				}
			}
			if dagOK && wfOK {
				return
			}
		}
	}
}
