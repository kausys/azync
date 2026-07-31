// Command watch-sse bridges azync's change-hint stream to Server-Sent Events
// with nothing but net/http: a Watcher subscribes to every change on the
// Core, an SSE endpoint fans the hints out to browsers, and a small queue
// worker generates traffic so there is something to watch. Open
// http://localhost:8091 for a minimal live view, or consume the stream raw:
//
//	curl -N http://localhost:8091/stream
//
// The consumer rule the page demonstrates is the whole contract: on a
// "reset" hint (and the stream always opens with one), refetch everything;
// on any other hint, refetch what it names. Hints carry identifiers and
// states only — never job payloads.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Blank import registers the "postgres"/"postgresql" DSN schemes with
	// azync.Open.
	_ "github.com/kausys/azync/driver/azyncpgx"

	"github.com/kausys/azync"
	"github.com/kausys/azync/queue"
	"github.com/kausys/azync/watch"
)

// defaultDSN matches the repo's compose.yml; override with DATABASE_URL.
//
//nolint:gosec // not a credential leak: matches compose.yml's dev-only DB
const defaultDSN = "postgres://azync:azync@localhost:5433/azync?sslmode=disable"

const listenAddr = ":8091"

// tickJob is the traffic generator's job: enqueued every couple of seconds so
// the stream always has lifecycle transitions to show.
type tickJob struct {
	N int `json:"n"`
}

func (tickJob) Kind() string { return "examples.watch.tick" }

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

	// Migrate is always explicit; 00011 installs the change triggers.
	if err := core.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	w, err := watch.New(core)
	if err != nil {
		return fmt.Errorf("new watcher: %w", err)
	}

	// Traffic generator: a queue worker plus a slow enqueue loop, so the
	// stream shows pending → active → succeeded transitions continuously.
	q, err := queue.New(core)
	if err != nil {
		return fmt.Errorf("new queue runtime: %w", err)
	}
	err = queue.Register(q.Worker(), func(context.Context, tickJob) error {
		time.Sleep(300 * time.Millisecond) // let "active" be visible
		return nil
	})
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	go func() {
		if err := q.Worker().Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("worker: %v", err)
		}
	}()
	go func() {
		for n := 0; ; n++ {
			if _, err := q.Producer().Enqueue(ctx, tickJob{N: n}); err != nil {
				return // ctx ended or the store closed
			}
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /stream", streamHandler(w))
	mux.HandleFunc("GET /", pageHandler)

	server := &http.Server{Addr: listenAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		// The parent ctx is already cancelled here; a fresh deadline is the
		// point of a graceful shutdown window.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		//nolint:contextcheck // deliberate: shutdown runs after ctx is already cancelled
		_ = server.Shutdown(shutdownCtx)
	}()

	slog.Info("watch-sse listening", "addr", listenAddr, "stream", "/stream")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// streamHandler bridges one Watch subscription to one SSE response. The
// subscription is scoped to the request context, so a departing browser tears
// its subscription down with it.
func streamHandler(w *watch.Watcher) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		ch, err := w.Watch(r.Context(), watch.Filter{})
		if err != nil {
			http.Error(rw, "change stream unavailable", http.StatusServiceUnavailable)
			return
		}
		fl, ok := rw.(http.Flusher)
		if !ok {
			http.Error(rw, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		rw.Header().Set("Content-Type", "text/event-stream")
		rw.Header().Set("Cache-Control", "no-cache")
		// Tells buffering proxies (nginx) to pass events through immediately.
		rw.Header().Set("X-Accel-Buffering", "no")

		// A comment heartbeat keeps idle connections alive through proxies.
		heartbeat := time.NewTicker(20 * time.Second)
		defer heartbeat.Stop()
		for {
			select {
			case c, open := <-ch:
				if !open {
					return // store closed; EventSource reconnects on its own
				}
				payload, err := json.Marshal(c)
				if err != nil {
					continue
				}
				if _, err := fmt.Fprintf(rw, "event: change\ndata: %s\n\n", payload); err != nil {
					return // client went away
				}
				fl.Flush()
			case <-heartbeat.C:
				if _, err := fmt.Fprint(rw, ": ping\n\n"); err != nil {
					return // client went away
				}
				fl.Flush()
			}
		}
	}
}

// pageHandler serves a dependency-free live view: an EventSource appending
// one line per hint, resets highlighted.
func pageHandler(rw http.ResponseWriter, _ *http.Request) {
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprint(rw, `<!doctype html>
<title>azync watch</title>
<style>
  body { font: 13px/1.5 ui-monospace, monospace; margin: 2rem; }
  .reset { color: #b45309; font-weight: bold; }
</style>
<h1>azync change hints</h1>
<p>Streaming from <code>/stream</code>. A <span class="reset">reset</span> means "refetch everything".</p>
<ul id="log"></ul>
<script>
  const log = document.getElementById("log");
  new EventSource("/stream").addEventListener("change", (e) => {
    const c = JSON.parse(e.data);
    const li = document.createElement("li");
    if (c.Entity === "reset") {
      li.className = "reset";
      li.textContent = "reset — refetch everything";
    } else {
      li.textContent = [c.Entity, c.Source, c.Kind, c.TaskKey, c.State,
        c.Bulk ? "bulk×" + c.Count : c.ID].filter(Boolean).join(" · ");
    }
    log.prepend(li);
    while (log.childElementCount > 40) log.lastChild.remove();
  });
</script>`)
}
