package azyncpgx

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// wakeBuffer bounds each subscriber's wake channel. A full buffer drops the
	// wakeup; the fetch loop's poll fallback keeps correctness.
	wakeBuffer = 64
	// listenBackoffMin and listenBackoffMax bound the reconnect backoff of the
	// dedicated LISTEN connection.
	listenBackoffMin = 500 * time.Millisecond
	listenBackoffMax = 30 * time.Second
)

// listener owns a single dedicated LISTEN connection per Store and fans each
// NOTIFY out to every registered Wake subscriber (a shared core has one queue
// worker and one event worker, each calling Wake). It carries no context field:
// the loop's lifetime is a cancel func, honoring containedctx.
type listener struct {
	pool     *pgxpool.Pool
	channel  string
	pollOnly bool
	logger   *slog.Logger

	mu          sync.Mutex
	subscribers []chan driver.Wake
	started     bool
	closed      bool
	lifeCancel  context.CancelFunc
	done        chan struct{}
	wg          sync.WaitGroup
}

func newListener(pool *pgxpool.Pool, channel string, pollOnly bool, logger *slog.Logger) *listener {
	return &listener{
		pool:     pool,
		channel:  channel,
		pollOnly: pollOnly,
		logger:   logger,
		done:     make(chan struct{}),
	}
}

// Wake returns a channel of wakeups signaled by enqueues and publishes. It may
// be called several times; each caller gets its own channel, closed when its ctx
// ends. A poll-only listener returns a nil channel and nil error.
func (s *Store) Wake(ctx context.Context) (<-chan driver.Wake, error) {
	return s.listener.wake(ctx)
}

func (l *listener) wake(ctx context.Context) (<-chan driver.Wake, error) {
	if l.pollOnly {
		//nolint:nilnil // poll-only backend: a nil channel with nil error is the contract signal
		return nil, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, errors.New("azyncpgx: listener is closed")
	}
	l.ensureStartedLocked()
	ch := make(chan driver.Wake, wakeBuffer)
	l.subscribers = append(l.subscribers, ch)

	// Register the unregister goroutine under l.mu so a concurrent close() cannot
	// run its wg.Wait() before this goroutine is added: with the Add happening
	// under the same lock that close() takes to set l.closed, either this Wake
	// observes the closed listener and adds nothing, or close() observes the
	// added goroutine and waits for it. That closes the TOCTOU window that a
	// wg.Go after unlocking would leave (a WaitGroup Add racing a Wait).
	l.wg.Go(func() {
		select {
		case <-ctx.Done():
		case <-l.done:
		}
		l.removeSubscriber(ch)
	})
	return ch, nil
}

// ensureStartedLocked starts the single LISTEN loop on the first subscription.
// The loop's lifetime is the store's, not any one caller's, so it runs on an
// independent context cancelled by close. Callers hold l.mu.
//
//nolint:contextcheck // deliberate: the listen loop outlives any single Wake ctx
func (l *listener) ensureStartedLocked() {
	if l.started {
		return
	}
	l.started = true
	lifeCtx, cancel := context.WithCancel(context.Background())
	l.lifeCancel = cancel
	l.wg.Go(func() { l.listenLoop(lifeCtx) })
}

func (l *listener) listenLoop(ctx context.Context) {
	backoff := listenBackoffMin
	for ctx.Err() == nil {
		err := l.listenOnce(ctx, func() { backoff = listenBackoffMin })
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			l.logger.Warn("azync listener connection lost, reconnecting",
				"error", err, "backoff", backoff)
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		backoff = min(backoff*2, listenBackoffMax)
	}
}

// listenOnce connects a dedicated pgx connection (the pool cannot own one for
// WaitForNotification), issues LISTEN, and streams notifications until the
// connection or ctx ends. onConnect resets the reconnect backoff once LISTEN is
// established.
func (l *listener) listenOnce(ctx context.Context, onConnect func()) error {
	conn, err := pgx.ConnectConfig(ctx, l.pool.Config().ConnConfig.Copy())
	if err != nil {
		return err
	}
	// Unblock WaitForNotification on cancel by closing the raw net.Conn; the
	// deferred pgx Close then runs once on this goroutine.
	if netConn := conn.PgConn().Conn(); netConn != nil {
		stop := context.AfterFunc(ctx, func() { _ = netConn.Close() })
		defer stop()
	}
	//nolint:contextcheck // close must run after ctx cancellation — a fresh deadline is deliberate
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{l.channel}.Sanitize()); err != nil {
		return err
	}
	onConnect()

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		if wake, ok := parseWake(notification.Payload); ok {
			l.broadcast(wake)
		}
	}
}

// broadcast delivers a wakeup to every live subscriber without blocking: a full
// buffer drops the wake, which polling covers. Sends run under l.mu so a channel
// removed and closed by removeSubscriber can never be sent to afterward.
func (l *listener) broadcast(wake driver.Wake) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, ch := range l.subscribers {
		select {
		case ch <- wake:
		default:
		}
	}
}

func (l *listener) removeSubscriber(ch chan driver.Wake) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, c := range l.subscribers {
		if c == ch {
			l.subscribers = slices.Delete(l.subscribers, i, i+1)
			close(ch)
			return
		}
	}
}

// close stops the listen loop and releases every subscriber. It is idempotent.
func (l *listener) close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	cancel := l.lifeCancel
	l.mu.Unlock()

	close(l.done)
	if cancel != nil {
		cancel()
	}
	l.wg.Wait()
}

// parseWake decodes a "source:kind" NOTIFY payload into a driver.Wake, splitting
// on the first colon so a kind may itself contain colons. A payload without a
// colon, or with an empty source or kind, is dropped.
func parseWake(payload string) (driver.Wake, bool) {
	i := strings.IndexByte(payload, ':')
	if i <= 0 || i == len(payload)-1 {
		return driver.Wake{}, false
	}
	return driver.Wake{Source: driver.Source(payload[:i]), Kind: payload[i+1:]}, true
}
