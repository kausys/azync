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
	// wakeBuffer bounds each wake subscriber's channel. A full buffer drops the
	// wakeup; the fetch loop's poll fallback keeps correctness.
	wakeBuffer = 64
	// listenBackoffMin and listenBackoffMax bound the reconnect backoff of a
	// dedicated LISTEN connection.
	listenBackoffMin = 500 * time.Millisecond
	listenBackoffMax = 30 * time.Second
)

// listener owns one dedicated LISTEN connection per Store per channel and
// fans each parsed NOTIFY out to every registered subscriber. It is generic
// over the parsed notification type: the wakeup listener carries driver.Wake,
// the change listener driver.Change. The connection starts lazily on the
// first subscription, so a Store whose callers never subscribe (a worker that
// never calls Changes) pays nothing for the second channel. It carries no
// context field: the loop's lifetime is a cancel func, honoring containedctx.
type listener[T any] struct {
	pool    *pgxpool.Pool
	channel string
	// pollOnly makes subscribe report the poll-only contract (nil channel).
	pollOnly bool
	logger   *slog.Logger
	// buffer bounds each subscriber's channel.
	buffer int
	// parse decodes one NOTIFY payload; a false return drops it.
	parse func(payload string) (T, bool)
	// reset, when non-nil, mints the in-band gap signal: it is pre-loaded
	// into every new subscription, broadcast on every successful LISTEN
	// (first connect and every reconnect — notifications during the outage
	// are lost), and substituted for dropped deliveries on a subscriber whose
	// buffer overflowed. Nil keeps plain drop-on-full semantics, which is the
	// wakeup contract (polling covers the loss).
	reset func() T

	mu          sync.Mutex
	subscribers []*subscriber[T]
	started     bool
	// connected reports a live LISTEN. It gates the reset a new subscription
	// opens with: delivered immediately only when the stream can already
	// observe changes, deferred to the connection-established broadcast
	// otherwise — so "reset received" always means "refetch now; a change
	// committed after this point is observed or announced by a further reset".
	connected  bool
	closed     bool
	lifeCancel context.CancelFunc
	done       chan struct{}
	wg         sync.WaitGroup
}

// subscriber is one live subscription: its channel plus the overflow flag
// that turns drops into an in-band reset when the listener carries one.
type subscriber[T any] struct {
	ch         chan T
	overflowed bool
}

func newListener[T any](
	pool *pgxpool.Pool, channel string, pollOnly bool, logger *slog.Logger,
	buffer int, parse func(string) (T, bool), reset func() T,
) *listener[T] {
	return &listener[T]{
		pool:     pool,
		channel:  channel,
		pollOnly: pollOnly,
		logger:   logger,
		buffer:   buffer,
		parse:    parse,
		reset:    reset,
		done:     make(chan struct{}),
	}
}

// Wake returns a channel of wakeups signaled by enqueues and publishes. It may
// be called several times; each caller gets its own channel, closed when its ctx
// ends. A poll-only listener returns a nil channel and nil error.
func (s *Store) Wake(ctx context.Context) (<-chan driver.Wake, error) {
	return s.listener.subscribe(ctx)
}

func (l *listener[T]) subscribe(ctx context.Context) (<-chan T, error) {
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
	sub := &subscriber[T]{ch: make(chan T, l.buffer)}
	if l.reset != nil && l.connected {
		// The stream is already live, so the opening reset can go out now:
		// refetching on it misses nothing. On a not-yet-connected listener
		// the reset is deferred to the connection-established broadcast —
		// sending it earlier would invite a refetch whose follow-up changes
		// LISTEN cannot observe yet. The channel is freshly made with a
		// positive buffer; never blocks.
		sub.ch <- l.reset()
	}
	l.subscribers = append(l.subscribers, sub)

	// Register the unregister goroutine under l.mu so a concurrent close() cannot
	// run its wg.Wait() before this goroutine is added: with the Add happening
	// under the same lock that close() takes to set l.closed, either this
	// subscribe observes the closed listener and adds nothing, or close()
	// observes the added goroutine and waits for it. That closes the TOCTOU
	// window that a wg.Go after unlocking would leave (a WaitGroup Add racing
	// a Wait).
	l.wg.Go(func() {
		select {
		case <-ctx.Done():
		case <-l.done:
		}
		l.removeSubscriber(sub)
	})
	return sub.ch, nil
}

// ensureStartedLocked starts the single LISTEN loop on the first subscription.
// The loop's lifetime is the store's, not any one caller's, so it runs on an
// independent context cancelled by close. Callers hold l.mu.
//
//nolint:contextcheck // deliberate: the listen loop outlives any single subscribe ctx
func (l *listener[T]) ensureStartedLocked() {
	if l.started {
		return
	}
	l.started = true
	//nolint:gosec // G118 false positive: the cancel func is stored on the struct and called by close()
	lifeCtx, cancel := context.WithCancel(context.Background())
	l.lifeCancel = cancel
	l.wg.Go(func() { l.listenLoop(lifeCtx) })
}

func (l *listener[T]) listenLoop(ctx context.Context) {
	backoff := listenBackoffMin
	for ctx.Err() == nil {
		err := l.listenOnce(ctx, func() {
			backoff = listenBackoffMin
			// Every successful LISTEN — the first and every reconnect —
			// broadcasts a reset: notifications sent while the connection was
			// down are lost, and this is the only honest signal. It also
			// delivers the opening reset to subscriptions made before the
			// stream went live (see subscribe).
			l.markConnectedAndReset()
		})
		l.markDisconnected()
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			l.logger.Warn("azync listener connection lost, reconnecting",
				"channel", l.channel, "error", err, "backoff", backoff)
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
func (l *listener[T]) listenOnce(ctx context.Context, onConnect func()) error {
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
		if v, ok := l.parse(notification.Payload); ok {
			l.broadcast(v)
		}
	}
}

// broadcast delivers a parsed notification to every live subscriber without
// blocking. Sends run under l.mu so a channel removed and closed by
// removeSubscriber can never be sent to afterward.
func (l *listener[T]) broadcast(v T) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, sub := range l.subscribers {
		l.deliverLocked(sub, v)
	}
}

// markConnectedAndReset records the live LISTEN and broadcasts one reset to
// every subscriber, on listeners that carry a reset. One critical section, so
// no subscription can slip between the flag and the broadcast and miss both
// paths that deliver its opening reset.
func (l *listener[T]) markConnectedAndReset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.connected = true
	if l.reset == nil {
		return
	}
	for _, sub := range l.subscribers {
		l.deliverLocked(sub, l.reset())
	}
}

// markDisconnected records that the LISTEN connection is gone, so new
// subscriptions defer their opening reset to the next reconnect broadcast.
func (l *listener[T]) markDisconnected() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.connected = false
}

// deliverLocked sends without blocking. Without a reset func a full buffer
// simply drops the value. With one, a drop marks the subscriber overflowed
// and its next delivery is preceded by a reset — the gap is announced
// in-band, never silent. Callers hold l.mu.
func (l *listener[T]) deliverLocked(sub *subscriber[T], v T) {
	if sub.overflowed && l.reset != nil {
		select {
		case sub.ch <- l.reset():
			sub.overflowed = false
		default:
			return // still full; the pending reset stands in for every drop
		}
	}
	select {
	case sub.ch <- v:
	default:
		sub.overflowed = true
	}
}

func (l *listener[T]) removeSubscriber(sub *subscriber[T]) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, s := range l.subscribers {
		if s == sub {
			l.subscribers = slices.Delete(l.subscribers, i, i+1)
			close(s.ch)
			return
		}
	}
}

// close stops the listen loop and releases every subscriber. It is idempotent.
func (l *listener[T]) close() {
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
