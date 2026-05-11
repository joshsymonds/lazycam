package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
)

// State is the daemon's externally-visible state: whether anything is
// currently holding a consumer fd on the watched device.
type State string

// State variants published over the UNIX socket.
const (
	StateIdle   State = "idle"
	StateActive State = "active"
)

// publishedEvent is the on-the-wire schema (v0). JSON tags use
// camelCase to match the team's tagliatelle config; the field set is
// the public protocol contract with the DMS indicator widget and any
// other future subscriber.
type publishedEvent struct {
	State     State     `json:"state"`
	RefCount  int       `json:"refCount"`
	Timestamp time.Time `json:"timestamp"`
}

// Publisher exposes daemon state to external subscribers (e.g. the
// DMS quickshell indicator). The unix-socket implementation broadcasts
// each Publish to every connected subscriber and sends a snapshot of
// the most recent state to each new subscriber on connect.
//
// Publish returns a non-nil error only for marshal failures (which
// would indicate a schema mismatch the daemon shouldn't ignore).
// Per-subscriber write failures are absorbed: a broken pipe to one
// client is not a daemon-level event and the failed subscriber is
// silently dropped. A nil return therefore means broadcast was
// attempted; it is NOT a delivery confirmation.
type Publisher interface {
	Publish(state State, refCount int, ts time.Time) error
	Close() error
}

const (
	publisherSocketMode   = 0o600
	publisherSocketUmask  = 0o077
	publisherWriteTimeout = time.Second
)

// unixSocketPublisher serves newline-delimited JSON events over a UNIX
// domain socket. Subscribers receive a snapshot on connect (so widgets
// that start mid-session render correctly) plus every subsequent event.
// Failed writes drop the subscriber silently — a broken pipe is normal
// when a client exits, not a daemon-level problem.
//
// Lock discipline: p.mu guards p.subscribers, p.lastState, and serializes
// all subscriber writes. registerSubscriber writes the snapshot AND adds
// the conn to the set under a single lock-hold; Publish iterates and
// writes under the same lock. The combination guarantees a freshly-
// registered subscriber sees the snapshot before any subsequent
// broadcast — preventing the ordering race where Publish could write
// S2 to the new conn before registerSubscriber's deferred S1 lands.
type unixSocketPublisher struct {
	logger   *slog.Logger
	path     string
	listener net.Listener

	mu          sync.Mutex
	subscribers map[net.Conn]struct{}
	lastState   publishedEvent

	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

// newPublisher binds a UNIX socket at path (removing any stale file
// from a previous run), chmods it 0600, and spawns the accept loop.
// The parent context governs lifetime: when it cancels, the loop
// drains and the listener closes — Close finishes the teardown
// synchronously.
//
// The bind is wrapped in a temporary umask of 0o077 so the socket
// file is created restrictively from the start; the subsequent Chmod
// is belt-and-suspenders. Without the umask wrapper, the socket would
// be created with `0666 & ~process-umask` and could be observed by
// same-user processes during the microsecond window before Chmod —
// not a cross-user risk under $XDG_RUNTIME_DIR (mode 0700), but a
// real one for explicit `--state-socket /tmp/...` configurations.
func newPublisher(ctx context.Context, logger *slog.Logger, path string) (*unixSocketPublisher, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale socket %q: %w", path, err)
	}

	// Tighten umask for the duration of Listen so the socket is
	// created with no group/other bits. syscall.Umask is process-wide;
	// it returns the previous value which we restore immediately.
	oldUmask := syscall.Umask(publisherSocketUmask)
	var listenCfg net.ListenConfig
	listener, err := listenCfg.Listen(ctx, "unix", path)
	syscall.Umask(oldUmask)
	if err != nil {
		return nil, fmt.Errorf("listen unix %q: %w", path, err)
	}
	if cherr := os.Chmod(path, publisherSocketMode); cherr != nil {
		if closeErr := listener.Close(); closeErr != nil {
			logger.WarnContext(ctx, "listener close after chmod failure", "err", closeErr)
		}
		return nil, fmt.Errorf("chmod %q: %w", path, cherr)
	}

	childCtx, cancel := context.WithCancel(ctx)
	p := &unixSocketPublisher{
		logger:      logger,
		path:        path,
		listener:    listener,
		subscribers: make(map[net.Conn]struct{}),
		cancel:      cancel,
		done:        make(chan struct{}),
		lastState: publishedEvent{
			State:     StateIdle,
			RefCount:  0,
			Timestamp: time.Now().UTC(),
		},
	}
	go p.acceptLoop(childCtx)
	return p, nil
}

// Publish broadcasts the new state to every connected subscriber AND
// updates the snapshot served to future connectors. Holds p.mu across
// the per-subscriber writes; this serializes against registerSubscriber's
// snapshot delivery, guaranteeing new subscribers see the snapshot
// before any subsequent broadcast. See the lock-discipline comment on
// unixSocketPublisher.
//
// Returns an error only if event marshaling fails. Per-subscriber write
// failures drop the subscriber silently — see the Publisher interface
// docstring.
func (p *unixSocketPublisher) Publish(state State, refCount int, ts time.Time) error {
	ev := publishedEvent{State: state, RefCount: refCount, Timestamp: ts.UTC()}
	line, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	line = append(line, '\n')

	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastState = ev
	for sub := range p.subscribers {
		if !p.writeLineLocked(sub, line) {
			delete(p.subscribers, sub)
			if cerr := sub.Close(); cerr != nil {
				p.logger.Debug("subscriber close returned error", "err", cerr)
			}
		}
	}
	return nil
}

// Close cancels the accept loop, waits for it to drain, then closes
// all subscriber conns and unlinks the socket file. Idempotent.
func (p *unixSocketPublisher) Close() error {
	var closeErr error
	p.closeOnce.Do(func() {
		p.cancel()
		<-p.done
		p.mu.Lock()
		for sub := range p.subscribers {
			if cerr := sub.Close(); cerr != nil {
				p.logger.Debug("subscriber close returned error", "err", cerr)
			}
		}
		p.subscribers = nil
		p.mu.Unlock()
		if err := os.Remove(p.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			closeErr = fmt.Errorf("remove socket %q: %w", p.path, err)
		}
	})
	return closeErr
}

// acceptLoop accepts new subscribers until the parent ctx cancels.
// Closes the listener on shutdown so Accept returns immediately.
func (p *unixSocketPublisher) acceptLoop(ctx context.Context) {
	defer close(p.done)
	// Close the listener on ctx cancel; Accept will return an error
	// that we detect by checking ctx.Err.
	go func() {
		<-ctx.Done()
		if cerr := p.listener.Close(); cerr != nil {
			p.logger.DebugContext(ctx, "listener close returned error", "err", cerr)
		}
	}()

	for {
		conn, err := p.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.logger.WarnContext(ctx, "accept failed", "err", err)
			return
		}
		p.registerSubscriber(ctx, conn)
	}
}

// registerSubscriber writes the current state snapshot to the new
// subscriber and adds it to the broadcast set — all under p.mu so a
// concurrent Publish cannot interleave a later event before our
// snapshot lands. If the snapshot write fails, the conn is closed and
// not added (so a slow-or-dead first read doesn't poison the set).
//
// A per-conn watcher goroutine is started on success to detect peer
// disconnection via conn.Read returning EOF — without it, dead
// subscribers would linger until the next Publish attempted a write.
func (p *unixSocketPublisher) registerSubscriber(ctx context.Context, conn net.Conn) {
	p.mu.Lock()
	snapshot := p.lastState
	line, err := json.Marshal(snapshot)
	if err != nil {
		p.mu.Unlock()
		p.logger.WarnContext(ctx, "marshal snapshot failed", "err", err)
		if cerr := conn.Close(); cerr != nil {
			p.logger.DebugContext(ctx, "subscriber close returned error", "err", cerr)
		}
		return
	}
	line = append(line, '\n')
	if !p.writeLineLocked(conn, line) {
		p.mu.Unlock()
		if cerr := conn.Close(); cerr != nil {
			p.logger.DebugContext(ctx, "subscriber close returned error", "err", cerr)
		}
		return
	}
	p.subscribers[conn] = struct{}{}
	p.mu.Unlock()

	go p.watchConn(conn)
}

// writeLineLocked writes one line to conn with the configured deadline.
// Caller must hold p.mu. Returns true on success, false on any write
// error (caller is responsible for evicting and closing).
func (p *unixSocketPublisher) writeLineLocked(conn net.Conn, line []byte) bool {
	if err := conn.SetWriteDeadline(time.Now().Add(publisherWriteTimeout)); err != nil {
		return false
	}
	if _, err := conn.Write(line); err != nil {
		return false
	}
	return true
}

// watchConn blocks on conn.Read so we notice peer disconnection without
// waiting for the next Publish attempt. lazycam's protocol is one-way
// (daemon → subscriber), so any byte received is discarded; the
// signal we care about is the error return when the peer closes.
func (p *unixSocketPublisher) watchConn(conn net.Conn) {
	buf := make([]byte, 1)
	for {
		if _, err := conn.Read(buf); err != nil {
			p.dropSubscriber(conn)
			return
		}
	}
}

// dropSubscriber removes conn from the subscriber set and closes it.
// Safe to call multiple times for the same conn — second call is a
// no-op because the conn is already absent from the set and Close on
// an already-closed conn returns ErrClosed (downgraded to Debug).
func (p *unixSocketPublisher) dropSubscriber(conn net.Conn) {
	p.mu.Lock()
	_, present := p.subscribers[conn]
	if present {
		delete(p.subscribers, conn)
	}
	p.mu.Unlock()
	if !present {
		return
	}
	if err := conn.Close(); err != nil {
		p.logger.Debug("subscriber close returned error", "err", err)
	}
}

// subscriberCount returns the current number of attached subscribers.
// Intended for tests; exported within the package only.
func (p *unixSocketPublisher) subscriberCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.subscribers)
}
