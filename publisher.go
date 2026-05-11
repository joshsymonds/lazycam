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
type Publisher interface {
	Publish(state State, refCount int, ts time.Time) error
	Close() error
}

const (
	publisherSocketMode   = 0o600
	publisherWriteTimeout = time.Second
)

// unixSocketPublisher serves newline-delimited JSON events over a UNIX
// domain socket. Subscribers receive a snapshot on connect (so widgets
// that start mid-session render correctly) plus every subsequent event.
// Failed writes drop the subscriber silently — a broken pipe is normal
// when a client exits, not a daemon-level problem.
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
func newPublisher(ctx context.Context, logger *slog.Logger, path string) (*unixSocketPublisher, error) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale socket %q: %w", path, err)
	}
	var listenCfg net.ListenConfig
	listener, err := listenCfg.Listen(ctx, "unix", path)
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
// updates the snapshot served to future connectors. Subscribers whose
// write fails are dropped (broken pipe / closed conn).
func (p *unixSocketPublisher) Publish(state State, refCount int, ts time.Time) error {
	ev := publishedEvent{State: state, RefCount: refCount, Timestamp: ts.UTC()}
	line, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	line = append(line, '\n')

	// Snapshot the subscriber set under the lock; release before
	// writing so a slow client doesn't block other writes.
	p.mu.Lock()
	p.lastState = ev
	subs := make([]net.Conn, 0, len(p.subscribers))
	for sub := range p.subscribers {
		subs = append(subs, sub)
	}
	p.mu.Unlock()

	for _, sub := range subs {
		p.writeToSubscriber(sub, line)
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

// registerSubscriber adds the conn to the subscriber set and sends
// the current state snapshot. If the snapshot write fails, the conn
// is dropped immediately.
func (p *unixSocketPublisher) registerSubscriber(ctx context.Context, conn net.Conn) {
	p.mu.Lock()
	p.subscribers[conn] = struct{}{}
	snapshot := p.lastState
	p.mu.Unlock()

	line, err := json.Marshal(snapshot)
	if err != nil {
		p.logger.WarnContext(ctx, "marshal snapshot failed", "err", err)
		p.dropSubscriber(conn)
		return
	}
	line = append(line, '\n')
	p.writeToSubscriber(conn, line)
}

// writeToSubscriber writes one line to conn with a bounded deadline.
// On failure, drops the subscriber. No logging — broken pipe on a
// peer-closed conn is expected during normal operation.
func (p *unixSocketPublisher) writeToSubscriber(conn net.Conn, line []byte) {
	if err := conn.SetWriteDeadline(time.Now().Add(publisherWriteTimeout)); err != nil {
		p.dropSubscriber(conn)
		return
	}
	if _, err := conn.Write(line); err != nil {
		p.dropSubscriber(conn)
	}
}

// dropSubscriber removes conn from the subscriber set and closes it.
// Safe to call multiple times for the same conn.
func (p *unixSocketPublisher) dropSubscriber(conn net.Conn) {
	p.mu.Lock()
	delete(p.subscribers, conn)
	p.mu.Unlock()
	if err := conn.Close(); err != nil {
		p.logger.Debug("subscriber close returned error", "err", err)
	}
}
