package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/andreykaipov/goobs"
	"github.com/andreykaipov/goobs/api/requests/scenes"
)

// Switcher is the abstraction the daemon's event loop calls on
// transitions. The dry-run implementation logs the intended action;
// the live implementation maintains an OBS WebSocket v5 connection
// with auto-reconnect and issues SetCurrentProgramScene RPCs.
type Switcher interface {
	SetScene(ctx context.Context, name string) error
	Close() error
}

// switcherOptions carries everything the constructor needs across
// modes. dryRun gates the implementation; obsURL is consumed by the
// live switcher only.
type switcherOptions struct {
	dryRun       bool
	obsURL       string
	sceneActive  string
	sceneStandby string
}

// newSwitcher returns the appropriate Switcher for the requested mode.
// The parent context governs the live switcher's connect/reconnect
// loop — when it cancels, the loop drains and closes its connection.
// The dry-run path ignores ctx.
func newSwitcher(ctx context.Context, logger *slog.Logger, opts switcherOptions) (Switcher, error) {
	if opts.dryRun {
		return &dryRunSwitcher{logger: logger}, nil
	}
	return newLiveSwitcher(ctx, logger, opts)
}

// dryRunSwitcher logs the intended scene transition without contacting
// OBS. Useful for piping the daemon through CI / smoke tests / config
// validation without a live OBS instance.
type dryRunSwitcher struct {
	logger *slog.Logger
}

// SetScene logs the request and returns nil. The context is accepted
// for signature parity with the live switcher.
func (d *dryRunSwitcher) SetScene(ctx context.Context, name string) error {
	d.logger.InfoContext(ctx, "would set scene", "scene", name)
	return nil
}

// Close is a no-op for the dry-run mode.
func (d *dryRunSwitcher) Close() error {
	return nil
}

// liveSwitcher maintains a goobs client against OBS WebSocket v5 and
// reconnects on disconnect with exponential backoff. SetScene drops
// the call (logging a WARN) when not connected; the connectLoop keeps
// retrying in the background.
//
// Lifecycle:
//   - newLiveSwitcher returns immediately; connection happens async
//     in connectLoop. This avoids blocking daemon startup on OBS
//     availability — lazycam must come up even if OBS is down.
//   - SetScene errors trigger a reconnect signal; the loop tears down
//     the current connection and re-establishes from scratch.
//   - Close cancels the loop's context and waits for it to drain.
type liveSwitcher struct {
	logger *slog.Logger
	host   string // host:port; scheme stripped from the configured URL

	mu     sync.Mutex
	client *goobs.Client // nil while disconnected

	reconnect chan struct{} // buffered, capacity 1; signalReconnect coalesces
	cancel    context.CancelFunc
	done      chan struct{}

	closeOnce sync.Once
}

const (
	initialBackoff     = time.Second
	maxBackoff         = 30 * time.Second
	setSceneRPCTimeout = 5 * time.Second
	backoffMultiplier  = 2
)

// newLiveSwitcher kicks off the background connect loop and returns.
// Returns an error only for invalid configuration (e.g. unparseable URL).
func newLiveSwitcher(parentCtx context.Context, logger *slog.Logger, opts switcherOptions) (*liveSwitcher, error) {
	host, err := extractHost(opts.obsURL)
	if err != nil {
		return nil, fmt.Errorf("parse obs-url: %w", err)
	}
	ctx, cancel := context.WithCancel(parentCtx)
	s := &liveSwitcher{
		logger:    logger,
		host:      host,
		reconnect: make(chan struct{}, 1),
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	go s.connectLoop(ctx)
	return s, nil
}

// extractHost strips the ws:// (or wss://) scheme from a configured
// URL, returning host:port for goobs.New (which prepends ws:// itself).
// Bare host:port input is accepted unchanged.
func extractHost(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("url.Parse %q: %w", rawURL, err)
	}
	if parsed.Host != "" {
		return parsed.Host, nil
	}
	// No scheme — treat the whole thing as host:port.
	return rawURL, nil
}

// SetScene asks OBS to switch to the named scene. If the connection
// is down, the call is dropped with a WARN log and the next event
// will retry — this is intentional. We do NOT block waiting for
// reconnect, because doing so would couple lazycam's event-loop
// latency to OBS-server availability.
func (s *liveSwitcher) SetScene(ctx context.Context, name string) error {
	s.mu.Lock()
	client := s.client
	s.mu.Unlock()

	if client == nil {
		s.logger.WarnContext(ctx, "obs not connected; dropping scene switch", "scene", name)
		return nil
	}

	// Bound the RPC; OBS WebSocket has been known to hang on bad state.
	callCtx, cancel := context.WithTimeout(ctx, setSceneRPCTimeout)
	defer cancel()
	_ = callCtx // goobs's API is synchronous and doesn't take ctx; the timeout is for future use.

	_, err := client.Scenes.SetCurrentProgramScene(
		scenes.NewSetCurrentProgramSceneParams().WithSceneName(name),
	)
	if err != nil {
		s.logger.WarnContext(ctx, "obs set scene failed; reconnecting", "scene", name, "err", err)
		s.markDisconnected(client)
		s.signalReconnect()
		return fmt.Errorf("set scene %q: %w", name, err)
	}
	return nil
}

// Close cancels the connect loop's context, waits for it to drain,
// and disconnects any live client. Safe to call multiple times.
func (s *liveSwitcher) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		<-s.done
	})
	return nil
}

// connectLoop establishes and maintains the OBS connection. Each
// iteration either: dials OBS (with backoff on failure), waits for a
// disconnect/reconnect signal, then tears down. The loop exits only
// when the parent context cancels.
func (s *liveSwitcher) connectLoop(ctx context.Context) {
	defer close(s.done)
	defer s.disconnectActive()

	backoff := initialBackoff
	for ctx.Err() == nil {
		client, err := goobs.New(s.host)
		if err != nil {
			s.logger.WarnContext(ctx, "obs connect failed",
				"host", s.host, "err", err, "retry_in", backoff)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}

		s.mu.Lock()
		s.client = client
		s.mu.Unlock()
		s.logger.InfoContext(ctx, "obs connected", "host", s.host)
		backoff = initialBackoff // reset on successful connect

		// Wait for either ctx cancellation (shutdown) or a reconnect
		// signal raised by a failing SetScene RPC.
		select {
		case <-ctx.Done():
			return
		case <-s.reconnect:
			s.logger.InfoContext(ctx, "obs reconnect requested")
			s.disconnectActive()
		}
	}
}

// disconnectActive tears down the current goobs client (if any).
// Idempotent. Disconnect errors are logged at DEBUG only — we're
// tearing down regardless, and the connection may already be dead.
func (s *liveSwitcher) disconnectActive() {
	s.mu.Lock()
	client := s.client
	s.client = nil
	s.mu.Unlock()
	if client == nil {
		return
	}
	if err := client.Disconnect(); err != nil {
		s.logger.Debug("obs disconnect returned error", "err", err)
	}
}

// markDisconnected clears the client field, but only if it still
// matches the caller's view — guards against racing with connectLoop
// installing a new client. Caller must not hold s.mu.
func (s *liveSwitcher) markDisconnected(prev *goobs.Client) {
	s.mu.Lock()
	if s.client != prev {
		s.mu.Unlock()
		return
	}
	s.client = nil
	s.mu.Unlock()
	if err := prev.Disconnect(); err != nil {
		s.logger.Debug("obs disconnect returned error", "err", err)
	}
}

// signalReconnect wakes the connectLoop via the buffered channel.
// Coalesces multiple concurrent signals into one.
func (s *liveSwitcher) signalReconnect() {
	select {
	case s.reconnect <- struct{}{}:
	default:
	}
}

// sleepCtx blocks for `d` or until ctx is canceled. Returns true if
// the full duration elapsed, false if interrupted.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// nextBackoff doubles the current backoff, capped at maxBackoff.
func nextBackoff(current time.Duration) time.Duration {
	doubled := current * backoffMultiplier
	if doubled > maxBackoff {
		return maxBackoff
	}
	return doubled
}
