package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/andreykaipov/goobs"
	"github.com/andreykaipov/goobs/api/requests/inputs"
	"github.com/andreykaipov/goobs/api/requests/scenes"
	"github.com/gorilla/websocket"
)

// Switcher is the abstraction the daemon's event loop calls on
// transitions. The dry-run implementation logs the intended action;
// the live implementation maintains an OBS WebSocket v5 connection
// with auto-reconnect and issues SetCurrentProgramScene and
// SetInputSettings RPCs.
//
// SetCameraDevice gates the underlying v4l2 file descriptor by
// rewriting the device_id setting of an OBS input source. With
// deviceID = "/dev/video0" OBS opens the camera (hardware LED on);
// with deviceID = "" OBS's v4l2 plugin fails the reopen and the
// previous fd is released (LED off). This works around OBS's
// v4l2_input plugin lacking show/hide hooks — without this gate
// the LED would stay lit for the entire OBS process lifetime,
// breaking lazycam's whole reason for existing. Pass sourceName=""
// to skip the gate entirely (back-compat for setups that don't
// want device-level gating).
type Switcher interface {
	SetScene(ctx context.Context, name string) error
	SetCameraDevice(ctx context.Context, sourceName, deviceID string) error
	Close() error
}

// switcherOptions carries everything the constructor needs across
// modes. dryRun gates the implementation; obsURL is consumed by the
// live switcher only. Scene names are owned by the daemon (not the
// Switcher) since each transition picks one — the switcher's API is
// stateless `SetScene(ctx, name)`.
type switcherOptions struct {
	dryRun bool
	obsURL string
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

// SetCameraDevice logs the intended device-id rewrite. Empty
// sourceName is a no-op (matches the live switcher's contract).
func (d *dryRunSwitcher) SetCameraDevice(ctx context.Context, sourceName, deviceID string) error {
	if sourceName == "" {
		return nil
	}
	d.logger.InfoContext(ctx, "would set camera device",
		"source", sourceName, "device_id", deviceID)
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
	initialBackoff    = time.Second
	maxBackoff        = 30 * time.Second
	backoffMultiplier = 2

	// handshakeTimeout bounds the worst-case time goobs.New can block.
	// gorilla/websocket's default is 45s, which combined with goobs's
	// 10s response timeout would let SIGTERM stall Close() up to ~55s
	// while a dial is in flight. Capping both at 5s keeps shutdown
	// snappy without sacrificing realistic LAN/loopback connect time.
	handshakeTimeout = 5 * time.Second
	rpcTimeout       = 5 * time.Second
)

// newLiveSwitcher kicks off the background connect loop and returns.
// Returns an error only for invalid configuration (e.g. unparseable
// URL, non-loopback host).
func newLiveSwitcher(parentCtx context.Context, logger *slog.Logger, opts switcherOptions) (*liveSwitcher, error) {
	host, err := extractHost(opts.obsURL)
	if err != nil {
		return nil, fmt.Errorf("parse obs-url: %w", err)
	}
	if lerr := requireLoopback(host); lerr != nil {
		return nil, fmt.Errorf("obs-url is not loopback: %w", lerr)
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
// URL and returns host:port for goobs.New (which prepends ws:// itself).
// Bare host:port input is accepted unchanged. Returns an error for an
// empty input or for a host without an explicit port — both would
// otherwise drive connectLoop into an infinite retry against an
// unreachable target with no operator-visible cause.
//
// The bare-host:port path bypasses url.Parse: Go's url package rejects
// inputs like "127.0.0.1:4455" because it treats "127.0.0.1" as a
// scheme. We detect scheme presence via "://" and fall back to a plain
// `:`-contains check when none is present.
func extractHost(rawURL string) (string, error) {
	if rawURL == "" {
		return "", fmt.Errorf("obs-url is empty")
	}
	var host string
	if strings.Contains(rawURL, "://") {
		parsed, err := url.Parse(rawURL)
		if err != nil {
			return "", fmt.Errorf("url.Parse %q: %w", rawURL, err)
		}
		host = parsed.Host
	} else {
		// No scheme — treat the whole thing as host:port.
		host = rawURL
	}
	if host == "" {
		return "", fmt.Errorf("obs-url %q yielded empty host", rawURL)
	}
	if !strings.Contains(host, ":") {
		return "", fmt.Errorf("obs-url %q missing port; expected host:port", rawURL)
	}
	return host, nil
}

// localhostName is the only hostname (non-IP) we accept for loopback
// validation. Pulled out as a constant to satisfy goconst.
const localhostName = "localhost"

// requireLoopback rejects an obs-url that points anywhere other than
// 127.0.0.0/8, ::1, or localhost. lazycam disables OBS WebSocket auth
// by design (the loopback-only invariant IS the auth boundary), so
// pointing at a remote OBS would be a privilege escalation footgun.
// The module documentation in nix/home-manager-module.nix explicitly
// promises "loopback only"; this validator enforces it.
func requireLoopback(hostPort string) error {
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		return fmt.Errorf("split host:port %q: %w", hostPort, err)
	}
	if host == localhostName {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("obs-url host %q is not an IP literal or %s; "+
			"refuse to dial off-loopback", host, localhostName)
	}
	if !ip.IsLoopback() {
		return fmt.Errorf("obs-url host %q is not a loopback address", host)
	}
	return nil
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

// SetCameraDevice issues a SetInputSettings RPC that overlays
// {device_id: deviceID} onto the named source. With deviceID set to
// the real camera path, OBS's v4l2 plugin opens the device (LED on);
// with deviceID="" the open fails and the prior fd is released
// (LED off). Overlay=true so other settings (pixelformat, buffering,
// etc.) survive untouched.
//
// Like SetScene, drops the call (WARN) when not connected so a
// transient OBS WebSocket loss doesn't kill the daemon.
func (s *liveSwitcher) SetCameraDevice(ctx context.Context, sourceName, deviceID string) error {
	if sourceName == "" {
		return nil
	}

	s.mu.Lock()
	client := s.client
	s.mu.Unlock()

	if client == nil {
		s.logger.WarnContext(ctx, "obs not connected; dropping camera device set",
			"source", sourceName, "device_id", deviceID)
		return nil
	}

	overlay := true
	_, err := client.Inputs.SetInputSettings(
		inputs.NewSetInputSettingsParams().
			WithInputName(sourceName).
			WithInputSettings(map[string]any{"device_id": deviceID}).
			WithOverlay(overlay),
	)
	if err != nil {
		s.logger.WarnContext(ctx, "obs set input settings failed; reconnecting",
			"source", sourceName, "device_id", deviceID, "err", err)
		s.markDisconnected(client)
		s.signalReconnect()
		return fmt.Errorf("set camera device %q on %q: %w", deviceID, sourceName, err)
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

	// Bound goobs.New's blocking time so SIGTERM doesn't stall Close()
	// waiting on the ~45s gorilla/websocket handshake default. The dialer
	// is shared across reconnect iterations; goobs reads it once per
	// New() call.
	dialer := &websocket.Dialer{HandshakeTimeout: handshakeTimeout}

	backoff := initialBackoff
	for ctx.Err() == nil {
		client, err := goobs.New(s.host,
			goobs.WithDialer(dialer),
			goobs.WithResponseTimeoutDuration(rpcTimeout),
		)
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
