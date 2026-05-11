// Command lazycam — on-demand v4l2loopback producer gating.
//
// Watches a v4l2 device (default /dev/video10) with inotify, maintains
// a consumer ref-count, and asks OBS to switch scenes on 0↔N
// transitions so the real camera handle (and its hardware LED) is only
// held while something is actually using the loopback.
//
// Live OBS WebSocket integration arrives in task #5; this iteration
// wires the Switcher abstraction with a dry-run implementation that
// logs intended scene transitions.
//
// See https://github.com/joshsymonds/lazycam for the design epic.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// config bundles the CLI options into a single value so run() and its
// helpers don't need long positional argument lists.
type config struct {
	device       string
	sceneActive  string
	sceneStandby string
	obsURL       string
	stateSocket  string
	cameraSource string
	cameraDevice string
	dryRun       bool
	debug        bool
}

// daemon holds the per-run state the event loop folds events into.
// Kept thread-unsafe — only the main run loop's goroutine mutates it.
type daemon struct {
	logger       *slog.Logger
	switcher     Switcher
	publisher    Publisher
	tracker      Tracker
	sceneActive  string
	sceneStandby string
	cameraSource string
	cameraDevice string
}

func main() {
	var cfg config
	flag.StringVar(&cfg.device, "device", "/dev/video10",
		"v4l2 device path to watch for opens/closes")
	flag.StringVar(&cfg.sceneActive, "scene-active", "Active",
		"OBS scene name to switch to when a consumer attaches")
	flag.StringVar(&cfg.sceneStandby, "scene-standby", "Standby",
		"OBS scene name to switch to when the last consumer releases")
	flag.StringVar(&cfg.obsURL, "obs-url", "ws://127.0.0.1:4455",
		"OBS WebSocket v5 endpoint (ws://host:port or bare host:port)")
	flag.BoolVar(&cfg.dryRun, "dry-run", false,
		"log intended scene transitions instead of contacting OBS")
	flag.BoolVar(&cfg.debug, "debug", false,
		"log every inotify event (otherwise only 0↔N transitions are logged)")
	flag.StringVar(&cfg.stateSocket, "state-socket", "",
		"UNIX socket path to publish state events on (default $XDG_RUNTIME_DIR/lazycam.sock)")
	flag.StringVar(&cfg.cameraSource, "camera-source", "",
		"OBS input name to gate via device-id swap; empty disables device-level gating")
	flag.StringVar(&cfg.cameraDevice, "camera-device", "/dev/video0",
		"v4l2 device path written on Activate; cleared on Deactivate to release the LED")
	flag.Parse()

	if cfg.stateSocket == "" {
		runtimeDir := os.Getenv("XDG_RUNTIME_DIR")
		if runtimeDir == "" {
			fmt.Fprintln(os.Stderr,
				"lazycam: XDG_RUNTIME_DIR is unset and --state-socket not provided")
			os.Exit(1)
		}
		cfg.stateSocket = runtimeDir + "/lazycam.sock"
	}

	level := slog.LevelInfo
	if cfg.debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	}))

	if err := run(logger, cfg); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, cfg config) error {
	if _, err := os.Stat(cfg.device); err != nil {
		return fmt.Errorf("device %q not accessible: %w", cfg.device, err)
	}

	// Signal-handler context — drives switcher lifecycle, inotify
	// shutdown, and the main select loop's exit. Built early so the
	// switcher's connect/reconnect loop can be tied to it.
	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	switcher, err := newSwitcher(ctx, logger, switcherOptions{
		dryRun: cfg.dryRun,
		obsURL: cfg.obsURL,
	})
	if err != nil {
		return fmt.Errorf("switcher: %w", err)
	}
	defer func() {
		if cerr := switcher.Close(); cerr != nil {
			logger.Warn("switcher close failed", "err", cerr)
		}
	}()

	publisher, err := newPublisher(ctx, logger, cfg.stateSocket)
	if err != nil {
		return fmt.Errorf("publisher: %w", err)
	}
	defer func() {
		if cerr := publisher.Close(); cerr != nil {
			logger.Warn("publisher close failed", "err", cerr)
		}
	}()

	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC)
	if err != nil {
		return fmt.Errorf("inotify_init1: %w", err)
	}
	defer func() {
		if cerr := unix.Close(fd); cerr != nil {
			logger.Warn("inotify fd close failed", "err", cerr)
		}
	}()

	mask := uint32(unix.IN_OPEN | unix.IN_CLOSE_NOWRITE | unix.IN_CLOSE_WRITE)
	wd, err := unix.InotifyAddWatch(fd, cfg.device, mask)
	if err != nil {
		return fmt.Errorf("inotify_add_watch %q: %w", cfg.device, err)
	}
	defer func() {
		// InotifyAddWatch returns a non-negative int on success, so the
		// uint32 conversion is safe; gosec can't infer the invariant.
		//nolint:gosec // wd is non-negative on success per inotify(7)
		if _, rerr := unix.InotifyRmWatch(fd, uint32(wd)); rerr != nil {
			logger.Warn("inotify rm_watch failed", "err", rerr)
		}
	}()

	logger.Info("watching", "device", cfg.device)

	// inotify reads block; do them on a goroutine and surface results on
	// channels so the main loop can select on ctx for shutdown.
	events := make(chan unix.InotifyEvent, 16)
	readErr := make(chan error, 1)
	go readLoop(ctx, fd, events, readErr)

	d := &daemon{
		logger:       logger,
		switcher:     switcher,
		publisher:    publisher,
		sceneActive:  cfg.sceneActive,
		sceneStandby: cfg.sceneStandby,
		cameraSource: cfg.cameraSource,
		cameraDevice: cfg.cameraDevice,
	}
	for {
		select {
		case <-ctx.Done():
			logger.Info("shutting down")
			return nil
		case ev := <-events:
			d.handleEvent(ctx, ev)
		case err := <-readErr:
			return fmt.Errorf("inotify read: %w", err)
		}
	}
}

// handleEvent folds one inotify event into the tracker and dispatches.
// Activate / Deactivate transitions always log at INFO so an operator
// running without --debug still sees the events that actually change
// the daemon's effect on the world; None-events log at DEBUG only.
// Switcher errors are warned, not fatal — a transient OBS WebSocket
// failure shouldn't kill the daemon.
func (d *daemon) handleEvent(ctx context.Context, ev unix.InotifyEvent) {
	transition := d.tracker.Apply(ev.Mask)
	switch transition {
	case TransitionActivate:
		d.logger.InfoContext(ctx, "activate",
			"kind", describeMask(ev.Mask),
			"ref_count", d.tracker.RefCount())
		// Order: open the device BEFORE switching to the Active
		// scene, so that when the scene becomes visible the source
		// already holds a live capture. If we switched scene first,
		// the user would see a black frame for ~1 RPC round-trip.
		if err := d.switcher.SetCameraDevice(ctx, d.cameraSource, d.cameraDevice); err != nil {
			d.logger.WarnContext(ctx, "open camera failed",
				"source", d.cameraSource, "err", err)
		}
		if err := d.switcher.SetScene(ctx, d.sceneActive); err != nil {
			d.logger.WarnContext(ctx, "set scene failed",
				"scene", d.sceneActive, "err", err)
		}
		if err := d.publisher.Publish(StateActive, d.tracker.RefCount(), time.Now()); err != nil {
			d.logger.WarnContext(ctx, "publish failed", "state", StateActive, "err", err)
		}
	case TransitionDeactivate:
		d.logger.InfoContext(ctx, "deactivate",
			"kind", describeMask(ev.Mask),
			"ref_count", d.tracker.RefCount())
		// Order: switch to Standby BEFORE closing the device, so OBS
		// hides the (still-live) source before its fd is torn down.
		// Reversed order would briefly show the source in an error
		// state until the scene flip lands.
		if err := d.switcher.SetScene(ctx, d.sceneStandby); err != nil {
			d.logger.WarnContext(ctx, "set scene failed",
				"scene", d.sceneStandby, "err", err)
		}
		if err := d.switcher.SetCameraDevice(ctx, d.cameraSource, ""); err != nil {
			d.logger.WarnContext(ctx, "close camera failed",
				"source", d.cameraSource, "err", err)
		}
		if err := d.publisher.Publish(StateIdle, d.tracker.RefCount(), time.Now()); err != nil {
			d.logger.WarnContext(ctx, "publish failed", "state", StateIdle, "err", err)
		}
	case TransitionNone:
		d.logger.DebugContext(ctx, "event",
			"kind", describeMask(ev.Mask),
			"mask", fmt.Sprintf("0x%x", ev.Mask),
			"ref_count", d.tracker.RefCount())
	}
}

// readLoop pulls inotify event records off fd. Each read may yield zero
// or more InotifyEvent structs back-to-back; we walk the buffer using
// the documented header size + Len trailer.
//
// Shutdown: `unix.Read` is uninterruptible-by-cancel, so we rely on the
// caller closing fd to make Read return EBADF and unwedge the loop.
// We additionally check ctx.Err() each iteration so that — if a future
// caller forgets to close the fd promptly — we still notice cancellation
// and exit cleanly rather than spin on the closed-but-still-readable fd.
func readLoop(ctx context.Context, fd int, events chan<- unix.InotifyEvent, errCh chan<- error) {
	defer close(events)
	buf := make([]byte, 4096)
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := unix.Read(fd, buf)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if ctx.Err() != nil {
				// Expected: caller closed the fd as part of shutdown.
				return
			}
			errCh <- err
			return
		}
		offset := 0
		for offset+unix.SizeofInotifyEvent <= n {
			// Standard inotify event parsing: the kernel writes a sequence
			// of InotifyEvent headers (optionally followed by Len bytes of
			// name trailer) into the fd's read buffer. golang.org/x/sys/unix
			// exposes the struct layout; pointer arithmetic over the byte
			// buffer is the idiomatic way to walk records — see inotify(7).
			//nolint:gosec // documented kernel ABI; struct layout fixed
			raw := (*unix.InotifyEvent)(unsafe.Pointer(&buf[offset]))
			events <- *raw
			offset += unix.SizeofInotifyEvent + int(raw.Len)
		}
	}
}

// describeMask turns an inotify event mask into a short token. We only
// expect one of IN_OPEN / IN_CLOSE_* per event, but a defensive default
// catches anything unexpected during bring-up.
func describeMask(mask uint32) string {
	switch {
	case mask&unix.IN_OPEN != 0:
		return "IN_OPEN"
	case mask&unix.IN_CLOSE_NOWRITE != 0:
		return "IN_CLOSE_NOWRITE"
	case mask&unix.IN_CLOSE_WRITE != 0:
		return "IN_CLOSE_WRITE"
	default:
		return fmt.Sprintf("UNKNOWN(0x%x)", mask)
	}
}
