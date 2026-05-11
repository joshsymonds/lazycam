// Command lazycam — on-demand v4l2loopback producer gating.
//
// This first iteration is a kernel-event smoke test: it watches a v4l2
// device (default /dev/video10) with inotify and logs every open/close.
// Later iterations add a consumer ref-count, OBS WebSocket scene-switch
// RPCs, and a UNIX socket state publisher for the DMS indicator widget.
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
	"unsafe"

	"golang.org/x/sys/unix"
)

func main() {
	device := flag.String("device", "/dev/video10",
		"v4l2 device path to watch for opens/closes")
	debug := flag.Bool("debug", false,
		"log every inotify event (otherwise only 0↔N transitions are logged)")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	}))

	if err := run(logger, *device); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, device string) error {
	if _, err := os.Stat(device); err != nil {
		return fmt.Errorf("device %q not accessible: %w", device, err)
	}

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
	wd, err := unix.InotifyAddWatch(fd, device, mask)
	if err != nil {
		return fmt.Errorf("inotify_add_watch %q: %w", device, err)
	}
	defer func() {
		// InotifyAddWatch returns a non-negative int on success, so the
		// uint32 conversion is safe; gosec can't infer the invariant.
		//nolint:gosec // wd is non-negative on success per inotify(7)
		if _, rerr := unix.InotifyRmWatch(fd, uint32(wd)); rerr != nil {
			logger.Warn("inotify rm_watch failed", "err", rerr)
		}
	}()

	logger.Info("watching", "device", device)

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	// inotify reads block; do them on a goroutine and surface results on
	// channels so the main loop can select on ctx for shutdown.
	events := make(chan unix.InotifyEvent, 16)
	readErr := make(chan error, 1)
	go readLoop(fd, events, readErr)

	var tracker Tracker
	for {
		select {
		case <-ctx.Done():
			logger.Info("shutting down")
			return nil
		case ev := <-events:
			handleEvent(logger, &tracker, ev)
		case err := <-readErr:
			return fmt.Errorf("inotify read: %w", err)
		}
	}
}

// handleEvent folds one inotify event into the tracker and logs at the
// right granularity. Activate / Deactivate transitions always log at
// INFO so an operator running without --debug still sees the events
// that actually change the daemon's effect on the world. None-events
// (e.g. a second concurrent open) log at DEBUG only.
func handleEvent(logger *slog.Logger, tracker *Tracker, ev unix.InotifyEvent) {
	transition := tracker.Apply(ev.Mask)
	switch transition {
	case TransitionActivate:
		logger.Info("activate",
			"kind", describeMask(ev.Mask),
			"ref_count", tracker.RefCount())
	case TransitionDeactivate:
		logger.Info("deactivate",
			"kind", describeMask(ev.Mask),
			"ref_count", tracker.RefCount())
	case TransitionNone:
		logger.Debug("event",
			"kind", describeMask(ev.Mask),
			"mask", fmt.Sprintf("0x%x", ev.Mask),
			"ref_count", tracker.RefCount())
	}
}

// readLoop pulls inotify event records off fd. Each read may yield zero
// or more InotifyEvent structs back-to-back; we walk the buffer using
// the documented header size + Len trailer.
func readLoop(fd int, events chan<- unix.InotifyEvent, errCh chan<- error) {
	defer close(events)
	buf := make([]byte, 4096)
	for {
		n, err := unix.Read(fd, buf)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
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
