package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// recordingSwitcher implements the Switcher interface and remembers
// every call in order, for asserting the ordering invariants
// documented at daemon.reconcile (activate must open the camera BEFORE
// switching the scene; deactivate must switch the scene BEFORE closing
// the camera).
//
// The connected channel is exposed so eventLoop tests can simulate an
// OBS reconnect signal by calling emitConnected, which mirrors what the
// liveSwitcher's connectLoop does after each successful (re)connect.
type recordingSwitcher struct {
	mu        sync.Mutex
	calls     []string
	connected chan struct{}
}

func (r *recordingSwitcher) SetScene(_ context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fmt.Sprintf("SetScene(%s)", name))
	return nil
}

func (r *recordingSwitcher) SetCameraDevice(_ context.Context, sourceName, deviceID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fmt.Sprintf("SetCameraDevice(%s,%s)", sourceName, deviceID))
	return nil
}

func (r *recordingSwitcher) Connected() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.connected == nil {
		// Lazy-allocate so tests that don't exercise the reconnect
		// pathway don't need to pre-construct the channel.
		r.connected = make(chan struct{}, 1)
	}
	return r.connected
}

// emitConnected simulates a successful (re)connect by sending one value
// on the channel. Buffered cap 1 + non-blocking send mirrors how
// liveSwitcher.signalConnected coalesces multiple connects.
func (r *recordingSwitcher) emitConnected() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.connected == nil {
		r.connected = make(chan struct{}, 1)
	}
	select {
	case r.connected <- struct{}{}:
	default:
	}
}

func (r *recordingSwitcher) Close() error { return nil }

func (r *recordingSwitcher) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	return out
}

// nopPublisher swallows all calls — daemon_test cares about switcher
// call ordering, not socket events.
type nopPublisher struct{}

func (nopPublisher) Publish(_ State, _ int, _ time.Time) error { return nil }
func (nopPublisher) Close() error                              { return nil }

// writeConsumerProc creates a fake /proc/<pid> entry with one fd
// symlink pointing at testDevice and a comm matching the supplied
// string. Used to control ProcScanner.Count in tests that need to
// drive reconcile through a 0↔N transition.
func writeConsumerProc(t *testing.T, root, pid, comm string) {
	t.Helper()
	procDir := filepath.Join(root, pid)
	if err := os.MkdirAll(filepath.Join(procDir, "fd"), 0o755); err != nil {
		t.Fatalf("mkdir %s/fd: %v", procDir, err)
	}
	if err := os.WriteFile(filepath.Join(procDir, "comm"), []byte(comm+"\n"), 0o644); err != nil {
		t.Fatalf("write comm: %v", err)
	}
	if err := os.Symlink(testDevice, filepath.Join(procDir, "fd", "3")); err != nil {
		t.Fatalf("symlink: %v", err)
	}
}

// removeConsumerProc tears down the /proc/<pid> tree for the supplied
// pid, simulating the consumer process exiting.
func removeConsumerProc(t *testing.T, root, pid string) {
	t.Helper()
	if err := os.RemoveAll(filepath.Join(root, pid)); err != nil {
		t.Fatalf("remove %s: %v", pid, err)
	}
}

// TestDaemon_ActivateOpensCameraBeforeSceneFlip pins the documented
// activate-ordering invariant from main.go: on a 0→N transition the
// camera-device RPC must precede the scene-switch RPC, so the source
// already holds a live capture by the time the scene becomes visible.
// A future refactor of the Switcher interface could trivially swap the
// order; this test prevents that silent regression.
func TestDaemon_ActivateOpensCameraBeforeSceneFlip(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeConsumerProc(t, root, "100", "zoom")

	rec := &recordingSwitcher{}
	logger := slog.New(slog.DiscardHandler)
	scanner := NewProcScanner(testDevice, nil, logger)
	scanner.procRoot = root

	d := &daemon{
		logger:       logger,
		switcher:     rec,
		publisher:    nopPublisher{},
		scanner:      scanner,
		sceneActive:  "Active",
		sceneStandby: "Standby",
		cameraSource: "Real Webcam",
		cameraDevice: "/dev/video0",
	}
	d.reconcile(context.Background(), "test-activate")

	got := rec.snapshot()
	want := []string{
		"SetCameraDevice(Real Webcam,/dev/video0)",
		"SetScene(Active)",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("activate call order = %v, want %v", got, want)
	}
}

// TestDaemon_PushCurrentState_ActiveReissuesRPCs verifies that
// pushCurrentState re-emits the activate RPCs when the tracker thinks
// it's active (count > 0), independent of whether a 0↔N transition
// fired. This is the convergence primitive used when OBS reconnects
// after a disconnect: the tracker may still be in "active" state
// internally, but OBS has lost (or never received) the prior RPCs,
// so we need to re-push them to bring OBS back into agreement.
//
// Regression guard for the lazycam bug observed 2026-05-12: when OBS
// dies and is restarted, lazycam reconnects but does not re-fire the
// dropped activation, leaving OBS in Standby with device_id="" while
// consumers are actively reading frames.
func TestDaemon_PushCurrentState_ActiveReissuesRPCs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeConsumerProc(t, root, "100", "zoom")

	rec := &recordingSwitcher{}
	logger := slog.New(slog.DiscardHandler)
	scanner := NewProcScanner(testDevice, nil, logger)
	scanner.procRoot = root

	d := &daemon{
		logger:       logger,
		switcher:     rec,
		publisher:    nopPublisher{},
		scanner:      scanner,
		sceneActive:  "Active",
		sceneStandby: "Standby",
		cameraSource: "Real Webcam",
		cameraDevice: "/dev/video0",
	}
	// Drive to active state first so the tracker has count=1.
	d.reconcile(context.Background(), "test-prep")
	prefix := rec.snapshot()

	// Simulate OBS reconnect: push current state. Expect the same
	// activate RPCs to fire again in the same order.
	d.pushCurrentState(context.Background(), "test-obs-connected")

	got := rec.snapshot()[len(prefix):]
	want := []string{
		"SetCameraDevice(Real Webcam,/dev/video0)",
		"SetScene(Active)",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("pushCurrentState (active) call order = %v, want %v", got, want)
	}
}

// TestDaemon_PushCurrentState_IdleReissuesRPCs verifies the inverse:
// when the tracker thinks it's idle (count == 0), pushCurrentState
// re-emits the deactivate RPCs in standby-before-close order. Idempotent
// at OBS level — setting scene to Standby when already Standby is a
// no-op, and clearing device_id when already "" is a no-op.
func TestDaemon_PushCurrentState_IdleReissuesRPCs(t *testing.T) {
	t.Parallel()
	root := t.TempDir() // empty /proc — count is 0

	rec := &recordingSwitcher{}
	logger := slog.New(slog.DiscardHandler)
	scanner := NewProcScanner(testDevice, nil, logger)
	scanner.procRoot = root

	d := &daemon{
		logger:       logger,
		switcher:     rec,
		publisher:    nopPublisher{},
		scanner:      scanner,
		sceneActive:  "Active",
		sceneStandby: "Standby",
		cameraSource: "Real Webcam",
		cameraDevice: "/dev/video0",
	}
	// Tracker is at its zero value (count=0). No prior reconcile needed.
	d.pushCurrentState(context.Background(), "test-obs-connected")

	got := rec.snapshot()
	want := []string{
		"SetScene(Standby)",
		"SetCameraDevice(Real Webcam,)",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("pushCurrentState (idle) call order = %v, want %v", got, want)
	}
}

// TestDaemon_DeactivateSwitchesSceneBeforeClosingCamera pins the
// inverse ordering invariant: on N→0 the scene must flip to Standby
// BEFORE the camera device_id is cleared, so OBS hides the still-live
// source before its fd is torn down. Reversed, the user would briefly
// see the source in an error state until the scene flip lands.
func TestDaemon_DeactivateSwitchesSceneBeforeClosingCamera(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeConsumerProc(t, root, "200", "ffmpeg")

	rec := &recordingSwitcher{}
	logger := slog.New(slog.DiscardHandler)
	scanner := NewProcScanner(testDevice, nil, logger)
	scanner.procRoot = root

	d := &daemon{
		logger:       logger,
		switcher:     rec,
		publisher:    nopPublisher{},
		scanner:      scanner,
		sceneActive:  "Active",
		sceneStandby: "Standby",
		cameraSource: "Real Webcam",
		cameraDevice: "/dev/video0",
	}
	// Drive the daemon to the Active state first, then yank the
	// consumer to force a deactivate. The 4-call snapshot we capture
	// afterwards must split cleanly into activate-then-deactivate
	// ordering.
	d.reconcile(context.Background(), "test-activate")
	removeConsumerProc(t, root, "200")
	d.reconcile(context.Background(), "test-deactivate")

	got := rec.snapshot()
	want := []string{
		"SetCameraDevice(Real Webcam,/dev/video0)",
		"SetScene(Active)",
		"SetScene(Standby)",
		"SetCameraDevice(Real Webcam,)",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("activate+deactivate call order = %v, want %v", got, want)
	}
}

// TestDaemon_EventLoop_ConnectedTriggersPushCurrentState wires the
// regression: a signal on switcher.Connected() (mirroring a successful
// OBS reconnect) must cause the daemon's eventLoop to call
// pushCurrentState, re-issuing the activate RPCs against the fresh
// connection. This is the fix for the lazycam bug where a missed
// activation after OBS restart leaves OBS stuck in Standby.
func TestDaemon_EventLoop_ConnectedTriggersPushCurrentState(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeConsumerProc(t, root, "100", "zoom")

	rec := &recordingSwitcher{}
	logger := slog.New(slog.DiscardHandler)
	scanner := NewProcScanner(testDevice, nil, logger)
	scanner.procRoot = root

	d := &daemon{
		logger:       logger,
		switcher:     rec,
		publisher:    nopPublisher{},
		scanner:      scanner,
		sceneActive:  "Active",
		sceneStandby: "Standby",
		cameraSource: "Real Webcam",
		cameraDevice: "/dev/video0",
	}
	// Drive to active state. After this, tracker.Count() == 1 and the
	// recorder holds 2 calls (the initial activate sequence).
	d.reconcile(context.Background(), "test-prep")
	prefix := len(rec.snapshot())

	// Run eventLoop in a goroutine. Use empty channels for inotify so
	// only the connected case can fire.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events := make(chan unix.InotifyEvent)
	readErr := make(chan error, 1)
	done := make(chan error, 1)
	go func() { done <- d.eventLoop(ctx, events, readErr) }()

	// Simulate a successful (re)connect.
	rec.emitConnected()

	// Poll until the eventLoop has processed the signal and recorded
	// the push. A short polling deadline avoids leaving the goroutine
	// dangling if the wiring regresses.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(rec.snapshot()) > prefix {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("eventLoop: %v", err)
	}

	got := rec.snapshot()[prefix:]
	want := []string{
		"SetCameraDevice(Real Webcam,/dev/video0)",
		"SetScene(Active)",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("post-Connected RPCs = %v, want %v", got, want)
	}
}
