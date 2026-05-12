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
)

// recordingSwitcher implements the Switcher interface and remembers
// every call in order, for asserting the ordering invariants
// documented at daemon.reconcile (activate must open the camera BEFORE
// switching the scene; deactivate must switch the scene BEFORE closing
// the camera).
type recordingSwitcher struct {
	mu    sync.Mutex
	calls []string
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
