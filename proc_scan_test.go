package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testDevice is the v4l2 loopback path the test fakes target. All
// scanner tests use the same value because the production daemon only
// ever watches one device at a time.
const testDevice = "/dev/video10"

// fakeProcTree builds a /proc-shaped directory tree under root for a
// set of process descriptors. Each descriptor specifies the process
// comm and the symlink targets that each of its fds point to. Test
// helper — keeps the tests' own file ops out of the assertion path.
type fakeProc struct {
	pid     string
	comm    string
	fdLinks map[string]string // fdNum -> symlink target
}

func buildFakeProc(t *testing.T, root string, procs []fakeProc) {
	t.Helper()
	for _, p := range procs {
		procDir := filepath.Join(root, p.pid)
		if err := os.MkdirAll(filepath.Join(procDir, "fd"), 0o755); err != nil {
			t.Fatalf("mkdir %s/fd: %v", procDir, err)
		}
		if err := os.WriteFile(filepath.Join(procDir, "comm"), []byte(p.comm+"\n"), 0o644); err != nil {
			t.Fatalf("write comm for %s: %v", p.pid, err)
		}
		for fdNum, target := range p.fdLinks {
			if err := os.Symlink(target, filepath.Join(procDir, "fd", fdNum)); err != nil {
				t.Fatalf("symlink %s/fd/%s -> %s: %v", procDir, fdNum, target, err)
			}
		}
	}
	// Add a non-pid entry to confirm Count skips it cleanly.
	if err := os.WriteFile(filepath.Join(root, "cpuinfo"), []byte("model name\t: test\n"), 0o644); err != nil {
		t.Fatalf("write cpuinfo: %v", err)
	}
}

// newTestScanner constructs a ProcScanner pointed at a tmpdir fake-proc
// tree. We replace the production procRoot via direct struct field
// assignment — production callers go through NewProcScanner which
// hardcodes "/proc".
func newTestScanner(t *testing.T, exclude []string, root string) *ProcScanner {
	t.Helper()
	s := NewProcScanner(testDevice, exclude, slog.New(slog.DiscardHandler))
	s.procRoot = root
	return s
}

func TestProcScanner_NoOpenersCountsZero(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	// One process with one fd to an unrelated file.
	buildFakeProc(t, root, []fakeProc{
		{pid: "100", comm: "bash", fdLinks: map[string]string{"0": "/dev/null"}},
	})
	s := newTestScanner(t, nil, root)
	got, err := s.Count(context.Background())
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 0 {
		t.Errorf("Count = %d, want 0", got)
	}
}

func TestProcScanner_SingleConsumerCountsOne(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	buildFakeProc(t, root, []fakeProc{
		{pid: "200", comm: "zoom", fdLinks: map[string]string{"5": "/dev/video10"}},
	})
	s := newTestScanner(t, nil, root)
	got, err := s.Count(context.Background())
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 1 {
		t.Errorf("Count = %d, want 1", got)
	}
}

func TestProcScanner_ExcludedProducerIgnored(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	buildFakeProc(t, root, []fakeProc{
		{pid: "300", comm: ".obs-wrapped", fdLinks: map[string]string{"7": "/dev/video10"}},
	})
	s := newTestScanner(t, []string{".obs-wrapped"}, root)
	got, err := s.Count(context.Background())
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 0 {
		t.Errorf("Count = %d, want 0 (producer excluded)", got)
	}
}

func TestProcScanner_ProducerPlusConsumerCountsOne(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	buildFakeProc(t, root, []fakeProc{
		{pid: "400", comm: ".obs-wrapped", fdLinks: map[string]string{"7": "/dev/video10"}},
		{pid: "500", comm: "zoom", fdLinks: map[string]string{"8": "/dev/video10"}},
	})
	s := newTestScanner(t, []string{".obs-wrapped"}, root)
	got, err := s.Count(context.Background())
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 1 {
		t.Errorf("Count = %d, want 1 (producer excluded, consumer counted)", got)
	}
}

func TestProcScanner_OneProcessMultipleFdsCountsOnce(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	// A consumer holding two fds against the same device — should
	// still count as one consumer process.
	buildFakeProc(t, root, []fakeProc{
		{pid: "600", comm: "ffmpeg", fdLinks: map[string]string{
			"3": "/dev/video10",
			"4": "/dev/video10",
		}},
	})
	s := newTestScanner(t, nil, root)
	got, err := s.Count(context.Background())
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 1 {
		t.Errorf("Count = %d, want 1 (per-process semantic)", got)
	}
}

func TestProcScanner_UnrelatedDeviceIgnored(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	buildFakeProc(t, root, []fakeProc{
		{pid: "700", comm: "zoom", fdLinks: map[string]string{"5": "/dev/video0"}},
	})
	s := newTestScanner(t, nil, root)
	got, err := s.Count(context.Background())
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 0 {
		t.Errorf("Count = %d, want 0 (different device)", got)
	}
}

func TestProcScanner_NonPidEntriesSkipped(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	// Just the cpuinfo file added by buildFakeProc — no actual processes.
	buildFakeProc(t, root, nil)
	s := newTestScanner(t, nil, root)
	got, err := s.Count(context.Background())
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got != 0 {
		t.Errorf("Count = %d, want 0", got)
	}
}

func TestProcScanner_MissingProcRootErrors(t *testing.T) {
	t.Parallel()
	s := newTestScanner(t, nil, "/nonexistent/proc/path/zzz")
	if _, err := s.Count(context.Background()); err == nil {
		t.Error("Count() with missing procRoot returned nil; want error")
	} else if !strings.Contains(err.Error(), "read") {
		t.Errorf("Count() error = %q, want substring 'read'", err.Error())
	}
}
