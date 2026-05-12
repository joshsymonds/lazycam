// /proc inspection — distinguishes consumer opens from producer opens.
//
// inotify IN_OPEN tells us /dev/video10 was opened but not by whom or in
// what mode — every opener (Zoom reading frames, OBS writing them, kernel
// probes) fires the same event. To honor the LED privacy invariant
// ("camera lit iff something is reading the loopback") we need to count
// only the readers. The kernel exposes no consumer-count attribute on
// the v4l2loopback device, so we walk /proc and inspect each process's
// open file descriptors, matching by symlink target against the device
// path and filtering out producers by process comm.
//
// Producer-vs-consumer is identified by comm string rather than by open
// mode (O_RDONLY/O_WRONLY/O_RDWR) because v4l2 apps typically open the
// device O_RDWR regardless of intent — the mode bits don't tell us
// reliably. comm is set by the kernel from the process's argv[0]
// basename (truncated to 15 chars by TASK_COMM_LEN); for OBS Studio on
// this nix-wrapped install it's `.obs-wrapped` exactly.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ProcScanner counts processes currently holding a given device path
// open, excluding any whose comm matches the configured exclusion set.
type ProcScanner struct {
	// device is the absolute device path to match against /proc/<pid>/fd
	// symlink targets, e.g. "/dev/video10".
	device string

	// excludeComms is the set of process comm strings whose opens are
	// considered "producer" opens and do not count toward the consumer
	// ref-count. Typically the OBS wrapper's comm.
	excludeComms map[string]struct{}

	// procRoot is the filesystem root for /proc — overridable for tests
	// (point it at a tmpdir tree mimicking /proc structure). Production
	// callers use NewProcScanner, which sets it to "/proc".
	procRoot string

	logger *slog.Logger
}

// NewProcScanner builds a scanner watching device, ignoring openers
// whose comm string is in excludeComms. The exclusion check is exact-
// match on the process's /proc/<pid>/comm value.
func NewProcScanner(device string, excludeComms []string, logger *slog.Logger) *ProcScanner {
	excl := make(map[string]struct{}, len(excludeComms))
	for _, c := range excludeComms {
		if c != "" {
			excl[c] = struct{}{}
		}
	}
	return &ProcScanner{
		device:       device,
		excludeComms: excl,
		procRoot:     "/proc",
		logger:       logger,
	}
}

// Count returns the number of distinct processes currently holding the
// device open, after filtering out producers. A process holding multiple
// fds on the device counts as one — the consumer-or-not semantic is
// per-process, not per-fd.
//
// Errors reading individual /proc entries (process exited mid-scan,
// permission denied for another user's process) are swallowed; the
// returned count reflects what we could observe. Only a failure to read
// /proc itself surfaces as an error.
func (s *ProcScanner) Count(ctx context.Context) (int, error) {
	procEntries, err := os.ReadDir(s.procRoot)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", s.procRoot, err)
	}
	n := 0
	for _, ent := range procEntries {
		if !ent.IsDir() {
			continue
		}
		// /proc contains many non-pid entries (cpuinfo, meminfo,
		// kallsyms, etc.); only numeric names are processes.
		if _, atoiErr := strconv.Atoi(ent.Name()); atoiErr != nil {
			continue
		}
		if s.processHolds(ctx, ent.Name()) {
			n++
		}
	}
	return n, nil
}

// processHolds returns true if the process at /proc/<pid> currently
// has at least one fd open against s.device AND its comm is not in the
// exclusion set. The order of checks (fd walk first, comm read second)
// matters: skipping the comm read for processes that don't hold the
// device avoids reading hundreds of /proc/<pid>/comm files on every
// scan in a typical workstation with 400+ processes.
func (s *ProcScanner) processHolds(ctx context.Context, pid string) bool {
	fdDir := filepath.Join(s.procRoot, pid, "fd")
	fds, err := os.ReadDir(fdDir)
	if err != nil {
		// Process disappeared, or we lack permission. Both are
		// expected — skip silently. (Logging here would spam the
		// journal with every scan: most processes belong to other
		// users or to systemd in restricted modes.)
		return false
	}
	matched := false
	for _, fdEnt := range fds {
		target, readErr := os.Readlink(filepath.Join(fdDir, fdEnt.Name()))
		if readErr != nil {
			continue
		}
		if target == s.device {
			matched = true
			break
		}
	}
	if !matched {
		return false
	}
	commPath := filepath.Join(s.procRoot, pid, "comm")
	// commPath is built from a validated numeric pid plus the literal
	// "comm" suffix under procRoot. There's no user-attacker-controlled
	// component — gosec's G304 heuristic flags variable paths but the
	// inputs here are bounded by the /proc walk above.
	//nolint:gosec // path is a /proc/<numeric-pid>/comm under procRoot
	commBytes, err := os.ReadFile(commPath)
	if err != nil {
		// Could not read comm (process gone between fd-read and
		// comm-read). Be conservative: count as a consumer. Better
		// to mistakenly leave the LED on than to miss a real one.
		s.logger.DebugContext(ctx, "comm read failed; counting as consumer",
			"pid", pid, "err", err)
		return true
	}
	comm := strings.TrimRight(string(commBytes), "\n")
	if _, excluded := s.excludeComms[comm]; excluded {
		return false
	}
	return true
}
