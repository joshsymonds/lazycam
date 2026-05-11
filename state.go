package main

import "golang.org/x/sys/unix"

// Transition reports a change to the daemon's consumer-attached state
// following an inotify event. None means the event did not move us
// across the 0↔N boundary; Activate means the first consumer just
// attached; Deactivate means the last consumer just released.
type Transition int

// Transition variants emitted by Tracker.Apply.
const (
	TransitionNone Transition = iota
	TransitionActivate
	TransitionDeactivate
)

// String returns a short lower-case label suitable for log fields.
func (tr Transition) String() string {
	switch tr {
	case TransitionNone:
		return "none"
	case TransitionActivate:
		return "activate"
	case TransitionDeactivate:
		return "deactivate"
	default:
		return "unknown"
	}
}

// Tracker maintains an in-process consumer ref-count for the watched
// v4l2 device. Apply folds one inotify event at a time and reports
// any 0↔N transition that resulted.
//
// Tracker is intentionally thread-unsafe: callers serialize through a
// single goroutine reading the inotify event channel. Wrap with a
// mutex if you ever fan in from multiple producers.
type Tracker struct {
	refCount int
}

// Apply folds one inotify event mask into the ref-count. Unrecognized
// masks (anything outside IN_OPEN | IN_CLOSE_*) are ignored. Closes
// against a zero ref-count clamp at 0 — defensive against any inotify
// desync at startup (we may not see the initial open if a consumer
// already had the device open before the daemon attached its watch).
func (t *Tracker) Apply(mask uint32) Transition {
	switch {
	case mask&unix.IN_OPEN != 0:
		t.refCount++
		if t.refCount == 1 {
			return TransitionActivate
		}
		return TransitionNone

	case mask&(unix.IN_CLOSE_NOWRITE|unix.IN_CLOSE_WRITE) != 0:
		if t.refCount == 0 {
			return TransitionNone
		}
		t.refCount--
		if t.refCount == 0 {
			return TransitionDeactivate
		}
		return TransitionNone

	default:
		return TransitionNone
	}
}

// RefCount returns the current consumer ref-count. Diagnostic accessor;
// used by both tests and the daemon's structured log fields.
func (t *Tracker) RefCount() int {
	return t.refCount
}
