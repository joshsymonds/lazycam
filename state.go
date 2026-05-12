package main

// Transition reports a change to the daemon's consumer-attached state.
// None means the latest count did not move us across the 0↔N boundary;
// Activate means we transitioned from no consumers to at least one;
// Deactivate means the last consumer just released.
type Transition int

// Transition variants emitted by Tracker.Update.
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

// Tracker remembers the most-recent consumer count and reports any 0↔N
// transition when a new count is published. The count itself is sourced
// externally (ProcScanner) — Tracker is just the edge-detector that
// translates count snapshots into Activate/Deactivate signals.
//
// Tracker is intentionally thread-unsafe: callers serialize through a
// single goroutine reading the inotify event channel.
type Tracker struct {
	count int
}

// Update accepts a fresh consumer count and reports any 0↔N transition
// the new value caused. Negative counts are clamped to 0 — defensive
// against any caller arithmetic error; the public API contract is
// the current number of consumers, never negative.
func (t *Tracker) Update(count int) Transition {
	if count < 0 {
		count = 0
	}
	prev := t.count
	t.count = count
	switch {
	case prev == 0 && count > 0:
		return TransitionActivate
	case prev > 0 && count == 0:
		return TransitionDeactivate
	default:
		return TransitionNone
	}
}

// Count returns the most-recently-published consumer count. Diagnostic
// accessor; used by both tests and the daemon's structured log fields.
func (t *Tracker) Count() int {
	return t.count
}
