package main

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestTracker_EmptyHasZeroRefCount(t *testing.T) {
	t.Parallel()
	var tr Tracker
	if got := tr.RefCount(); got != 0 {
		t.Errorf("RefCount = %d, want 0", got)
	}
}

func TestTracker_SingleOpenActivates(t *testing.T) {
	t.Parallel()
	var tr Tracker
	if got := tr.Apply(unix.IN_OPEN); got != TransitionActivate {
		t.Errorf("Apply(IN_OPEN) = %v, want Activate", got)
	}
	if got := tr.RefCount(); got != 1 {
		t.Errorf("RefCount = %d, want 1", got)
	}
}

func TestTracker_OpenThenCloseDeactivates(t *testing.T) {
	t.Parallel()
	var tr Tracker
	if got := tr.Apply(unix.IN_OPEN); got != TransitionActivate {
		t.Errorf("first Apply = %v, want Activate", got)
	}
	if got := tr.Apply(unix.IN_CLOSE_NOWRITE); got != TransitionDeactivate {
		t.Errorf("second Apply = %v, want Deactivate", got)
	}
	if got := tr.RefCount(); got != 0 {
		t.Errorf("RefCount = %d, want 0", got)
	}
}

func TestTracker_TwoOpensThenTwoCloses(t *testing.T) {
	t.Parallel()
	var tr Tracker
	type step struct {
		mask uint32
		want Transition
	}
	seq := []step{
		{unix.IN_OPEN, TransitionActivate},
		{unix.IN_OPEN, TransitionNone},
		{unix.IN_CLOSE_NOWRITE, TransitionNone},
		{unix.IN_CLOSE_NOWRITE, TransitionDeactivate},
	}
	for i, s := range seq {
		if got := tr.Apply(s.mask); got != s.want {
			t.Errorf("step %d: Apply(%#x) = %v, want %v", i, s.mask, got, s.want)
		}
	}
	if got := tr.RefCount(); got != 0 {
		t.Errorf("RefCount = %d, want 0", got)
	}
}

func TestTracker_UnbalancedCloseClampsAtZero(t *testing.T) {
	t.Parallel()
	var tr Tracker
	if got := tr.Apply(unix.IN_CLOSE_NOWRITE); got != TransitionNone {
		t.Errorf("Apply on empty = %v, want None", got)
	}
	if got := tr.RefCount(); got != 0 {
		t.Errorf("RefCount = %d, want 0 (clamped)", got)
	}
}

func TestTracker_MixedCloseVariantsBothDecrement(t *testing.T) {
	t.Parallel()
	var tr Tracker
	_ = tr.Apply(unix.IN_OPEN)
	_ = tr.Apply(unix.IN_OPEN)
	if got := tr.Apply(unix.IN_CLOSE_NOWRITE); got != TransitionNone {
		t.Errorf("first close (NOWRITE) = %v, want None", got)
	}
	if got := tr.Apply(unix.IN_CLOSE_WRITE); got != TransitionDeactivate {
		t.Errorf("second close (WRITE) = %v, want Deactivate", got)
	}
	if got := tr.RefCount(); got != 0 {
		t.Errorf("RefCount = %d, want 0", got)
	}
}

func TestTracker_UnknownMaskIgnored(t *testing.T) {
	t.Parallel()
	var tr Tracker
	_ = tr.Apply(unix.IN_OPEN)
	if got := tr.Apply(unix.IN_ACCESS); got != TransitionNone {
		t.Errorf("Apply(IN_ACCESS) = %v, want None", got)
	}
	if got := tr.RefCount(); got != 1 {
		t.Errorf("RefCount = %d, want 1 (unchanged)", got)
	}
}

func TestTransition_StringReadable(t *testing.T) {
	t.Parallel()
	cases := map[Transition]string{
		TransitionNone:       "none",
		TransitionActivate:   "activate",
		TransitionDeactivate: "deactivate",
		// Unknown enum values fall through to the default branch.
		// Using a numerically-out-of-range Transition exercises the
		// fallthrough that an exhaustive-switch refactor could miss.
		Transition(99): "unknown",
	}
	for tr, want := range cases {
		if got := tr.String(); got != want {
			t.Errorf("Transition(%d).String() = %q, want %q", tr, got, want)
		}
	}
}
