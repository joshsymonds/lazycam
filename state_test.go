package main

import "testing"

func TestTracker_ZeroToOneActivates(t *testing.T) {
	t.Parallel()
	var tr Tracker
	if got := tr.Update(1); got != TransitionActivate {
		t.Errorf("Update(1) from zero = %v, want Activate", got)
	}
	if got := tr.Count(); got != 1 {
		t.Errorf("Count = %d, want 1", got)
	}
}

func TestTracker_OneToZeroDeactivates(t *testing.T) {
	t.Parallel()
	var tr Tracker
	_ = tr.Update(1)
	if got := tr.Update(0); got != TransitionDeactivate {
		t.Errorf("Update(0) from one = %v, want Deactivate", got)
	}
	if got := tr.Count(); got != 0 {
		t.Errorf("Count = %d, want 0", got)
	}
}

func TestTracker_OneToTwoIsNone(t *testing.T) {
	t.Parallel()
	var tr Tracker
	_ = tr.Update(1)
	if got := tr.Update(2); got != TransitionNone {
		t.Errorf("Update(2) from one = %v, want None (still active)", got)
	}
	if got := tr.Count(); got != 2 {
		t.Errorf("Count = %d, want 2", got)
	}
}

func TestTracker_TwoToOneIsNone(t *testing.T) {
	t.Parallel()
	var tr Tracker
	_ = tr.Update(2)
	if got := tr.Update(1); got != TransitionNone {
		t.Errorf("Update(1) from two = %v, want None (still active)", got)
	}
	if got := tr.Count(); got != 1 {
		t.Errorf("Count = %d, want 1", got)
	}
}

func TestTracker_ZeroToZeroIsNone(t *testing.T) {
	t.Parallel()
	var tr Tracker
	if got := tr.Update(0); got != TransitionNone {
		t.Errorf("Update(0) on fresh tracker = %v, want None", got)
	}
}

func TestTracker_NegativeCountClampsAtZero(t *testing.T) {
	t.Parallel()
	var tr Tracker
	if got := tr.Update(-3); got != TransitionNone {
		t.Errorf("Update(-3) = %v, want None (clamped)", got)
	}
	if got := tr.Count(); got != 0 {
		t.Errorf("Count = %d, want 0 (clamped)", got)
	}
}

func TestTracker_DropFromTwoToZeroDeactivates(t *testing.T) {
	t.Parallel()
	var tr Tracker
	_ = tr.Update(2)
	if got := tr.Update(0); got != TransitionDeactivate {
		t.Errorf("Update(0) from two = %v, want Deactivate", got)
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
