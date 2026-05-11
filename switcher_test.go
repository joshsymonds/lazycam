package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// captureLogger returns a *slog.Logger that writes JSON lines into the
// returned buffer. JSON output is greppable in assertions.
func captureLogger(t *testing.T) (*slog.Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	return logger, buf
}

func TestDryRunSwitcher_LogsWithoutNetwork(t *testing.T) {
	t.Parallel()
	logger, buf := captureLogger(t)
	sw, err := newSwitcher(logger, switcherOptions{dryRun: true})
	if err != nil {
		t.Fatalf("newSwitcher: %v", err)
	}
	t.Cleanup(func() {
		if cerr := sw.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	})

	if err := sw.SetScene(context.Background(), "Active"); err != nil {
		t.Fatalf("SetScene: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, `"msg":"would set scene"`) {
		t.Errorf("expected 'would set scene' message; got: %s", out)
	}
	if !strings.Contains(out, `"scene":"Active"`) {
		t.Errorf("expected scene=Active field; got: %s", out)
	}
}

func TestDryRunSwitcher_CloseIsNoop(t *testing.T) {
	t.Parallel()
	logger, _ := captureLogger(t)
	sw, err := newSwitcher(logger, switcherOptions{dryRun: true})
	if err != nil {
		t.Fatalf("newSwitcher: %v", err)
	}
	if err := sw.Close(); err != nil {
		t.Errorf("Close (first call): %v", err)
	}
	if err := sw.Close(); err != nil {
		t.Errorf("Close (second call): %v", err)
	}
}

// TestNewSwitcher_LiveModeNotYetImplemented locks the contract for the
// pre-task-#5 state: requesting live mode (dryRun=false) returns the
// sentinel `errLiveNotImplemented`. DELETE this test in task #5 when
// the live switcher actually exists.
func TestNewSwitcher_LiveModeNotYetImplemented(t *testing.T) {
	t.Parallel()
	logger, _ := captureLogger(t)
	_, err := newSwitcher(logger, switcherOptions{dryRun: false})
	if err == nil {
		t.Fatal("expected error for live mode, got nil")
	}
	if !errors.Is(err, errLiveNotImplemented) {
		t.Errorf("expected errLiveNotImplemented, got: %v", err)
	}
}
