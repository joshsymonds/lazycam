package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
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
	ctx := context.Background()
	sw, err := newSwitcher(ctx, logger, switcherOptions{dryRun: true})
	if err != nil {
		t.Fatalf("newSwitcher: %v", err)
	}
	t.Cleanup(func() {
		if cerr := sw.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	})

	if err := sw.SetScene(ctx, "Active"); err != nil {
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
	ctx := context.Background()
	sw, err := newSwitcher(ctx, logger, switcherOptions{dryRun: true})
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

// TestLiveSwitcher_SetSceneBeforeConnectedDoesNotPanic guarantees that
// the daemon doesn't crash when OBS is unreachable at startup. The
// SetScene call is dropped with a WARN log; the connectLoop continues
// retrying in the background. This is the daemon's "OBS is down,
// don't take me down with it" invariant.
func TestLiveSwitcher_SetSceneBeforeConnectedDoesNotPanic(t *testing.T) {
	t.Parallel()
	logger, _ := captureLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Port 1 is the TCPMUX well-known port — nothing listens by default,
	// and connection failure is fast (ECONNREFUSED rather than timeout).
	sw, err := newSwitcher(ctx, logger, switcherOptions{
		dryRun: false,
		obsURL: "ws://127.0.0.1:1",
	})
	if err != nil {
		t.Fatalf("newSwitcher: %v", err)
	}
	t.Cleanup(func() {
		if cerr := sw.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	})

	// SetScene must return without panicking; nil err means call was
	// gracefully dropped, non-nil means a hard failure surfaced.
	done := make(chan error, 1)
	go func() { done <- sw.SetScene(ctx, "Active") }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("SetScene before connected: got err %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SetScene before connected: did not return within 1s")
	}
}

// TestLiveSwitcher_CloseBeforeConnect locks the invariant that Close
// is safe to call before the first connection attempt has succeeded.
// Without this, a daemon SIGTERM during OBS startup could hang.
func TestLiveSwitcher_CloseBeforeConnect(t *testing.T) {
	t.Parallel()
	logger, _ := captureLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sw, err := newSwitcher(ctx, logger, switcherOptions{
		dryRun: false,
		obsURL: "ws://127.0.0.1:1",
	})
	if err != nil {
		t.Fatalf("newSwitcher: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- sw.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close: did not return within 2s")
	}
}
