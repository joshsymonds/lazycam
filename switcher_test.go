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
		dryRun:     false,
		obsURL:     "ws://127.0.0.1:1",
		maxBackoff: 30 * time.Second,
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

func TestExtractHost(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "ws scheme", input: "ws://127.0.0.1:4455", want: "127.0.0.1:4455"},
		{name: "wss scheme", input: "wss://127.0.0.1:4455", want: "127.0.0.1:4455"},
		{name: "bare host port", input: "127.0.0.1:4455", want: "127.0.0.1:4455"},
		{name: "localhost", input: "ws://localhost:4455", want: "localhost:4455"},
		// IPv6 bracket form: net/url returns Host with the brackets
		// preserved (e.g. "[::1]:4455"), which is what net.Dial expects
		// for a bracketed-host:port. Pin the bracket-preservation so a
		// future refactor of extractHost doesn't accidentally strip them.
		{name: "ws scheme ipv6", input: "ws://[::1]:4455", want: "[::1]:4455"},
		{name: "wss scheme ipv6", input: "wss://[::1]:4455", want: "[::1]:4455"},
		{name: "empty", input: "", wantErr: true},
		{name: "no port", input: "ws://obs", wantErr: true},
		{name: "no port bare", input: "obs", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := extractHost(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("extractHost(%q) = %q, want error", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Errorf("extractHost(%q) returned error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("extractHost(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestRequireLoopback(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		hostPort string
		wantErr  bool
	}{
		{name: "v4 loopback", hostPort: "127.0.0.1:4455", wantErr: false},
		{name: "v4 loopback other octet", hostPort: "127.0.0.42:4455", wantErr: false},
		{name: "v6 loopback", hostPort: "[::1]:4455", wantErr: false},
		{name: "localhost", hostPort: "localhost:4455", wantErr: false},
		{name: "public v4", hostPort: "8.8.8.8:4455", wantErr: true},
		{name: "private v4", hostPort: "192.168.1.1:4455", wantErr: true},
		{name: "hostname non-localhost", hostPort: "obs.example.com:4455", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := requireLoopback(tc.hostPort)
			if tc.wantErr && err == nil {
				t.Errorf("requireLoopback(%q): want error, got nil", tc.hostPort)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("requireLoopback(%q): %v", tc.hostPort, err)
			}
		})
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
		dryRun:     false,
		obsURL:     "ws://127.0.0.1:1",
		maxBackoff: 30 * time.Second,
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
