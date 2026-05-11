package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testSocketPath returns a per-test UNIX socket path short enough to fit
// sun_path (108 bytes on Linux). Using /tmp keeps us comfortably under.
func testSocketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "p.sock")
}

// dialAndReadLine connects to the publisher's socket and reads one line.
// Returns the parsed event so each test can assert on shape, not strings.
func dialAndReadLine(t *testing.T, path string) publishedEvent {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial %q: %v", path, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read line: %v", err)
	}
	var ev publishedEvent
	if err := json.Unmarshal(line, &ev); err != nil {
		t.Fatalf("unmarshal %q: %v", line, err)
	}
	return ev
}

func TestUnixSocketPublisher_NewSubscriberGetsSnapshot(t *testing.T) {
	t.Parallel()
	logger, _ := captureLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := testSocketPath(t)
	pub, err := newPublisher(ctx, logger, path)
	if err != nil {
		t.Fatalf("newPublisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	// Set a non-default state so we can tell snapshot from default.
	if err := pub.Publish(StateActive, 2, time.Unix(1700000000, 0).UTC()); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ev := dialAndReadLine(t, path)
	if ev.State != StateActive {
		t.Errorf("snapshot State = %q, want %q", ev.State, StateActive)
	}
	if ev.RefCount != 2 {
		t.Errorf("snapshot RefCount = %d, want 2", ev.RefCount)
	}
}

func TestUnixSocketPublisher_PublishBroadcasts(t *testing.T) {
	t.Parallel()
	logger, _ := captureLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := testSocketPath(t)
	pub, err := newPublisher(ctx, logger, path)
	if err != nil {
		t.Fatalf("newPublisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	// Two subscribers connect. Each immediately gets the snapshot
	// (default StateIdle).
	const numSubscribers = 2
	type sub struct {
		conn   net.Conn
		reader *bufio.Reader
	}
	subs := make([]sub, numSubscribers)
	for i := range subs {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("dial[%d]: %v", i, err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		reader := bufio.NewReader(conn)
		// Consume the snapshot line so the next read is the broadcast.
		if _, err := reader.ReadBytes('\n'); err != nil {
			t.Fatalf("snapshot read[%d]: %v", i, err)
		}
		subs[i] = sub{conn: conn, reader: reader}
	}

	// Broadcast Activate.
	now := time.Now().UTC()
	if err := pub.Publish(StateActive, 1, now); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for i, s := range subs {
		line, err := s.reader.ReadBytes('\n')
		if err != nil {
			t.Errorf("subscriber[%d] read: %v", i, err)
			continue
		}
		var ev publishedEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			t.Errorf("subscriber[%d] unmarshal: %v", i, err)
			continue
		}
		if ev.State != StateActive || ev.RefCount != 1 {
			t.Errorf("subscriber[%d] got %+v, want State=active RefCount=1", i, ev)
		}
	}
}

func TestUnixSocketPublisher_DropsDeadSubscribers(t *testing.T) {
	t.Parallel()
	logger, _ := captureLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := testSocketPath(t)
	pub, err := newPublisher(ctx, logger, path)
	if err != nil {
		t.Fatalf("newPublisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// Wait until the publisher has registered us. Polling
	// subscriberCount avoids racing on a fixed sleep.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && pub.subscriberCount() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := pub.subscriberCount(); got != 1 {
		t.Fatalf("subscriberCount after dial = %d, want 1", got)
	}

	// Close from the client side. The publisher's watchConn goroutine
	// should observe the EOF and drop us; we then verify by polling.
	_ = conn.Close()
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) && pub.subscriberCount() != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := pub.subscriberCount(); got != 0 {
		t.Errorf("subscriberCount after client close = %d, want 0", got)
	}

	// Subsequent Publishes must not hang or error even though our
	// subscriber is gone.
	for i := range 3 {
		if err := pub.Publish(StateActive, i, time.Now()); err != nil {
			t.Errorf("Publish[%d] after subscriber death: %v", i, err)
		}
	}
}

// TestUnixSocketPublisher_OrderingMonotonic locks the ordering
// invariant: a subscriber's reads are monotonic in publish timestamp.
// Without the snapshot-write-under-lock fix, a Publish racing with
// registerSubscriber could deliver its event to the new conn before
// the snapshot landed — and the subscriber would observe state in
// reverse-chronological order, which is the bug the indicator widget
// would manifest as a stuck stale display.
//
// We assert the weaker but provable invariant: across many concurrent
// dial+publish races, no subscriber ever observes a line whose
// timestamp predates a line it read earlier on the same connection.
func TestUnixSocketPublisher_OrderingMonotonic(t *testing.T) {
	t.Parallel()
	logger, _ := captureLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := testSocketPath(t)
	pub, err := newPublisher(ctx, logger, path)
	if err != nil {
		t.Fatalf("newPublisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	const trials = 30
	for trial := range trials {
		conn, derr := net.Dial("unix", path)
		if derr != nil {
			t.Fatalf("trial %d: dial: %v", trial, derr)
		}

		// Race a Publish against the dial+register sequence. The
		// publish's timestamp is necessarily later than the snapshot's
		// (the snapshot's was set by the prior trial's last publish,
		// or by newPublisher's lastState init).
		broadcastDone := make(chan struct{})
		go func() {
			_ = pub.Publish(StateActive, trial, time.Now())
			close(broadcastDone)
		}()

		if rerr := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); rerr != nil {
			t.Fatalf("trial %d: SetReadDeadline: %v", trial, rerr)
		}
		reader := bufio.NewReader(conn)

		// Read 1-2 lines. The first is the snapshot; the second (if
		// present) is either the racing broadcast (registerSubscriber
		// won) or a subsequent publish. We require: ts[i+1] >= ts[i].
		var prevTS time.Time
		for i := range 2 {
			line, rerr := reader.ReadBytes('\n')
			if rerr != nil {
				break // Second line may legitimately time out.
			}
			var ev publishedEvent
			if uerr := json.Unmarshal(line, &ev); uerr != nil {
				t.Fatalf("trial %d line %d: unmarshal: %v", trial, i, uerr)
			}
			if i > 0 && ev.Timestamp.Before(prevTS) {
				t.Errorf("trial %d: line %d timestamp %v precedes previous %v — reverse ordering",
					trial, i, ev.Timestamp, prevTS)
			}
			prevTS = ev.Timestamp
		}
		<-broadcastDone
		_ = conn.Close()
	}
}

func TestUnixSocketPublisher_CloseRemovesSocketFile(t *testing.T) {
	t.Parallel()
	logger, _ := captureLogger(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := testSocketPath(t)
	pub, err := newPublisher(ctx, logger, path)
	if err != nil {
		t.Fatalf("newPublisher: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("socket file missing post-create: %v", err)
	}

	if err := pub.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("socket file still present post-Close: stat err = %v", err)
	}

	// Idempotent: second Close is a no-op, no error.
	if err := pub.Close(); err != nil {
		t.Errorf("Close (second call): %v", err)
	}
}
