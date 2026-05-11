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

	// Connect, then close from the client side without reading.
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// Give the publisher a moment to register us + write the snapshot
	// (which will succeed; the kernel buffer absorbs it). Then close.
	time.Sleep(50 * time.Millisecond)
	_ = conn.Close()

	// Now Publish multiple events. None should hang or return errors;
	// the publisher should detect the broken pipe and drop us.
	for i := range 3 {
		if err := pub.Publish(StateActive, i, time.Now()); err != nil {
			t.Errorf("Publish[%d] after subscriber death: %v", i, err)
		}
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
