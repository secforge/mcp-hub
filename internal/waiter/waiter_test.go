package waiter

import (
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeSource struct {
	mu        sync.Mutex
	hasEvents bool
	connected bool
	formatted string
}

func (f *fakeSource) Peek() (bool, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hasEvents, f.connected
}

func (f *fakeSource) Drain() (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	formatted := f.formatted
	f.hasEvents = false
	f.formatted = ""
	return formatted, f.connected
}

func (f *fakeSource) push(text string) {
	f.mu.Lock()
	f.hasEvents = true
	f.formatted = text
	f.mu.Unlock()
}

func dialAndRead(t *testing.T, path string) string {
	t.Helper()
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(data)
}

func TestWaitDeliversAlreadyBufferedEvent(t *testing.T) {
	src := &fakeSource{connected: true}
	src.push("hello")
	w, err := Listen("session-a", "peer-a", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	got := dialAndRead(t, w.socketPath)
	if got != "hello\n\nRun this command again to keep receiving:\n"+w.WaitCommand()+"\n" {
		t.Fatalf("unexpected output: %q", got)
	}
}

func TestPokeDeliversToRegisteredWaiter(t *testing.T) {
	src := &fakeSource{connected: true}
	w, err := Listen("session-b", "peer-b", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	done := make(chan string, 1)
	go func() { done <- dialAndRead(t, w.socketPath) }()
	time.Sleep(100 * time.Millisecond) // let it register as the current waiter

	src.push("new message")
	w.Poke()

	select {
	case got := <-done:
		if got == "" {
			t.Fatal("expected non-empty output")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}
}

func TestNewWaiterSupersedesOld(t *testing.T) {
	src := &fakeSource{connected: true}
	w, err := Listen("session-c", "peer-c", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	oldDone := make(chan string, 1)
	go func() { oldDone <- dialAndRead(t, w.socketPath) }()
	time.Sleep(100 * time.Millisecond)

	newDone := make(chan string, 1)
	go func() { newDone <- dialAndRead(t, w.socketPath) }()

	select {
	case got := <-oldDone:
		if got != "superseded by a newer wait\n" {
			t.Fatalf("expected the old waiter to be superseded, got: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the old waiter to be superseded")
	}

	src.push("hi")
	w.Poke()
	select {
	case got := <-newDone:
		if got == "" || got == "superseded by a newer wait\n" {
			t.Fatalf("expected the new waiter to receive the message, got: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the new waiter's delivery")
	}
}

func TestSocketPathStaysWithinUnixSocketPathLimit(t *testing.T) {
	// Real sessionId/peerId are both 36-char UUIDs. Unix-domain sockets have a
	// 108-byte sun_path limit on Linux, macOS, AND Windows' AF_UNIX - the
	// filename portion alone must leave generous headroom for the OS temp
	// directory (which on Windows can itself be 40-50+ chars).
	src := &fakeSource{connected: true}
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	peerID := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	w, err := Listen(sessionID, peerID, src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	filename := filepath.Base(w.socketPath)
	if len(filename) > 50 {
		t.Fatalf("socket filename too long (%d bytes), leaves no headroom under the 108-byte "+
			"sun_path limit once combined with a temp dir: %q", len(filename), filename)
	}
}

func TestSocketPathDiffersForDifferentPeers(t *testing.T) {
	src := &fakeSource{connected: true}
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	w1, err := Listen(sessionID, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", src)
	if err != nil {
		t.Fatalf("listen 1: %v", err)
	}
	defer w1.Close()

	w2, err := Listen(sessionID, "6ba7b811-9dad-11d1-80b4-00c04fd430c8", src)
	if err != nil {
		t.Fatalf("listen 2: %v", err)
	}
	defer w2.Close()

	if w1.socketPath == w2.socketPath {
		t.Fatalf("expected distinct socket paths for distinct peerIds, both got %q", w1.socketPath)
	}
}

func TestDisconnectedSourceReportedImmediately(t *testing.T) {
	src := &fakeSource{connected: false}
	w, err := Listen("session-d", "peer-d", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	got := dialAndRead(t, w.socketPath)
	if got != "hub disconnected\n" {
		t.Fatalf("got %q", got)
	}
}
