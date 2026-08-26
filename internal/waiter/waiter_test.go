package waiter

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
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

// push appends text to whatever's already buffered (joined by a space),
// mirroring hubconn.Conn's real buffer — which accumulates every event
// until drained — rather than overwriting, so concurrent pushes before a
// Drain aren't lost at the fake's own level.
func (f *fakeSource) push(text string) {
	f.mu.Lock()
	f.hasEvents = true
	if f.formatted != "" {
		f.formatted += " "
	}
	f.formatted += text
	f.mu.Unlock()
}

// dialAndRead connects in "once" mode (like the default `wait` invocation):
// sends the once-mode byte, reads until the server closes the connection.
func dialAndRead(t *testing.T, path string) string {
	t.Helper()
	conn := dialMode(t, path, ModeOnce)
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	data, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(data)
}

// dialFollow connects in "follow" mode: sends the follow-mode byte and
// returns the open connection for the caller to read repeated deliveries
// from without reconnecting.
func dialFollow(t *testing.T, path string) net.Conn {
	t.Helper()
	return dialMode(t, path, ModeFollow)
}

func dialMode(t *testing.T, path string, mode byte) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if _, err := conn.Write([]byte{mode}); err != nil {
		t.Fatalf("write mode byte: %v", err)
	}
	return conn
}

// readChunk reads until it sees the "\n\n" chunk terminator follow-mode
// deliveries use, without consuming past it (so the connection can be read
// again for the next chunk).
func readChunk(t *testing.T, conn net.Conn) string {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var buf []byte
	one := make([]byte, 1)
	for {
		n, err := conn.Read(one)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if n == 1 {
			buf = append(buf, one[0])
		}
		if len(buf) >= 2 && buf[len(buf)-1] == '\n' && buf[len(buf)-2] == '\n' {
			return string(buf)
		}
	}
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
		if got != w.supersededMessage() {
			t.Fatalf("expected the old waiter to be superseded, got: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the old waiter to be superseded")
	}

	src.push("hi")
	w.Poke()
	select {
	case got := <-newDone:
		if got == "" || got == w.supersededMessage() {
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

func TestFollowDeliversMultipleEventsOverSameConnection(t *testing.T) {
	src := &fakeSource{connected: true}
	src.push("first")
	w, err := Listen("session-e", "peer-e", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	conn := dialFollow(t, w.socketPath)
	defer conn.Close()

	got := readChunk(t, conn)
	if got != "first\n\n" {
		t.Fatalf("first chunk: got %q", got)
	}

	src.push("second")
	w.Poke()

	got = readChunk(t, conn)
	if got != "second\n\n" {
		t.Fatalf("second chunk: got %q", got)
	}
}

func TestFollowChunksHaveNoRerunTrailer(t *testing.T) {
	src := &fakeSource{connected: true}
	src.push("hello")
	w, err := Listen("session-f", "peer-f", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	conn := dialFollow(t, w.socketPath)
	defer conn.Close()

	got := readChunk(t, conn)
	if got != "hello\n\n" {
		t.Fatalf("follow-mode delivery should have no rerun trailer, got %q", got)
	}
}

func TestNewWaiterSupersedesFollowConnection(t *testing.T) {
	src := &fakeSource{connected: true}
	w, err := Listen("session-g", "peer-g", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	oldConn := dialFollow(t, w.socketPath)
	defer oldConn.Close()
	time.Sleep(100 * time.Millisecond) // let it register

	oldConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	newDone := make(chan string, 1)
	go func() { newDone <- dialAndRead(t, w.socketPath) }()

	got, err := io.ReadAll(oldConn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != w.supersededMessage() {
		t.Fatalf("expected the follow connection to be superseded, got %q", got)
	}

	src.push("hi")
	w.Poke()
	select {
	case got := <-newDone:
		if got == "" || got == w.supersededMessage() {
			t.Fatalf("expected the new waiter to receive the message, got: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the new waiter's delivery")
	}
}

func TestWaitFollowCommandAppendsFollowFlag(t *testing.T) {
	src := &fakeSource{connected: true}
	w, err := Listen("session-e", "peer-e", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	if got, want := w.WaitFollowCommand(), w.WaitCommand()+" --follow"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestFollowNeverLosesAnEventToRegistrationRace is a regression test for a
// real production incident: deliver()'s re-registration used to check
// source.Peek() *before* acquiring w.mu, then separately lock to set
// w.current. A Poke() landing in that gap (triggered by an event that had,
// in fact, already arrived) would find w.current still nil, silently no-op
// (Poke does nothing when nothing is registered), and the event that
// arrived in the gap would then never be delivered — deliver() would go on
// to register based on its earlier, now-stale Peek() result, and nothing
// would ever poke that registration again. The connection stayed alive,
// looking like a healthy listener, forever waiting for a notification that
// had already happened and was missed. Checking Peek() and setting
// w.current under the same uninterrupted lock hold (what Poke() also uses)
// closes the gap. This test hammers Poke() concurrently with pushes to
// prove no event is ever silently dropped.
func TestFollowNeverLosesAnEventToRegistrationRace(t *testing.T) {
	src := &fakeSource{connected: true}
	w, err := Listen("session-i", "peer-i", src)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer w.Close()

	conn := dialFollow(t, w.socketPath)
	defer conn.Close()

	const n = 200
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			src.push(fmt.Sprintf("msg-%d", i))
			w.Poke()
		}
	}()
	go func() {
		defer wg.Done()
		// Extra, redundant pokes maximize the chance of landing exactly in
		// the vulnerable window on the old, buggy code.
		for i := 0; i < n; i++ {
			w.Poke()
		}
	}()
	wg.Wait()

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var all strings.Builder
	buf := make([]byte, 4096)
	want := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		want[fmt.Sprintf("msg-%d", i)] = false
	}
	remaining := n
	deadline := time.Now().Add(5 * time.Second)
	for remaining > 0 && time.Now().Before(deadline) {
		nRead, err := conn.Read(buf)
		if err != nil {
			break
		}
		all.Write(buf[:nRead])
		for k, seen := range want {
			if !seen && strings.Contains(all.String(), k) {
				want[k] = true
				remaining--
			}
		}
	}
	if remaining > 0 {
		missing := make([]string, 0, remaining)
		for k, seen := range want {
			if !seen {
				missing = append(missing, k)
			}
		}
		t.Fatalf("lost %d/%d events to the registration race, e.g. %v", remaining, n, missing[:min(5, len(missing))])
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
