package waiter

import (
	"fmt"
	"io"
	"net"
	"os"
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
	// chunks holds one entry per push (mirroring hubconn.Conn's buffer of
	// discrete events, one formatted string each) — Drain joins them into
	// a single string (space-separated); DrainBatch returns them as-is.
	chunks []string
}

func (f *fakeSource) Peek() (bool, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hasEvents, f.connected
}

func (f *fakeSource) Drain() (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	formatted := strings.Join(f.chunks, " ")
	f.hasEvents = false
	f.chunks = nil
	return formatted, f.connected
}

func (f *fakeSource) DrainBatch() ([]string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	chunks := f.chunks
	f.hasEvents = false
	f.chunks = nil
	return chunks, f.connected
}

// push appends text to whatever's already buffered, mirroring
// hubconn.Conn's real buffer — which accumulates every event until
// drained — rather than overwriting, so concurrent pushes before a Drain
// aren't lost at the fake's own level.
func (f *fakeSource) push(text string) {
	f.mu.Lock()
	f.hasEvents = true
	f.chunks = append(f.chunks, text)
	f.mu.Unlock()
}

// setConnected flips the source's connected state, standing in for a
// connection ending under a reader that is attached to several.
func (f *fakeSource) setConnected(v bool) {
	f.mu.Lock()
	f.connected = v
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
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
	defer w.Close()

	got := dialAndRead(t, w.socketPath)
	if got != "Run this command again to keep receiving:\n"+w.WaitCommand()+
		"\n\n[connection: t]\nhello\n" {
		t.Fatalf("unexpected output: %q", got)
	}
	// The re-run reminder must lead, not trail: a reader whose own read
	// times out partway through only ever sees a prefix of the output, so
	// anything after that point (a reminder tacked onto the end) can be
	// silently lost — see the comment on this in deliver().
	if !strings.HasPrefix(got, "Run this command again to keep receiving:") {
		t.Fatalf("expected the re-run reminder to lead the output, got: %q", got)
	}
}

func TestPokeDeliversToRegisteredWaiter(t *testing.T) {
	src := &fakeSource{connected: true}
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
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

// TestPokeDoesNotDeliverWhileSourceReportsNotWakeWorthy is the waiter-side
// regression test for own-message suppression (see hubconn.Conn.Peek):
// even with content already sitting in the source's buffer, Poke must not
// drain and deliver it while Peek still says hasEvents=false (e.g. an
// own-send echo held back pending a real reply) — otherwise the whole
// point of the source's suppression decision would be defeated by the
// waiter draining it anyway. Once the source flips to wake-worthy, the
// still-registered waiter gets everything, including what was held back.
func TestPokeDoesNotDeliverWhileSourceReportsNotWakeWorthy(t *testing.T) {
	src := &fakeSource{connected: true}
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
	defer w.Close()

	done := make(chan string, 1)
	go func() { done <- dialAndRead(t, w.socketPath) }()
	time.Sleep(100 * time.Millisecond) // let it register as the current waiter

	// Simulate content sitting in the buffer that the source has decided
	// is not (yet) wake-worthy — Peek must keep reporting hasEvents=false
	// even though Drain would return something.
	src.mu.Lock()
	src.chunks = []string{"own echo, not wake-worthy yet"}
	src.mu.Unlock()
	w.Poke()

	select {
	case got := <-done:
		t.Fatalf("expected no delivery while the source isn't wake-worthy, got: %q", got)
	case <-time.After(300 * time.Millisecond):
		// expected: still registered, nothing delivered
	}

	// Now the source becomes wake-worthy (e.g. a real reply arrived).
	src.mu.Lock()
	src.hasEvents = true
	src.mu.Unlock()
	w.Poke()

	select {
	case got := <-done:
		if !strings.Contains(got, "own echo, not wake-worthy yet") {
			t.Fatalf("expected the held-back content to be delivered once wake-worthy, got: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery after becoming wake-worthy")
	}
}

func TestNewWaiterSupersedesOld(t *testing.T) {
	src := &fakeSource{connected: true}
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
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
	// Unix-domain sockets have a 108-byte sun_path limit on Linux, macOS,
	// AND Windows' AF_UNIX - the filename portion alone must leave
	// generous headroom for the OS temp directory (which on Windows can
	// itself be 40-50+ chars).
	src := &fakeSource{connected: true}
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
	defer w.Close()

	filename := filepath.Base(w.socketPath)
	if len(filename) > 50 {
		t.Fatalf("socket filename too long (%d bytes), leaves no headroom under the 108-byte "+
			"sun_path limit once combined with a temp dir: %q", len(filename), filename)
	}
}

// TestSocketPathDiffersAcrossCalls proves the path is genuinely random per
// call, not derived from anything about the connection — including two
// Listen calls that would, under the old (sessionID, peerID)-derived
// scheme, have produced the identical path (same peerID both times, the
// now-routine case via a persisted reconnectSecret/supersede). See
// Listen's own doc comment for why a deterministic path became a real
// cross-process collision hazard once that became the common case.
func TestSocketPathDiffersAcrossCalls(t *testing.T) {
	w1, err := Listen()
	if err != nil {
		t.Fatalf("listen 1: %v", err)
	}
	defer w1.Close()

	w2, err := Listen()
	if err != nil {
		t.Fatalf("listen 2: %v", err)
	}
	defer w2.Close()

	if w1.socketPath == w2.socketPath {
		t.Fatalf("expected distinct socket paths across separate Listen calls, both got %q", w1.socketPath)
	}
}

func TestFollowDeliversMultipleEventsOverSameConnection(t *testing.T) {
	src := &fakeSource{connected: true}
	src.push("first")
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
	defer w.Close()

	conn := dialFollow(t, w.socketPath)
	defer conn.Close()

	got := readChunk(t, conn)
	if got != "[connection: t]\nfirst\n\n" {
		t.Fatalf("first chunk: got %q", got)
	}

	src.push("second")
	w.Poke()

	got = readChunk(t, conn)
	if got != "[connection: t]\nsecond\n\n" {
		t.Fatalf("second chunk: got %q", got)
	}
}

// TestFollowDeliversABurstAsSeparateChunksNotOneJoinedWrite is the
// regression test for the truncation-exposure fix: several events
// buffered before a single Poke() must go out as N separately-delimited
// chunks (DrainBatch), not one blob (the old Drain-based behavior) — see
// Source.DrainBatch's doc comment. readChunk's "\n\n" framing is what a
// reader actually relies on to tell chunks apart, regardless of how many
// underlying network writes or downstream reads happen to combine them,
// which is why this asserts on the delimited chunks rather than on write
// counts the test can't observe from the client side of the socket
// anyway.
// TestFollowSpacesWritesWithinABurst is the regression test for live-
// emission pacing: two chunks delivered in the same burst must be
// separated by at least liveEmissionSpacing, so a downstream layer
// coalescing anything within its own window cannot merge them — see
// deliver's own doc comment and the design doc's live-emission-pacing
// section.
func TestFollowSpacesWritesWithinABurst(t *testing.T) {
	orig := liveEmissionSpacing
	liveEmissionSpacing = 100 * time.Millisecond
	t.Cleanup(func() { liveEmissionSpacing = orig })

	src := &fakeSource{connected: true}
	src.push("first")
	src.push("second")
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
	defer w.Close()

	conn := dialFollow(t, w.socketPath)
	defer conn.Close()
	w.Poke()

	start := time.Now()
	if got := readChunk(t, conn); got != "[connection: t]\nfirst\n\n" {
		t.Fatalf("first chunk: got %q", got)
	}
	firstAt := time.Since(start)
	if got := readChunk(t, conn); got != "[connection: t]\nsecond\n\n" {
		t.Fatalf("second chunk: got %q", got)
	}
	secondAt := time.Since(start)

	if firstAt > 50*time.Millisecond {
		t.Fatalf("expected the first chunk with no delay, took %v", firstAt)
	}
	if secondAt-firstAt < liveEmissionSpacing {
		t.Fatalf("expected at least %v between chunks, got %v", liveEmissionSpacing, secondAt-firstAt)
	}
}

func TestFollowDeliversABurstAsSeparateChunksNotOneJoinedWrite(t *testing.T) {
	orig := liveEmissionSpacing
	liveEmissionSpacing = time.Millisecond
	t.Cleanup(func() { liveEmissionSpacing = orig })

	src := &fakeSource{connected: true}
	src.push("first")
	src.push("second")
	src.push("third")
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
	defer w.Close()

	conn := dialFollow(t, w.socketPath)
	defer conn.Close()
	w.Poke()

	for _, want := range []string{"[connection: t]\nfirst\n\n", "[connection: t]\nsecond\n\n", "[connection: t]\nthird\n\n"} {
		if got := readChunk(t, conn); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestFollowChunksHaveNoRerunTrailer(t *testing.T) {
	src := &fakeSource{connected: true}
	src.push("hello")
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
	defer w.Close()

	conn := dialFollow(t, w.socketPath)
	defer conn.Close()

	got := readChunk(t, conn)
	if got != "[connection: t]\nhello\n\n" {
		t.Fatalf("follow-mode delivery should have no rerun trailer, got %q", got)
	}
}

func TestNewWaiterSupersedesFollowConnection(t *testing.T) {
	src := &fakeSource{connected: true}
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
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
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
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
	orig := liveEmissionSpacing
	liveEmissionSpacing = 0
	t.Cleanup(func() { liveEmissionSpacing = orig })

	src := &fakeSource{connected: true}
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
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
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
	defer w.Close()

	// One connection ending is reported BY NAME rather than as the
	// channel disconnecting: the channel carries others, and a reader told
	// "hub disconnected" could not tell which of the two happened.
	got := dialAndRead(t, w.socketPath)
	if !strings.Contains(got, `"t" disconnected`) {
		t.Fatalf("got %q", got)
	}
}

// fakeSourceWithNote is a fakeSource that also implements disconnectNoter,
// simulating a teams Source (e.g. *hubconn.Conn after a relay
// server's close code) that can explain why it died beyond an ordinary
// drop.
type fakeSourceWithNote struct {
	fakeSource
	note string
}

func (f *fakeSourceWithNote) DisconnectNote() string { return f.note }

func TestDisconnectedSourceNoteIsAppendedWhenSourceProvidesOne(t *testing.T) {
	src := &fakeSourceWithNote{fakeSource: fakeSource{connected: false}, note: " (revoked — do not reconnect)"}
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
	defer w.Close()

	got := dialAndRead(t, w.socketPath)
	if !strings.Contains(got, `"t" disconnected (revoked — do not reconnect)`) {
		t.Fatalf("got %q", got)
	}
}

// TestListenSweepsStaleSocketsButNeverTouchesALiveOne is the core safety
// test for the janitor sweep: it plants both a genuinely stale socket file
// (from a listener that's already closed) and a real, still-active one,
// then confirms a fresh Listen() call removes only the stale one — the
// live listener's file, its ability to accept connections, and its
// internal state must all survive completely untouched.
func TestListenSweepsStaleSocketsButNeverTouchesALiveOne(t *testing.T) {
	// a socket *file* with no listener behind it at all — net.Listener's
	// own Close() would unlink it, which isn't what actually happens: a
	// killed/crashed process never runs Close() (or any defer) in the
	// first place, so the file is simply left behind with nothing ever
	// having cleaned it up. A plain regular file reproduces that end state
	// well enough for isStaleSocket's purposes — dialing it fails
	// (ENOTSOCK rather than ECONNREFUSED, but isStaleSocket only checks
	// for any dial error) exactly as dialing a truly-orphaned socket file
	// would.
	stalePath := filepath.Join(socketDir, "mcp-hub-wait-0000000000000000.sock")
	if err := os.WriteFile(stalePath, nil, 0o600); err != nil {
		t.Fatalf("create stale socket file: %v", err)
	}

	// a real, still-active listener that must survive the sweep untouched.
	liveSrc := &fakeSource{connected: true}
	liveWaiter, err := Listen()
	if err != nil {
		t.Fatalf("listen (live): %v", err)
	}
	liveWaiter.SetSource("t", liveSrc)
	defer liveWaiter.Close()

	// register a real waiter on the live one, so we can also prove its
	// internal state (w.current) wasn't disturbed by the sweep.
	liveConn := dialFollow(t, liveWaiter.socketPath)
	defer liveConn.Close()
	time.Sleep(50 * time.Millisecond) // let it register as current

	// a fresh Listen() call — the thing that actually triggers a sweep.
	triggerWaiter, err := Listen()
	if err != nil {
		t.Fatalf("listen (sweep trigger): %v", err)
	}
	defer triggerWaiter.Close()

	if _, err := os.Stat(stalePath); !os.IsNotExist(err) {
		t.Fatalf("expected the stale socket file to be swept away, stat err: %v", err)
	}
	if _, err := os.Stat(liveWaiter.socketPath); err != nil {
		t.Fatalf("expected the live socket file to survive the sweep, stat err: %v", err)
	}

	// the live listener must still actually work: push an event and
	// confirm the already-registered connection still receives it,
	// proving the sweep's probe-connect-and-abandon didn't supersede or
	// otherwise disturb its registration.
	liveSrc.push("still alive after the sweep")
	liveWaiter.Poke()
	got := readChunk(t, liveConn)
	if got != "[connection: t]\nstill alive after the sweep\n\n" {
		t.Fatalf("expected the live waiter to still deliver normally after the sweep, got %q", got)
	}
}

func TestIsStaleSocketDetectsAbsentAndDeadSockets(t *testing.T) {
	if !isStaleSocket(filepath.Join(socketDir, "does-not-exist.sock")) {
		t.Fatal("expected a nonexistent path to be reported stale")
	}
}

// A nil *Waiter is a valid state: in push mode no wait socket is bound,
// so every method must tolerate being called on nothing. Guarding at the
// call sites instead cost an MCP server crash — ten sites, one missed,
// and Poke ran on the first connection event.
func TestEveryMethodToleratesANilWaiter(t *testing.T) {
	var w *Waiter
	// Each of these ran on a live path in push mode.
	w.Poke()
	w.ExpectReconnect("t")
	w.Announce("anything")
	w.SetSource("t", nil)
	if w.Following() {
		t.Error("a nil waiter reported a follower")
	}
	if got := w.WaitCommand(); got != "" {
		t.Errorf("WaitCommand on nil = %q, want empty", got)
	}
	if got := w.WaitFollowCommand(); got != "" {
		t.Errorf("WaitFollowCommand on nil = %q, want empty", got)
	}
	if err := w.Close(); err != nil {
		t.Errorf("Close on nil = %v, want nil", err)
	}
}

// One channel, several conversations: a reader gets every connection's
// traffic on the one socket, and every line says which connection it
// belongs to. A pull harness runs one `wait` process and cannot run one
// per connection, so this is the only shape that reaches it at all.
func TestOneChannelCarriesSeveralConnectionsLabelled(t *testing.T) {
	alpha := &fakeSource{connected: true}
	beta := &fakeSource{connected: true}
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("alpha", alpha)
	w.SetSource("beta", beta)
	defer w.Close()

	conn := dialFollow(t, w.socketPath)
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	alpha.push("from alpha")
	w.Poke()
	if got := readChunk(t, conn); got != "[connection: alpha]\nfrom alpha\n\n" {
		t.Fatalf("expected alpha's line labelled, got %q", got)
	}
	beta.push("from beta")
	w.Poke()
	if got := readChunk(t, conn); got != "[connection: beta]\nfrom beta\n\n" {
		t.Fatalf("expected beta's line labelled, got %q", got)
	}
}

// One connection ending is a fact about that conversation, not about the
// channel. The reader is told which one, by name, and stays attached —
// releasing it would leave the remaining conversations with nothing
// listening, and nothing could reattach it.
func TestOneConnectionEndingDoesNotEndTheChannel(t *testing.T) {
	alpha := &fakeSource{connected: true}
	beta := &fakeSource{connected: true}
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("alpha", alpha)
	w.SetSource("beta", beta)
	defer w.Close()

	conn := dialFollow(t, w.socketPath)
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	alpha.setConnected(false)
	w.Poke()
	got := readChunk(t, conn)
	if !strings.Contains(got, `"alpha" disconnected`) {
		t.Fatalf("expected alpha's ending to be named, got %q", got)
	}
	if strings.HasPrefix(got, "hub disconnected") {
		t.Fatalf("expected the channel NOT to report itself disconnected, got %q", got)
	}

	// Still live for beta, on the same connection the reader already has.
	beta.push("still here")
	w.Poke()
	if got := readChunk(t, conn); got != "[connection: beta]\nstill here\n\n" {
		t.Fatalf("expected beta to keep delivering on the same channel, got %q", got)
	}
	if attached := w.Attached(); len(attached) != 1 || attached[0] != "beta" {
		t.Fatalf("expected only beta attached, got %v", attached)
	}
}

// Poke is called from the CONNECTION'S READ LOOP, so it must return at
// once. It used to deliver inline, which meant every pause delivery took
// — the spacing between chunks, a write to a slow reader — was time that
// socket was not being read: a twenty-event burst spaced at 500ms stopped
// reading for about 9.5 seconds, worst exactly when messages arrive
// fastest.
func TestPokeDoesNotWaitForTheDelivery(t *testing.T) {
	orig := liveEmissionSpacing
	liveEmissionSpacing = 300 * time.Millisecond
	defer func() { liveEmissionSpacing = orig }()

	src := &fakeSource{connected: true}
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
	defer w.Close()

	conn := dialFollow(t, w.socketPath)
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	// Three chunks: delivery pays two spacings, 600ms.
	src.push("one")
	src.push("two")
	src.push("three")

	start := time.Now()
	w.Poke()
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Poke blocked for %v — the read loop would be stalled that long", elapsed)
	}

	// And the delivery still happens, spaced, on its own goroutine.
	for _, want := range []string{"one", "two", "three"} {
		if got := readChunk(t, conn); got != "[connection: t]\n"+want+"\n\n" {
			t.Fatalf("expected %q, got %q", want, got)
		}
	}
	if elapsed := time.Since(start); elapsed < 2*liveEmissionSpacing {
		t.Fatalf("expected the spacing to still be paid on the delivery side, took only %v", elapsed)
	}
}

// TestASilentConnectionDoesNotBlockTheNextReader is Codex's finding,
// 2026-09-19: handleAccept waited for the mode byte on the accept loop
// itself and with no deadline, so one local connection that connected and
// then said nothing stopped every later reader from being accepted.
func TestASilentConnectionDoesNotBlockTheNextReader(t *testing.T) {
	src := &fakeSource{connected: true}
	w, err := Listen()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	w.SetSource("t", src)
	defer w.Close()

	// Connects, says nothing, holds the socket open.
	silent, err := net.Dial("unix", w.socketPath)
	if err != nil {
		t.Fatalf("dialling the wait socket: %v", err)
	}
	defer silent.Close()

	// A real reader arriving behind it must still be accepted and served.
	served := make(chan error, 1)
	go func() {
		c, err := net.Dial("unix", w.socketPath)
		if err != nil {
			served <- err
			return
		}
		defer c.Close()
		_, err = c.Write([]byte{ModeFollow})
		served <- err
	}()

	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("the second reader could not be served: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a connection that sent nothing blocked the accept loop, so no later reader " +
			"could be served at all")
	}

	deadline := time.Now().Add(5 * time.Second)
	for !w.Following() {
		if time.Now().After(deadline) {
			t.Fatal("expected the follower behind the silent connection to register")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
