package hubconn

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
	"github.com/secforge/mcp-hub/internal/wsserver"
)

func startTestServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(wsserver.NewHandler())
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func waitForActivity(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for activity")
	}
}

func TestDialJoinsAndAssignsPeerID(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if c.PeerID() == "" {
		t.Fatal("expected a non-empty peerID")
	}
}

func TestDialRejectsInvalidSessionID(t *testing.T) {
	url := startTestServer(t)
	if _, err := Dial(url, "not-a-uuid", DialOptions{}); err == nil {
		t.Fatal("expected an error for an invalid sessionId")
	}
}

func TestDialSendsCurrentProtocolVersionAsQueryParam(t *testing.T) {
	var gotQuery string
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	want := "v=" + strconv.Itoa(wire.ProtocolVersion)
	if gotQuery != want {
		t.Fatalf("got query %q, want %q", gotQuery, want)
	}
}

func TestDialCapturesServerVersion(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if c.ServerVersion() != wire.ProtocolVersion {
		t.Fatalf("got ServerVersion %d, want %d", c.ServerVersion(), wire.ProtocolVersion)
	}
}

func TestExpectedPeerCountMatchesJoinedPeerCount(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	if a.ExpectedPeerCount() != 0 {
		t.Fatalf("a: expected 0, got %d", a.ExpectedPeerCount())
	}

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()
	if b.ExpectedPeerCount() != 1 {
		t.Fatalf("b: expected 1, got %d", b.ExpectedPeerCount())
	}
}

func TestRosterCompleteImmediatelyWhenNoExistingPeers(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	activity := make(chan struct{}, 8)
	c.OnActivity(func() { activity <- struct{}{} })

	waitForActivity(t, activity) // the server's rosterComplete for an empty roster
	if !c.RosterComplete() {
		t.Fatal("expected RosterComplete to be true once the server's rosterComplete event arrives")
	}

	formatted, connected := c.Drain()
	if !connected {
		t.Fatal("expected still connected")
	}
	if !strings.Contains(formatted, "roster complete") {
		t.Fatalf("expected a rosterComplete notification already buffered, got: %q", formatted)
	}
}

func TestRosterCompleteBecomesTrueOnceCaughtUp(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()

	activity := make(chan struct{}, 8)
	b.OnActivity(func() { activity <- struct{}{} })

	if b.RosterComplete() {
		t.Fatal("expected RosterComplete to be false before the roster catch-up event arrives")
	}
	waitForActivity(t, activity) // b is told about a (roster entry)
	waitForActivity(t, activity) // b's rosterComplete

	if !b.RosterComplete() {
		t.Fatal("expected RosterComplete to be true after catching up on the existing roster")
	}
	formatted, connected := b.Drain()
	if !connected {
		t.Fatal("expected still connected")
	}
	if !strings.Contains(formatted, "roster complete") {
		t.Fatalf("expected a rosterComplete notification in the drained events, got: %q", formatted)
	}
}

func TestDialRejectsHostWithPath(t *testing.T) {
	url := startTestServer(t)
	if _, err := Dial(url+"/extra-path", "550e8400-e29b-41d4-a716-446655440000", DialOptions{}); err == nil {
		t.Fatal("expected Dial to reject a host containing a path")
	}
}

func TestSendAndReceiveBetweenTwoConns(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()

	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })
	waitForActivity(t, activity) // a's own rosterComplete (no peers yet)

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()

	waitForActivity(t, activity) // a sees b's peerJoined

	if err := b.Send("hello"); err != nil {
		t.Fatalf("send: %v", err)
	}
	waitForActivity(t, activity) // a sees the message

	formatted, connected := a.Drain()
	if !connected {
		t.Fatal("expected still connected")
	}
	if !strings.Contains(formatted, "hello") {
		t.Fatalf("expected drained events to include the message, got: %s", formatted)
	}
}

func TestSendToDeliversOnlyToTargetAndMarksPrivate(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	activityA := make(chan struct{}, 8)
	a.OnActivity(func() { activityA <- struct{}{} })
	waitForActivity(t, activityA) // a's own rosterComplete (no peers yet)

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()
	activityB := make(chan struct{}, 8)
	b.OnActivity(func() { activityB <- struct{}{} })

	waitForActivity(t, activityA) // a sees b's peerJoined
	a.Drain()

	if err := b.SendTo("just for you", a.PeerID()); err != nil {
		t.Fatalf("sendTo: %v", err)
	}
	waitForActivity(t, activityA)

	formatted, connected := a.Drain()
	if !connected {
		t.Fatal("expected still connected")
	}
	if !strings.Contains(formatted, "just for you") || !strings.Contains(formatted, "PRIVATE") {
		t.Fatalf("expected a private message wrapper, got: %s", formatted)
	}
}

func TestSendToUnknownPeerSurfacesAsErrorEvent(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer a.Close()
	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })
	waitForActivity(t, activity) // a's own rosterComplete (no peers yet)

	if err := a.SendTo("hello?", "00000000-0000-0000-0000-000000000000"); err != nil {
		t.Fatalf("sendTo: %v", err)
	}
	waitForActivity(t, activity)

	formatted, connected := a.Drain()
	if !connected {
		t.Fatal("expected still connected")
	}
	if !strings.Contains(formatted, "[HUB ERROR]") {
		t.Fatalf("expected an error event, got: %s", formatted)
	}
}

func TestPeersTracksRosterAsPeersJoinAndLeave(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })
	waitForActivity(t, activity) // a's own rosterComplete (no peers yet)

	if peers := a.Peers(); len(peers) != 0 {
		t.Fatalf("expected no peers before anyone else joins, got %v", peers)
	}

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	waitForActivity(t, activity) // a sees b's peerJoined

	peers := a.Peers()
	if len(peers) != 1 || peers[0].ID != b.PeerID() {
		t.Fatalf("expected [%s], got %v", b.PeerID(), peers)
	}

	c, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial c: %v", err)
	}
	defer c.Close()
	waitForActivity(t, activity) // a sees c's peerJoined

	peers = a.Peers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %v", peers)
	}

	b.Close()
	waitForActivity(t, activity) // a sees b's peerLeft

	peers = a.Peers()
	if len(peers) != 1 || peers[0].ID != c.PeerID() {
		t.Fatalf("expected just [%s] after b left, got %v", c.PeerID(), peers)
	}
}

func TestPeekAndDrainReflectDisconnect(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	b.Close()
	// server closing the connection propagates as a peerLeft to a, then
	// (separately) a's own socket must be closed by the test to observe its
	// own disconnect:
	a.Close()

	// a's readLoop goroutine notices the close asynchronously (it's still
	// mid-flight on whatever roster/peerJoined/peerLeft traffic arrived
	// before the close), so poll briefly instead of checking Peek()
	// immediately.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, connected := a.Peek(); !connected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected Peek to eventually report disconnected after Close")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSilentDropIsDetectedViaReadDeadline proves the fix for a real gap: a
// server that goes silent without ever closing the TCP connection (no data,
// no ping, no FIN/RST — e.g. a killed process behind a still-open socket,
// or a network partition) used to be invisible to the client forever, since
// ws.ReadMessage blocked with no deadline. Peek/Drain would report
// connected indefinitely and anything sent in that window was silently
// lost. With a read deadline that only pongWait resets, a silent server
// must be noticed once that deadline elapses.
func TestSilentDropIsDetectedViaReadDeadline(t *testing.T) {
	origPongWait, origWriteWait := pongWait, writeWait
	pongWait = 60 * time.Millisecond
	writeWait = 30 * time.Millisecond
	defer func() { pongWait, writeWait = origPongWait, origWriteWait }()

	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		// go silent forever — no more frames, no ping, no close.
		select {}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, connected := c.Peek(); !connected {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("expected the silent server to eventually be detected as disconnected")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

const testAgePublicKey = "age1scdm7mae5t68c9ch0sqfzlusqyflpgxlrgk3zwl44zwl9vvq2guqtdv4fk"

func TestDialRejectsMalformedAgePublicKey(t *testing.T) {
	url := startTestServer(t)
	_, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{AgePublicKey: "not-a-key"})
	if err == nil {
		t.Fatal("expected an error for a malformed agePublicKey")
	}
}

func TestDialEchoesBackSanitizedNameAndAgePublicKey(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000",
		DialOptions{Name: "Alice\n\x1b[31m", AgePublicKey: testAgePublicKey})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if c.Name() != "Alice[31m" {
		t.Fatalf("expected control chars/newlines stripped from name, got %q", c.Name())
	}
	if c.AgePublicKey() != testAgePublicKey {
		t.Fatalf("got AgePublicKey %q, want %q", c.AgePublicKey(), testAgePublicKey)
	}
}

func TestPeersReportsNameAndAgePublicKeyOfOthers(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })
	waitForActivity(t, activity) // a's own rosterComplete (no peers yet)

	b, err := Dial(url, sessionID, DialOptions{Name: "Alice", AgePublicKey: testAgePublicKey})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()
	waitForActivity(t, activity) // a sees b's peerJoined

	peers := a.Peers()
	if len(peers) != 1 || peers[0].Name != "Alice" || peers[0].AgePublicKey != testAgePublicKey {
		t.Fatalf("expected b's name/agePublicKey to be reported, got %+v", peers)
	}
}
