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
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000")
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
	if _, err := Dial(url, "not-a-uuid"); err == nil {
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
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0))
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
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
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000")
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

	a, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	if a.ExpectedPeerCount() != 0 {
		t.Fatalf("a: expected 0, got %d", a.ExpectedPeerCount())
	}

	b, err := Dial(url, sessionID)
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
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if !c.RosterComplete() {
		t.Fatal("expected RosterComplete to be true immediately when there were no existing peers")
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

	a, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()

	b, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()

	activity := make(chan struct{}, 8)
	b.OnActivity(func() { activity <- struct{}{} })

	if b.RosterComplete() {
		t.Fatal("expected RosterComplete to be false before the roster catch-up event arrives")
	}
	waitForActivity(t, activity) // b catches up on the roster (a)

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
	if _, err := Dial(url+"/extra-path", "550e8400-e29b-41d4-a716-446655440000"); err == nil {
		t.Fatal("expected Dial to reject a host containing a path")
	}
}

func TestSendAndReceiveBetweenTwoConns(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()

	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })

	b, err := Dial(url, sessionID)
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

	a, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	activityA := make(chan struct{}, 8)
	a.OnActivity(func() { activityA <- struct{}{} })

	b, err := Dial(url, sessionID)
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

	a, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer a.Close()
	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })

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

	a, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })

	if peers := a.Peers(); len(peers) != 0 {
		t.Fatalf("expected no peers before anyone else joins, got %v", peers)
	}

	b, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	waitForActivity(t, activity) // a sees b's peerJoined

	peers := a.Peers()
	if len(peers) != 1 || peers[0] != b.PeerID() {
		t.Fatalf("expected [%s], got %v", b.PeerID(), peers)
	}

	c, err := Dial(url, sessionID)
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
	if len(peers) != 1 || peers[0] != c.PeerID() {
		t.Fatalf("expected just [%s] after b left, got %v", c.PeerID(), peers)
	}
}

func TestPeekAndDrainReflectDisconnect(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })

	b, err := Dial(url, sessionID)
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	waitForActivity(t, activity) // a sees b's peerJoined
	b.Close()
	// server closing the connection propagates as a peerLeft to a, then
	// (separately) a's own socket must be closed by the test to observe its
	// own disconnect:
	a.Close()

	if _, connected := a.Peek(); connected {
		t.Fatal("expected Peek to report disconnected after Close")
	}
}
