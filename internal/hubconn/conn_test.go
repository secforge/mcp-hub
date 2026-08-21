package hubconn

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
