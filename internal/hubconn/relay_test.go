package hubconn

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

// startRelayTestServer starts a bare websocket server that records the
// handshake headers it received, upgrades, sends a "joined" message, then
// hands the raw *websocket.Conn to onConnected (if given) for the test to
// drive further (e.g. sending a close frame with a specific code).
func startRelayTestServer(t *testing.T, onConnected func(*websocket.Conn)) (wsURL string, gotHeaders *http.Header) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	var captured http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = r.Header.Clone()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
		raw, _ := json.Marshal(joined)
		if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
			return
		}
		if onConnected != nil {
			onConnected(conn)
			return
		}
		// keep the connection open until the test's own cleanup tears down
		// the server, unless onConnected already decided to close it
		time.Sleep(2 * time.Second)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc", &captured
}

func TestSendAwaitingAckOnPlainConnDoesNotWait(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if c.IsBridge() {
		t.Fatal("expected a plain Dial connection to report IsBridge() false")
	}
	start := time.Now()
	ev, ok, err := c.SendAwaitingAck("hello", "")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("SendAwaitingAck: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false on a plain connection (nothing to wait for), got event: %+v", ev)
	}
	if elapsed > time.Second {
		t.Fatalf("expected no ack-wait latency on a plain connection, took %v", elapsed)
	}
}

func TestSendAwaitingAckReturnsSendAckOnBridge(t *testing.T) {
	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var m wire.Msg
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		conn.WriteJSON(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "ext-1", OK: true})
		time.Sleep(2 * time.Second)
	})
	c, err := DialRelay(link+"#secret", RelayDialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("DialRelay: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.SendAwaitingAck("hello", "")
	if err != nil {
		t.Fatalf("SendAwaitingAck: %v", err)
	}
	if !ok || ev.Kind != "sendAck" || ev.ExternalID != "ext-1" || !ev.ActionOK {
		t.Fatalf("expected the sendAck delivered directly, got ok=%v event=%+v", ok, ev)
	}
}

func TestSendAwaitingAckReturnsErrorEventOnRefusal(t *testing.T) {
	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var m wire.Msg
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		conn.WriteJSON(wire.Error{Type: wire.TypeError, Message: "refused", Code: "send_refused", Retryable: false})
		time.Sleep(2 * time.Second)
	})
	c, err := DialRelay(link+"#secret", RelayDialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("DialRelay: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.SendAwaitingAck("hello", "")
	if err != nil {
		t.Fatalf("SendAwaitingAck: %v", err)
	}
	if !ok || ev.Kind != "error" || ev.Code != "send_refused" {
		t.Fatalf("expected the refusal error delivered directly, got ok=%v event=%+v", ok, ev)
	}
}

func TestSendAwaitingAckTimesOutWithoutStealingLaterEvents(t *testing.T) {
	orig := AckWaitTimeout
	AckWaitTimeout = 100 * time.Millisecond
	defer func() { AckWaitTimeout = orig }()

	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var m wire.Msg
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		time.Sleep(300 * time.Millisecond) // arrives after AckWaitTimeout
		conn.WriteJSON(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "ext-late", OK: true})
		time.Sleep(2 * time.Second)
	})
	c, err := DialRelay(link+"#secret", RelayDialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("DialRelay: %v", err)
	}
	defer c.Close()

	_, ok, err := c.SendAwaitingAck("hello", "")
	if err != nil {
		t.Fatalf("SendAwaitingAck: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false: the ack arrives after AckWaitTimeout")
	}

	// The late ack must still land in the general buffer once the claim
	// has been cancelled — never silently lost.
	deadline := time.Now().Add(2 * time.Second)
	var formatted string
	for time.Now().Before(deadline) {
		if hasEvents, _ := c.Peek(); hasEvents {
			formatted, _ = c.Drain()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(formatted, "ext-late") {
		t.Fatalf("expected the late sendAck to still arrive via the normal buffer, got: %q", formatted)
	}
}

func TestClaimNextAckDoesNotStealUnrelatedEvents(t *testing.T) {
	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var m wire.Msg
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		// An unrelated event arrives while the claim is pending — must
		// not be diverted, must still reach Peek/Drain normally.
		conn.WriteJSON(wire.NewPeerJoined("550e8400-e29b-41d4-a716-446655440099", "Alice", ""))
		time.Sleep(50 * time.Millisecond)
		conn.WriteJSON(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "ext-1", OK: true})
		time.Sleep(2 * time.Second)
	})
	c, err := DialRelay(link+"#secret", RelayDialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("DialRelay: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.SendAwaitingAck("hello", "")
	if err != nil {
		t.Fatalf("SendAwaitingAck: %v", err)
	}
	if !ok || ev.Kind != "sendAck" {
		t.Fatalf("expected the sendAck claimed directly, got ok=%v event=%+v", ok, ev)
	}

	deadline := time.Now().Add(2 * time.Second)
	var formatted string
	for time.Now().Before(deadline) {
		if hasEvents, _ := c.Peek(); hasEvents {
			formatted, _ = c.Drain()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(formatted, "Alice") {
		t.Fatalf("expected the unrelated peerJoined to still reach the general buffer, got: %q", formatted)
	}
	if strings.Contains(formatted, "ext-1") {
		t.Fatalf("expected the claimed sendAck NOT to also appear in the general buffer, got: %q", formatted)
	}
}

func TestDeleteMessageAwaitingAckReturnsDeleteAckOnBridge(t *testing.T) {
	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var d wire.Delete
		if err := conn.ReadJSON(&d); err != nil {
			return
		}
		conn.WriteJSON(wire.DeleteAck{Type: wire.TypeDeleteAck, ExternalID: d.ExternalID, OK: true})
		time.Sleep(2 * time.Second)
	})
	c, err := DialRelay(link+"#secret", RelayDialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("DialRelay: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.DeleteMessageAwaitingAck("ext-1")
	if err != nil {
		t.Fatalf("DeleteMessageAwaitingAck: %v", err)
	}
	if !ok || ev.Kind != "deleteAck" || ev.ExternalID != "ext-1" || !ev.ActionOK {
		t.Fatalf("expected the deleteAck delivered directly, got ok=%v event=%+v", ok, ev)
	}
}

func TestDeleteMessageOnPlainConnDoesNotWait(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	start := time.Now()
	ev, ok, err := c.DeleteMessageAwaitingAck("ext-1")
	elapsed := time.Since(start)
	// mcp-hub-server ignores an unrecognized message type, so the write
	// itself succeeds; there's simply nothing to wait for.
	if err != nil {
		t.Fatalf("DeleteMessageAwaitingAck: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false on a plain connection, got event: %+v", ev)
	}
	if elapsed > time.Second {
		t.Fatalf("expected no ack-wait latency on a plain connection, took %v", elapsed)
	}
}

func TestDialRelaySplitsFragmentAndSendsHeaders(t *testing.T) {
	base, headers := startRelayTestServer(t, nil)
	link := base + "#the-link-secret"

	c, err := DialRelay(link, RelayDialOptions{ReconnectSecret: "resume-me", Name: "tester"})
	if err != nil {
		t.Fatalf("DialRelay: %v", err)
	}
	defer c.Close()

	if got := headers.Get("Authorization"); got != "Bearer the-link-secret" {
		t.Fatalf("expected Authorization header with the link secret, got %q", got)
	}
	if got := headers.Get("Reconnect-Secret"); got != "resume-me" {
		t.Fatalf("expected Reconnect-Secret header, got %q", got)
	}
	if got := headers.Get("Agent-Name"); got != "tester" {
		t.Fatalf("expected Agent-Name header, got %q", got)
	}
}

func TestDialRelayOmitsAgentNameHeaderWhenNameEmpty(t *testing.T) {
	base, headers := startRelayTestServer(t, nil)
	link := base + "#the-link-secret"

	c, err := DialRelay(link, RelayDialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("DialRelay: %v", err)
	}
	defer c.Close()

	if got := headers.Get("Agent-Name"); got != "" {
		t.Fatalf("expected no Agent-Name header when Name is empty, got %q", got)
	}
}

func TestDialRelayRejectsLinkWithoutFragment(t *testing.T) {
	if _, err := DialRelay("wss://example.com/relay/join?c=abc", RelayDialOptions{ReconnectSecret: "x"}); err == nil {
		t.Fatal("expected an error for a link with no #-delimited secret")
	}
}

func TestDialRelayRejectsEmptyReconnectSecret(t *testing.T) {
	if _, err := DialRelay("wss://example.com/relay/join?c=abc#secret", RelayDialOptions{}); err == nil {
		t.Fatal("expected an error when reconnectSecret is empty")
	}
}

func TestDisconnectNoteReflectsKnownRelayCloseCode(t *testing.T) {
	base, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(4001, "revoked"))
	})
	link := base + "#the-link-secret"

	c, err := DialRelay(link, RelayDialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("DialRelay: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !c.Connected() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if c.Connected() {
		t.Fatal("expected the connection to be detected as closed")
	}
	if got := c.DisconnectNote(); got != " (revoked — do not reconnect)" {
		t.Fatalf("got %q", got)
	}
}

func TestDisconnectNoteEmptyForOrdinaryClose(t *testing.T) {
	base, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	})
	link := base + "#the-link-secret"

	c, err := DialRelay(link, RelayDialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("DialRelay: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !c.Connected() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := c.DisconnectNote(); got != "" {
		t.Fatalf("expected no note for an ordinary close, got %q", got)
	}
}
