package mcptools

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/wire"
	"github.com/secforge/mcp-hub/internal/wsserver"
)

func startTestServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(wsserver.NewHandler())
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func TestConnectSendReceiveDisconnect(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"host": url, "sessionId": sessionID}
	res, err := hubA.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}

	hubB := NewHub()
	res, err = hubB.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "hello from b"}
	if res, err := hubB.handleSend(ctx, sendReq); err != nil || res.IsError {
		t.Fatalf("send failed: err=%v result=%+v", err, res)
	}

	recvReq := mcp.CallToolRequest{}
	var got string
	deadlinePoll(t, func() bool {
		res, err := hubA.handleReceive(ctx, recvReq)
		if err != nil {
			t.Fatalf("receive failed: %v", err)
		}
		got = textOf(res)
		return strings.Contains(got, "hello from b")
	})

	if !strings.Contains(got, "[HUB MESSAGE") {
		t.Fatalf("expected untrusted wrapper, got: %s", got)
	}

	discReq := mcp.CallToolRequest{}
	if res, err := hubA.handleDisconnect(ctx, discReq); err != nil || res.IsError {
		t.Fatalf("disconnect failed: err=%v result=%+v", err, res)
	}
	if res, err := hubA.handleSend(ctx, sendReq); err != nil || !res.IsError {
		t.Fatal("expected send after disconnect to error")
	}

	hubB.handleDisconnect(ctx, discReq)
}

func TestPeersToolReturnsRoster(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"host": url, "sessionId": sessionID}
	if res, err := hubA.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	defer hubA.handleDisconnect(ctx, mcp.CallToolRequest{})

	res, err := hubA.handlePeers(ctx, mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("peers failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "no other peers") {
		t.Fatalf("expected an empty-roster message, got: %s", textOf(res))
	}

	hubB := NewHub()
	res, err = hubB.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}
	defer hubB.handleDisconnect(ctx, mcp.CallToolRequest{})

	var peersText string
	deadlinePoll(t, func() bool {
		res, err := hubA.handlePeers(ctx, mcp.CallToolRequest{})
		if err != nil {
			t.Fatalf("peers failed: %v", err)
		}
		peersText = textOf(res)
		return strings.Contains(peersText, hubB.conn.PeerID())
	})
	_ = peersText
}

func TestConnectResultStatesExpectedPeerCountAndRosterNotification(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"host": url, "sessionId": sessionID}
	res, err := hubA.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	defer hubA.handleDisconnect(ctx, mcp.CallToolRequest{})
	if !strings.Contains(textOf(res), "No other peers are in this session yet") {
		t.Fatalf("expected a-no-existing-peers note, got: %s", textOf(res))
	}

	hubB := NewHub()
	res, err = hubB.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}
	defer hubB.handleDisconnect(ctx, mcp.CallToolRequest{})
	if !strings.Contains(textOf(res), "1 other peer(s) already in this session") {
		t.Fatalf("expected b-1-existing-peer note, got: %s", textOf(res))
	}
	if !strings.Contains(textOf(res), "roster complete") {
		t.Fatalf("expected a mention of the roster-complete notification, got: %s", textOf(res))
	}
}

func TestPeersToolNotesWhenStillCatchingUp(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"host": url, "sessionId": sessionID}
	if res, err := hubA.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	defer hubA.handleDisconnect(ctx, mcp.CallToolRequest{})

	hubB := NewHub()
	if res, err := hubB.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}
	defer hubB.handleDisconnect(ctx, mcp.CallToolRequest{})

	// hubA immediately asks for peers, likely before the roster catch-up
	// event (about b) has arrived over the (real, but very fast) network -
	// exercise this deterministically by checking RosterComplete directly
	// rather than racing a real timing window.
	if hubA.conn.RosterComplete() {
		t.Skip("roster caught up before we could observe the in-progress state (fast local network) — not flaky, just nothing to assert here")
	}
	res, err := hubA.handlePeers(ctx, mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("peers failed: %v", err)
	}
	if !strings.Contains(textOf(res), "still catching up") {
		t.Fatalf("expected a catching-up note, got: %s", textOf(res))
	}
}

func TestConnectResultNotesOutdatedClientVersion(t *testing.T) {
	// A raw fake server (not wsserver.NewHandler) that reports a newer
	// serverVersion than this client build understands.
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0)
		joined.ServerVersion = wire.ProtocolVersion + 1
		conn.WriteJSON(joined)
		// Keep the connection open briefly so the client's background read
		// loop doesn't immediately see a disconnect.
		time.Sleep(200 * time.Millisecond)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx := context.Background()
	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"host": url, "sessionId": "6ba7b810-9dad-11d1-80b4-00c04fd430c8"}
	res, err := hub.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	if !strings.Contains(textOf(res), "update mcp-hub-client") {
		t.Fatalf("expected an update-the-client note, got: %s", textOf(res))
	}
}

func TestPeersToolErrorsWhenNotConnected(t *testing.T) {
	hub := NewHub()
	res, err := hub.handlePeers(context.Background(), mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error when not connected")
	}
}

func TestPrivateSendOnlyReachesTarget(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"host": url, "sessionId": sessionID}
	res, err := hubA.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	peerA := hubA.conn.PeerID()

	hubB := NewHub()
	if res, err := hubB.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}

	hubC := NewHub()
	if res, err := hubC.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect c failed: err=%v result=%+v", err, res)
	}

	// drain each hub's connect-time peerJoined noise so the poll below only
	// sees the private message we're about to send.
	hubA.handleReceive(ctx, mcp.CallToolRequest{})
	hubC.handleReceive(ctx, mcp.CallToolRequest{})

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "just for you", "to": peerA}
	if res, err := hubB.handleSend(ctx, sendReq); err != nil || res.IsError {
		t.Fatalf("private send failed: err=%v result=%+v", err, res)
	}

	var got string
	deadlinePoll(t, func() bool {
		res, err := hubA.handleReceive(ctx, mcp.CallToolRequest{})
		if err != nil {
			t.Fatalf("receive failed: %v", err)
		}
		got = textOf(res)
		return strings.Contains(got, "just for you")
	})
	if !strings.Contains(got, "[HUB PRIVATE MESSAGE") {
		t.Fatalf("expected private wrapper, got: %s", got)
	}

	// c must not have received it.
	res, err = hubC.handleReceive(ctx, mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("hubC receive failed: %v", err)
	}
	if strings.Contains(textOf(res), "just for you") {
		t.Fatalf("hubC should not have received the private message: %s", textOf(res))
	}

	hubA.handleDisconnect(ctx, mcp.CallToolRequest{})
	hubB.handleDisconnect(ctx, mcp.CallToolRequest{})
	hubC.handleDisconnect(ctx, mcp.CallToolRequest{})
}

func TestPrivateSendToUnknownPeerReturnsErrorEvent(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"host": url, "sessionId": sessionID}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{
		"text": "hello?",
		"to":   "00000000-0000-0000-0000-000000000000",
	}
	if res, err := hub.handleSend(ctx, sendReq); err != nil || res.IsError {
		t.Fatalf("expected hub_send itself to succeed (fire-and-forget), got: err=%v result=%+v", err, res)
	}

	var got string
	deadlinePoll(t, func() bool {
		res, err := hub.handleReceive(ctx, mcp.CallToolRequest{})
		if err != nil {
			t.Fatalf("receive failed: %v", err)
		}
		got = textOf(res)
		return strings.Contains(got, "[HUB ERROR]")
	})
	_ = got
}

func TestConnectWithoutSessionIDGeneratesOneAndShowsIt(t *testing.T) {
	url := startTestServer(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"host": url}
	res, err := hub.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	text := textOf(res)
	if !strings.Contains(text, "new session") {
		t.Fatalf("expected result to flag this as a newly generated session, got: %s", text)
	}
	if !strings.Contains(strings.ToLower(text), "share") {
		t.Fatalf("expected result to instruct sharing the sessionId, got: %s", text)
	}

	// Extract the generated sessionId (the one after "new session:", not the
	// peerId that appears earlier in the text) and confirm it's well-formed
	// by using it to open a second connection to the same session.
	const marker = "new session: "
	idx := strings.Index(text, marker)
	if idx == -1 {
		t.Fatalf("could not find %q in result text: %s", marker, text)
	}
	sessionID := strings.Fields(text[idx+len(marker):])[0]
	if !wire.IsValidID(sessionID) {
		t.Fatalf("extracted sessionId %q is not a valid UUID", sessionID)
	}

	hub2 := NewHub()
	connReq2 := mcp.CallToolRequest{}
	connReq2.Params.Arguments = map[string]any{"host": url, "sessionId": sessionID}
	res2, err := hub2.handleConnect(ctx, connReq2)
	if err != nil || res2.IsError {
		t.Fatalf("second connect using the generated sessionId failed: err=%v result=%+v", err, res2)
	}
	hub2.handleDisconnect(ctx, mcp.CallToolRequest{})

	want := "Connect to the hub at " + url + " with sessionId " + sessionID + ", then wait for messages."
	if !strings.Contains(text, want) {
		t.Fatalf("expected a copy-pasteable invite string %q in result, got: %s", want, text)
	}
}

func TestConnectWithSessionIDDoesNotClaimItsNew(t *testing.T) {
	url := startTestServer(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{
		"host":      url,
		"sessionId": "550e8400-e29b-41d4-a716-446655440000",
	}
	res, err := hub.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	if strings.Contains(textOf(res), "new session") {
		t.Fatalf("should not claim a new session was generated when one was given: %s", textOf(res))
	}

	want := "Connect to the hub at " + url + " with sessionId 550e8400-e29b-41d4-a716-446655440000, then wait for messages."
	if !strings.Contains(textOf(res), want) {
		t.Fatalf("expected a copy-pasteable invite string %q in result, got: %s", want, textOf(res))
	}
}

func TestConnectTwiceErrors(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()
	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"host": url, "sessionId": sessionID}

	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("first connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	res, err := hub.handleConnect(ctx, connReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected second connect to report an error")
	}
}

func deadlinePoll(t *testing.T, check func() bool) {
	t.Helper()
	for i := 0; i < 50; i++ {
		if check() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition never became true")
}

func textOf(res *mcp.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(mcp.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}
