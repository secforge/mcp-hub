package mcptools

import (
	"context"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

// connectTwo opens two live connections to two different sessions on one
// server, from ONE Hub — which is the arrangement the whole session split
// exists for, and the one nothing could test before it.
func connectTwo(t *testing.T, hub *Hub) (string, string) {
	t.Helper()
	url := startTestServer(t)
	const (
		sessionA = "550e8400-e29b-41d4-a716-446655440000"
		sessionB = "550e8400-e29b-41d4-a716-446655440001"
	)
	ctx := context.Background()
	for name, id := range map[string]string{"alpha": sessionA, "beta": sessionB} {
		req := mcp.CallToolRequest{}
		req.Params.Arguments = map[string]any{"as": name, "link": hubLink(url, id)}
		res, err := hub.handleConnect(ctx, req)
		if err != nil || res.IsError {
			t.Fatalf("connecting %q failed: err=%v result=%+v", name, err, res)
		}
	}
	return sessionA, sessionB
}

// Two connections at once, each with its own peer identity. Before the
// split this could not be expressed at all: a second connect replaced the
// first.
func TestTwoConnectionsAreHeldAtOnce(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	hub := NewHub()
	defer hub.Shutdown()
	connectTwo(t, hub)

	a, err := hub.session("alpha")
	if err != nil {
		t.Fatalf("alpha: %v", err)
	}
	b, err := hub.session("beta")
	if err != nil {
		t.Fatalf("beta: %v", err)
	}
	connA, _ := a.activeConn()
	connB, _ := b.activeConn()
	if connA == nil || connB == nil {
		t.Fatal("expected both connections to be live")
	}
	if connA == connB {
		t.Fatal("expected two distinct connections")
	}
	if !connA.Connected() || !connB.Connected() {
		t.Fatal("expected both to report connected")
	}
}

// The reading position is per connection. A position shared between two
// would be a position for neither: cursors from two servers are opaque,
// so advancing the wrong one skips a message and nothing can notice.
func TestReadingPositionsDoNotBleedBetweenConnections(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	hub := NewHub()
	defer hub.Shutdown()
	connectTwo(t, hub)

	a, _ := hub.session("alpha")
	b, _ := hub.session("beta")

	a.mu.Lock()
	a.lastHandedOverCursor = "cursor-from-alpha"
	a.knownContiguous = true
	a.handedOverAhead = map[string]bool{"ahead-on-alpha": true}
	a.mu.Unlock()

	b.mu.Lock()
	cursor, contiguous, ahead := b.lastHandedOverCursor, b.knownContiguous, b.handedOverAhead
	b.mu.Unlock()

	if cursor != "" {
		t.Fatalf("alpha's position leaked into beta: %q", cursor)
	}
	if contiguous {
		t.Fatal("alpha's contiguity claim leaked into beta")
	}
	if ahead["ahead-on-alpha"] {
		t.Fatal("alpha's ahead set leaked into beta")
	}
	// And the two identities differ, which is what keeps the persisted
	// positions apart across a restart.
	a.mu.Lock()
	b.mu.Lock()
	sameID := a.catchUpID == b.catchUpID
	a.mu.Unlock()
	b.mu.Unlock()
	if sameID {
		t.Fatal("expected the two connections to persist under different identities")
	}
}

// Naming the wrong connection must not be answered by the other one.
func TestToolsAnswerOnlyTheConnectionTheyWereGiven(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	hub := NewHub()
	defer hub.Shutdown()
	connectTwo(t, hub)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"connection": "gamma"}
	res, err := hub.handlePeers(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an unknown connection name to be refused, not served by another")
	}
	got := textOf(res)
	for _, want := range []string{"alpha", "beta"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected the refusal to list what is open (%q missing), got: %s", want, got)
		}
	}
}

// Ending one connection leaves the other alone. A teardown that reached
// across would take down a conversation nobody asked to leave.
func TestDisconnectingOneLeavesTheOther(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	hub := NewHub()
	defer hub.Shutdown()
	connectTwo(t, hub)

	if _, err := hub.handleDisconnect(context.Background(), connReqFor("alpha")); err != nil {
		t.Fatalf("disconnecting alpha: %v", err)
	}
	if _, err := hub.session("alpha"); err == nil {
		t.Fatal("expected alpha's name to be released")
	}
	b, err := hub.session("beta")
	if err != nil {
		t.Fatalf("expected beta to survive alpha's disconnect: %v", err)
	}
	conn, _ := b.activeConn()
	if conn == nil || !conn.Connected() {
		t.Fatal("expected beta to still be connected")
	}
	// And alpha's name is immediately reusable, for the same conversation
	// or another one.
	if _, err := hub.open("alpha"); err != nil {
		t.Fatalf("expected alpha to be reusable: %v", err)
	}
}

// A reply relayed from the inbox names its connection, and a reply that
// names none is refused rather than sent to whichever is open.
func TestARelayedReplyMustNameItsConnection(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	hub := NewHub()
	defer hub.Shutdown()
	connectTwo(t, hub)

	hub.sendFromInbox("no directive line here, so no connection named")
	note := hub.takeAutoReconnectNote()
	if !strings.Contains(note, "NOTHING was sent") {
		t.Fatalf("expected an un-addressed reply to be refused outright, got: %s", note)
	}
	if !strings.Contains(note, "conn=") {
		t.Fatalf("expected the refusal to say how to address it, got: %s", note)
	}

	hub.sendFromInbox("#hub conn=gamma\nmeant for a connection that is not open")
	note = hub.takeAutoReconnectNote()
	if !strings.Contains(note, "NOTHING was sent") {
		t.Fatalf("expected an unknown connection to be refused, got: %s", note)
	}
}
