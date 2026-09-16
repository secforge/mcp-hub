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

// Which connection a reply answers is settled by the inbox it arrived
// on, so the name cannot be mistyped into another conversation. A conn=
// directive is an assertion about that, and a wrong one is refused rather
// than honoured either way.
func TestARelayedReplyBelongsToTheInboxItArrivedOn(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	hub := NewHub()
	defer hub.Shutdown()
	connectTwo(t, hub)

	alpha, err := hub.session("alpha")
	if err != nil {
		t.Fatalf("alpha: %v", err)
	}

	// Naming its own connection is redundant but true, and is relayed.
	alpha.sendFromInbox("#hub conn=alpha\nthis one is for alpha")
	if note := hub.takeAutoReconnectNote(); strings.Contains(note, "NOTHING was sent") {
		t.Fatalf("expected a reply naming its own connection to go through, got: %s", note)
	}

	// Naming another is refused: the sender believed it was answering
	// something else, and both readings cannot be honoured.
	alpha.sendFromInbox("#hub conn=beta\nthis one thinks it is for beta")
	note := hub.takeAutoReconnectNote()
	if !strings.Contains(note, "NOTHING was sent") {
		t.Fatalf("expected a mismatched conn= to be refused, got: %s", note)
	}
	for _, want := range []string{"alpha", "beta"} {
		if !strings.Contains(note, want) {
			t.Fatalf("expected the refusal to name both sides (%q missing), got: %s", want, note)
		}
	}
}

// Each connection is attributed and answered under its own name, which is
// the whole of the routing: the address a reply returns to and the name
// the reader sees have to describe the same conversation.
func TestEachConnectionHasItsOwnReturnAddress(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	hub := NewHub()
	defer hub.Shutdown()
	connectTwo(t, hub)

	alpha, _ := hub.session("alpha")
	beta, _ := hub.session("beta")
	// Without a harness there is nothing to bind to, and that is a state
	// to report rather than a failure: the addresses are equal only
	// because both are absent.
	if !harnessAvailableForTest() {
		if alpha.inbox != nil || beta.inbox != nil {
			t.Fatal("expected no inbox without a harness messaging socket")
		}
		return
	}
	if alpha.inbox == nil || beta.inbox == nil {
		t.Fatal("expected each connection to bind its own inbox")
	}
	if alpha.inbox.Address() == beta.inbox.Address() {
		t.Fatalf("expected distinct return addresses, both are %q", alpha.inbox.Address())
	}
}

// Confirming a cursor one connection delivered against a DIFFERENT
// connection would move that connection's read position to a place
// derived from another conversation — past whatever sits between,
// permanently and silently. Cursors are opaque, so nothing about the
// string reveals this; who delivered it is what makes it detectable.
func TestACursorFromAnotherConnectionIsRefused(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	hub := NewHub()
	defer hub.Shutdown()
	connectTwo(t, hub)

	alpha, _ := hub.session("alpha")
	beta, _ := hub.session("beta")
	alpha.noteDelivered("cursor-alpha-42")

	betaConn, _ := beta.activeConn()
	_, err := beta.confirmCursor(betaConn, "cursor-alpha-42")
	if err == nil {
		t.Fatal("expected a cursor delivered on alpha to be refused against beta")
	}
	for _, want := range []string{"alpha", "beta"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected the refusal to name both connections (%q missing): %v", want, err)
		}
	}

	// A cursor no open connection claims is NOT refused: it may be one
	// delivered before a restart, which is a legitimate thing to confirm.
	// Refusing on suspicion rather than proof would block that forever.
	if other := hub.cursorBelongsElsewhere(beta, "cursor-from-a-previous-run"); other != "" {
		t.Fatalf("expected an unclaimed cursor to belong nowhere, got %q", other)
	}
	// And a connection confirming its own is never refused by this check.
	if other := hub.cursorBelongsElsewhere(alpha, "cursor-alpha-42"); other != "" {
		t.Fatalf("expected alpha's own cursor to be fine on alpha, got %q", other)
	}
}

// The memory is bounded: the mistake it catches is made within a turn or
// two of seeing the message, so an unbounded set would grow for the life
// of the process to catch nothing extra.
func TestTheDeliveredCursorMemoryIsBounded(t *testing.T) {
	s := &session{name: "x"}
	for i := 0; i < maxRecentCursors+50; i++ {
		s.noteDelivered(strings.Repeat("c", 1) + string(rune('a'+i%26)) + string(rune(i)))
	}
	s.mu.Lock()
	n, order := len(s.delivered), len(s.deliveredOrder)
	s.mu.Unlock()
	if n > maxRecentCursors || order > maxRecentCursors {
		t.Fatalf("expected the set to stay bounded, got %d/%d", n, order)
	}
}

// One window for the process, not one per connection. The window bounds
// what a reader can absorb, and a reader is a process: eight connections
// with a window each is eight times the ceiling that was set by watching
// one receiver die.
func TestConnectionsShareOneDeliveryWindow(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	hub := NewHub()
	defer hub.Shutdown()
	connectTwo(t, hub)

	alpha, _ := hub.session("alpha")
	beta, _ := hub.session("beta")
	connA, _ := alpha.activeConn()
	connB, _ := beta.activeConn()

	// Spending on one is visible to the other, which is the whole point
	// and also the accepted cost.
	connA.ChargeDelivered("cursor-a", 1000)
	if !connB.SameDeliveryBudget(connA) {
		t.Fatal("expected both connections to spend against one window")
	}
	if hub.budget == nil {
		t.Fatal("expected the process to own the window")
	}
}

// Backlog walks are serialized: two at once produce a stream in which
// neither conversation reads as a conversation.
func TestOnlyOneBacklogWalkRunsAtATime(t *testing.T) {
	h := NewHub()
	// Taking the slot stands in for a run in progress.
	h.catchUpSlot <- struct{}{}
	select {
	case h.catchUpSlot <- struct{}{}:
		t.Fatal("expected the second walk to find the slot taken")
	default:
	}
	<-h.catchUpSlot
	select {
	case h.catchUpSlot <- struct{}{}:
	default:
		t.Fatal("expected the slot to be free once the first run finished")
	}
}
