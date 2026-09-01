package httpmcp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/hubsession"
)

// fakeSession is a minimal server.ClientSession for tests that need to put
// a specific MCP session id on the context without a real transport.
type fakeSession struct {
	id string
}

func (f *fakeSession) SessionID() string                                  { return f.id }
func (f *fakeSession) NotificationChannel() chan<- mcp.JSONRPCNotification { return make(chan mcp.JSONRPCNotification, 1) }
func (f *fakeSession) Initialize()                                        {}
func (f *fakeSession) Initialized() bool                                  { return true }

func newTestServer(t *testing.T) (*Server, *server.MCPServer) {
	t.Helper()
	s := NewServer(hubsession.NewManager())
	mcpServer := server.NewMCPServer("test", "0.0.0", server.WithToolCapabilities(false))
	s.Register(mcpServer)
	return s, mcpServer
}

func ctxFor(mcpServer *server.MCPServer, mcpSessionID string) context.Context {
	return mcpServer.WithContext(context.Background(), &fakeSession{id: mcpSessionID})
}

// ctxForSession builds a context carrying mcpSessionID as the active MCP
// client session, using a throwaway MCPServer purely for its WithContext
// helper (server.ClientSessionFromContext's storage key is unexported, so
// this is the only way to construct such a context from outside the
// mcp-go/server package).
func ctxForSession(mcpSessionID string) context.Context {
	throwaway := server.NewMCPServer("test", "0.0.0")
	return throwaway.WithContext(context.Background(), &fakeSession{id: mcpSessionID})
}

// callTool invokes one of Server's tool handler methods directly (they are
// ordinary func(ctx, mcp.CallToolRequest) (*mcp.CallToolResult, error)
// values) — no need to round-trip through JSON-RPC/HandleMessage for a
// unit test.
func callTool(t *testing.T, ctx context.Context, s *Server, handler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error), args map[string]any) string {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = args
	result, err := handler(ctx, req)
	if err != nil {
		t.Fatalf("handler returned an error (not a tool-error result): %v", err)
	}
	if len(result.Content) != 1 {
		t.Fatalf("expected exactly 1 content block, got %d: %+v", len(result.Content), result.Content)
	}
	tc, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected text content, got %T", result.Content[0])
	}
	return tc.Text
}

func TestConnectSendReceiveRoundTrip(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctxA := ctxFor(mcpServer, "mcp-a")
	ctxB := ctxFor(mcpServer, "mcp-b")

	connectA := callTool(t, ctxA, s, s.handleConnect, map[string]any{"name": "Alice"})
	if !strings.Contains(connectA, "sessionId=") {
		t.Fatalf("expected connect result to include a sessionId, got %q", connectA)
	}
	sessionID := s.hubFor("mcp-a").session().ID()

	callTool(t, ctxB, s, s.handleConnect, map[string]any{"sessionId": sessionID, "name": "Bob"})

	callTool(t, ctxA, s, s.handleSend, map[string]any{"text": "hello from Alice"})

	// Deliver is synchronous, so no retry/sleep should be needed, but give
	// it a moment anyway to keep this robust against future changes.
	time.Sleep(10 * time.Millisecond)
	received := callTool(t, ctxB, s, s.handleReceive, map[string]any{})
	if !strings.Contains(received, "hello from Alice") {
		t.Fatalf("expected Bob to receive Alice's message, got %q", received)
	}

	// Sender never gets its own broadcast back.
	selfReceived := callTool(t, ctxA, s, s.handleReceive, map[string]any{})
	if strings.Contains(selfReceived, "hello from Alice") {
		t.Fatalf("sender should not receive its own broadcast, got %q", selfReceived)
	}
}

func TestHubPeersListsOthersNotSelf(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctxA := ctxFor(mcpServer, "mcp-a")
	ctxB := ctxFor(mcpServer, "mcp-b")

	callTool(t, ctxA, s, s.handleConnect, map[string]any{"name": "Alice"})
	sessionID := s.hubFor("mcp-a").session().ID()
	callTool(t, ctxB, s, s.handleConnect, map[string]any{"sessionId": sessionID, "name": "Bob"})

	peers := callTool(t, ctxA, s, s.handlePeers, map[string]any{})
	if !strings.Contains(peers, "Bob") {
		t.Fatalf("expected Alice's hub_peers to mention Bob, got %q", peers)
	}
	if strings.Contains(peers, "Alice") {
		t.Fatalf("expected hub_peers to exclude the caller itself, got %q", peers)
	}
}

func TestHubWaitReturnsDeliveredEvent(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctxA := ctxFor(mcpServer, "mcp-a")
	ctxB := ctxFor(mcpServer, "mcp-b")

	callTool(t, ctxA, s, s.handleConnect, map[string]any{})
	sessionID := s.hubFor("mcp-a").session().ID()
	callTool(t, ctxB, s, s.handleConnect, map[string]any{"sessionId": sessionID})
	// Drain A's own rosterComplete plus the peerJoined it got told about B
	// joining — both landed in A's buffer as a side effect of connecting,
	// before the message this test actually cares about.
	callTool(t, ctxA, s, s.handleReceive, map[string]any{})

	go func() {
		time.Sleep(30 * time.Millisecond)
		callTool(t, ctxB, s, s.handleSend, map[string]any{"text": "async hello"})
	}()

	waited := callTool(t, ctxA, s, s.handleWait, map[string]any{"timeoutSeconds": float64(2)})
	if !strings.Contains(waited, "async hello") {
		t.Fatalf("expected hub_wait to return the delivered message, got %q", waited)
	}
}

func TestHubWaitTimesOutWithNoEvents(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctxA := ctxFor(mcpServer, "mcp-a")
	callTool(t, ctxA, s, s.handleConnect, map[string]any{})
	// Drain the rosterComplete that landed as a side effect of connecting —
	// this test is about there being nothing *further*.
	callTool(t, ctxA, s, s.handleReceive, map[string]any{})

	waited := callTool(t, ctxA, s, s.handleWait, map[string]any{"timeoutSeconds": float64(0.05)})
	if !strings.Contains(waited, "no new events") {
		t.Fatalf("expected a no-new-events result, got %q", waited)
	}
}

func TestToolsErrorWhenNotConnected(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctx := ctxFor(mcpServer, "mcp-a")

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"text": "hi"}
	result, err := s.handleSend(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected hub_send to report an error result when not connected")
	}
}

func TestDisconnectThenReconnectWorks(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctx := ctxFor(mcpServer, "mcp-a")

	callTool(t, ctx, s, s.handleConnect, map[string]any{})
	callTool(t, ctx, s, s.handleDisconnect, map[string]any{})
	callTool(t, ctx, s, s.handleConnect, map[string]any{}) // must not error "already connected"
}
