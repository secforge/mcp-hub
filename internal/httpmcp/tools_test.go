package httpmcp

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/hubsession"
	"github.com/secforge/mcp-hub/internal/wire"
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

func TestSendWithImageDataDeliversImageContentBlock(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctxA := ctxFor(mcpServer, "mcp-a")
	ctxB := ctxFor(mcpServer, "mcp-b")

	connectA := callTool(t, ctxA, s, s.handleConnect, map[string]any{"name": "Alice"})
	if !strings.Contains(connectA, "sessionId=") {
		t.Fatalf("expected connect result to include a sessionId, got %q", connectA)
	}
	sessionID := s.hubFor("mcp-a").session().ID()
	callTool(t, ctxB, s, s.handleConnect, map[string]any{"sessionId": sessionID, "name": "Bob"})

	rawImage := []byte("not a real png, just test bytes")
	imageData := base64.StdEncoding.EncodeToString(rawImage)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{
		"text": "look at this", "imageData": imageData, "imageContentType": "image/png",
	}
	sendRes, err := s.handleSend(ctxA, req)
	if err != nil || sendRes.IsError {
		t.Fatalf("send failed: err=%v result=%+v", err, sendRes)
	}

	time.Sleep(10 * time.Millisecond)
	recvRes, err := s.handleReceive(ctxB, mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("receive failed: %v", err)
	}

	var gotImage *mcp.ImageContent
	var gotText string
	for _, c := range recvRes.Content {
		switch v := c.(type) {
		case mcp.ImageContent:
			gotImage = &v
		case mcp.TextContent:
			gotText += v.Text
		}
	}
	if !strings.Contains(gotText, "look at this") {
		t.Fatalf("expected text content to include the message, got %q", gotText)
	}
	if gotImage == nil {
		t.Fatalf("expected an image content block, got %+v", recvRes.Content)
	}
	if gotImage.MIMEType != "image/png" || gotImage.Data != imageData {
		t.Fatalf("unexpected image content block: %+v", gotImage)
	}
}

func TestSendWithFileDataDeliversEmbeddedResourceBlock(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctxA := ctxFor(mcpServer, "mcp-a")
	ctxB := ctxFor(mcpServer, "mcp-b")

	callTool(t, ctxA, s, s.handleConnect, map[string]any{"name": "Alice"})
	sessionID := s.hubFor("mcp-a").session().ID()
	callTool(t, ctxB, s, s.handleConnect, map[string]any{"sessionId": sessionID, "name": "Bob"})

	rawFile := []byte("%PDF-1.4 not a real pdf, just test bytes")
	fileData := base64.StdEncoding.EncodeToString(rawFile)

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{
		"text": "here's the report", "fileData": fileData,
		"fileContentType": "application/pdf", "fileName": "report.pdf",
	}
	sendRes, err := s.handleSend(ctxA, req)
	if err != nil || sendRes.IsError {
		t.Fatalf("send failed: err=%v result=%+v", err, sendRes)
	}

	time.Sleep(10 * time.Millisecond)
	recvRes, err := s.handleReceive(ctxB, mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("receive failed: %v", err)
	}

	var gotResource *mcp.EmbeddedResource
	var gotText string
	for _, c := range recvRes.Content {
		switch v := c.(type) {
		case mcp.EmbeddedResource:
			gotResource = &v
		case mcp.TextContent:
			gotText += v.Text
		case mcp.ImageContent:
			t.Fatalf("expected no image content block for a non-image attachment, got %+v", v)
		}
	}
	if !strings.Contains(gotText, "here's the report") {
		t.Fatalf("expected text content to include the message, got %q", gotText)
	}
	if gotResource == nil {
		t.Fatalf("expected an embedded resource block, got %+v", recvRes.Content)
	}
	blob, ok := gotResource.Resource.(mcp.BlobResourceContents)
	if !ok {
		t.Fatalf("expected BlobResourceContents, got %T", gotResource.Resource)
	}
	if blob.MIMEType != "application/pdf" || blob.Blob != fileData {
		t.Fatalf("unexpected embedded resource: %+v", blob)
	}
}

func TestSendRejectsImageDataOverSizeLimit(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctx := ctxFor(mcpServer, "mcp-a")
	callTool(t, ctx, s, s.handleConnect, map[string]any{"name": "Alice"})

	oversized := base64.StdEncoding.EncodeToString(make([]byte, wire.MaxAttachmentRawBytes+1))
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{
		"text": "too big", "imageData": oversized, "imageContentType": "image/png",
	}
	res, err := s.handleSend(ctx, req)
	if err != nil {
		t.Fatalf("handleSend returned unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error result for oversized imageData")
	}
}

func TestSendRejectsUnsupportedImageContentType(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctx := ctxFor(mcpServer, "mcp-a")
	callTool(t, ctx, s, s.handleConnect, map[string]any{"name": "Alice"})

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{
		"text": "nope", "imageData": base64.StdEncoding.EncodeToString([]byte("x")), "imageContentType": "application/pdf",
	}
	res, err := s.handleSend(ctx, req)
	if err != nil {
		t.Fatalf("handleSend returned unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error result for an unsupported content type")
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

func TestSendWithMentionsIsRelayedToOtherPeer(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctxA := ctxFor(mcpServer, "mcp-a")
	ctxB := ctxFor(mcpServer, "mcp-b")

	callTool(t, ctxA, s, s.handleConnect, map[string]any{"name": "Alice"})
	sessionID := s.hubFor("mcp-a").session().ID()
	callTool(t, ctxB, s, s.handleConnect, map[string]any{"sessionId": sessionID, "name": "Bob"})

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{
		// Exactly one of id/peerId/name per entry — see parseMentions and
		// chat-relay's own confirmed rule (never two).
		"text":     "hi @Bob",
		"mentions": []any{map[string]any{"id": "dir-1"}},
	}
	if res, err := s.handleSend(ctxA, req); err != nil || res.IsError {
		t.Fatalf("send failed: err=%v result=%+v", err, res)
	}

	time.Sleep(10 * time.Millisecond)
	received := callTool(t, ctxB, s, s.handleReceive, map[string]any{})
	if !strings.Contains(received, "mentions=dir-1") {
		t.Fatalf("expected the relayed mention to render, got %q", received)
	}
}

func TestSendRejectsMentionWithoutExactlyOneIdentifier(t *testing.T) {
	s, mcpServer := newTestServer(t)
	ctxA := ctxFor(mcpServer, "mcp-a")
	callTool(t, ctxA, s, s.handleConnect, map[string]any{"name": "Alice"})

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{
		"text":     "hi",
		"mentions": []any{map[string]any{}},
	}
	res, err := s.handleSend(ctxA, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected a client-side validation error, got: %+v", res)
	}
	tc, ok := res.Content[0].(mcp.TextContent)
	if !ok || !strings.Contains(tc.Text, "exactly one") {
		t.Fatalf("expected an exactly-one error message, got: %+v", res.Content)
	}
}
