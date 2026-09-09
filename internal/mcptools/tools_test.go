package mcptools

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/wire"
	"github.com/secforge/mcp-hub/internal/wsserver"
)

func startTestServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(wsserver.NewHandler())
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// hubLink builds the link for one session on a test server. wsserver
// addresses a session by path segment, and the fragment is the credential
// hub_connect requires — a URL fragment is never transmitted, so wsserver
// never sees it.
func hubLink(base, sessionID string) string {
	return hubAddress(base, sessionID) + "#" + sessionID
}

// hubAddress is the dialable part of the link, without its credential.
// Note connstore keys an entry by the WHOLE link (see targetForLink), so
// this is for building a link, not for looking one up.
func hubAddress(base, sessionID string) string {
	return base + "/" + sessionID
}

// connectAs calls hub.handleConnect(ctx, connReq) with connstore's project
// scope (see connstore.CurrentProject) pinned to project for the duration
// of this call — used any time a test connects more than one *Hub to the
// exact same link: without a distinct project per hub they all resolve
// the same stored secret (see handleConnect), and because Session.Join
// supersedes a still-live identity rather than assigning a fresh one on
// collision (see hubsession.SupersededCloseCode), each connect would
// silently kill the previous one — the real multi-agent collision this
// project scoping prevents, simulated by several *Hub instances in one
// test process instead of separate real ones. t.Setenv
// auto-restores the previous value at test end and is safe here because
// every call site connects sequentially, never concurrently.
func connectAs(t *testing.T, ctx context.Context, hub *Hub, connReq mcp.CallToolRequest, project string) (*mcp.CallToolResult, error) {
	t.Helper()
	t.Setenv("MCP_HUB_PROJECT_DIR", project)
	return hub.handleConnect(ctx, connReq)
}

// fakeClientSession is a minimal server.SessionWithClientInfo, used to put
// a specific clientInfo.name into a handler's context the same way a real
// MCP `initialize` handshake would, so tests can exercise Codex-detection
// without a real client connection.
type fakeClientSession struct {
	id         string
	clientInfo mcp.Implementation
}

func (f *fakeClientSession) SessionID() string                                   { return f.id }
func (f *fakeClientSession) NotificationChannel() chan<- mcp.JSONRPCNotification { return nil }
func (f *fakeClientSession) Initialize()                                         {}
func (f *fakeClientSession) Initialized() bool                                   { return true }
func (f *fakeClientSession) GetClientInfo() mcp.Implementation                   { return f.clientInfo }
func (f *fakeClientSession) SetClientInfo(info mcp.Implementation)               { f.clientInfo = info }
func (f *fakeClientSession) GetClientCapabilities() mcp.ClientCapabilities {
	return mcp.ClientCapabilities{}
}
func (f *fakeClientSession) SetClientCapabilities(mcp.ClientCapabilities) {}

// ctxWithClientName returns a context carrying a fake client session
// reporting the given clientInfo.name, as server.ClientSessionFromContext
// would see it for a real connection.
func ctxWithClientName(name string) context.Context {
	srv := server.NewMCPServer("test", "0.0.0")
	return srv.WithContext(context.Background(), &fakeClientSession{
		id:         "fake-session",
		clientInfo: mcp.Implementation{Name: name},
	})
}

func TestConnectResultMentionsFollowModeAndMonitorGuidance(t *testing.T) {
	url := startTestServer(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, "550e8400-e29b-41d4-a716-446655440000")}
	res, err := hub.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	text := textOf(res)
	if !strings.Contains(text, "--follow") {
		t.Fatalf("expected the result to mention --follow mode, got: %s", text)
	}
	if !strings.Contains(text, "Monitor") {
		t.Fatalf("expected the result to mention Monitor-style tools as the preferred pairing for --follow, got: %s", text)
	}
	_, w := hub.activeConn()
	if !strings.Contains(text, w.WaitCommand()) || !strings.Contains(text, w.WaitFollowCommand()) {
		t.Fatalf("expected the result to include both the once and follow commands, got: %s", text)
	}
}

func TestConnectResultTellsCodexToUseHubWait(t *testing.T) {
	url := startTestServer(t)
	ctx := ctxWithClientName("codex")

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, "550e8400-e29b-41d4-a716-446655440000")}
	res, err := hub.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	text := textOf(res)
	if !strings.Contains(text, "Persistent monitoring is active") {
		t.Fatalf("expected the persistent-monitoring checklist, got: %s", text)
	}
	if !strings.Contains(text, "call the foreground hub_wait tool") {
		t.Fatalf("expected instructions to call hub_wait, got: %s", text)
	}
	if !strings.Contains(text, "Never return a final response merely because one waiter call ended") {
		t.Fatalf("expected the never-stop-early instruction, got: %s", text)
	}
	if !strings.Contains(text, "reconnect via hub_connect with the same link") {
		t.Fatalf("expected the reconnect-on-disconnect instruction, got: %s", text)
	}
	// The generic "background one of these two modes" framing, and the CLI
	// wait-binary guidance meant for backgroundable harnesses, must never
	// appear at all for Codex — not even followed by a correction — since
	// presenting it and then walking it back is exactly the confusing
	// sequence this is meant to avoid.
	if strings.Contains(text, "Two modes") || strings.Contains(text, "backgrounded via a tool") {
		t.Fatalf("expected the generic two-mode framing to be entirely absent for Codex, got: %s", text)
	}
	if strings.Contains(text, "Monitor/background-streaming tool directly") {
		t.Fatalf("expected the generic Monitor guidance to be replaced, not appended, got: %s", text)
	}
	if !strings.Contains(text, "A timeout with no event is normal") {
		t.Fatalf("expected the timeout-is-normal note, got: %s", text)
	}
	// Identity is established and recorded by the client itself, so step 2
	// has no missing prerequisite (Codex included) and the result says as
	// much rather than asking the model to hold anything.
	if !strings.Contains(text, "the identity the server assigned has been recorded") {
		t.Fatalf("expected a note that the identity was recorded automatically, got: %s", text)
	}
}


func TestConnectResultDoesNotWarnNonCodexClients(t *testing.T) {
	url := startTestServer(t)
	ctx := ctxWithClientName("claude-code")

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, "550e8400-e29b-41d4-a716-446655440000")}
	res, err := hub.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	text := textOf(res)
	if strings.Contains(text, "WARNING") {
		t.Fatalf("expected no Codex warning for a non-Codex client, got: %s", text)
	}
}

func TestSendWithFormatReachesOtherPeer(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := connectAs(t, ctx, hubA, connReq, "hubA"); err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	defer hubA.handleDisconnect(ctx, mcp.CallToolRequest{})

	hubB := NewHub()
	if res, err := connectAs(t, ctx, hubB, connReq, "hubB"); err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}
	defer hubB.handleDisconnect(ctx, mcp.CallToolRequest{})

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "<b>hi</b>", "format": "html"}
	if res, err := hubB.handleSend(ctx, sendReq); err != nil || res.IsError {
		t.Fatalf("send failed: err=%v result=%+v", err, res)
	}

	var gotFormat string
	deadlinePoll(t, func() bool {
		conn, _ := hubA.activeConn()
		events, _ := conn.DrainEvents()
		for _, ev := range events {
			if ev.Kind == "msg" && strings.Contains(ev.Text, "hi") {
				gotFormat = ev.Format
				return true
			}
		}
		return false
	})
	if gotFormat != "html" {
		t.Fatalf("expected the received msg event to carry format=html, got %q", gotFormat)
	}
}

func TestSendWithReplyToReachesOtherPeer(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := connectAs(t, ctx, hubA, connReq, "hubA"); err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	defer hubA.handleDisconnect(ctx, mcp.CallToolRequest{})

	hubB := NewHub()
	if res, err := connectAs(t, ctx, hubB, connReq, "hubB"); err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}
	defer hubB.handleDisconnect(ctx, mcp.CallToolRequest{})

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "reply text", "replyTo": "ext-orig"}
	if res, err := hubB.handleSend(ctx, sendReq); err != nil || res.IsError {
		t.Fatalf("send failed: err=%v result=%+v", err, res)
	}

	var gotReplyTo string
	deadlinePoll(t, func() bool {
		conn, _ := hubA.activeConn()
		events, _ := conn.DrainEvents()
		for _, ev := range events {
			if ev.Kind == "msg" && strings.Contains(ev.Text, "reply text") {
				gotReplyTo = ev.ReplyTo
				return true
			}
		}
		return false
	})
	if gotReplyTo != "ext-orig" {
		t.Fatalf("expected the received msg event to carry replyTo=ext-orig, got %q", gotReplyTo)
	}
}

func TestSendWithFilePathSendsAndSavesNonImageAttachment(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := connectAs(t, ctx, hubA, connReq, "hubA"); err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	defer hubA.handleDisconnect(ctx, mcp.CallToolRequest{})

	hubB := NewHub()
	if res, err := connectAs(t, ctx, hubB, connReq, "hubB"); err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}
	defer hubB.handleDisconnect(ctx, mcp.CallToolRequest{})

	filePath := filepath.Join(t.TempDir(), "report.pdf")
	rawFile := []byte("%PDF-1.4 not a real pdf, just test bytes")
	if err := os.WriteFile(filePath, rawFile, 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "here's the report", "filePath": filePath}
	if res, err := hubB.handleSend(ctx, sendReq); err != nil || res.IsError {
		t.Fatalf("send failed: err=%v result=%+v", err, res)
	}

	var res *mcp.CallToolResult
	deadlinePoll(t, func() bool {
		var err error
		res, err = hubA.handleReceive(ctx, mcp.CallToolRequest{})
		if err != nil {
			t.Fatalf("receive failed: %v", err)
		}
		return strings.Contains(textOf(res), "here's the report")
	})

	text := textOf(res)
	if len(imagesOf(res)) != 0 {
		t.Fatalf("expected no inline image content blocks for a non-image attachment, got %+v", res.Content)
	}
	savedPath := extractSavedImagePath(t, text)
	got, err := os.ReadFile(savedPath)
	if err != nil {
		t.Fatalf("could not read saved attachment at %q: %v", savedPath, err)
	}
	if string(got) != string(rawFile) {
		t.Fatalf("saved attachment content mismatch: got %q, want %q", got, rawFile)
	}
	if filepath.Base(savedPath) != "1-report.pdf" {
		t.Fatalf("expected the saved filename to preserve the original name, got %q", filepath.Base(savedPath))
	}
}

func TestSendRejectsBothImagePathAndFilePath(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{
		"text": "hi", "imagePath": "/tmp/whatever.png", "filePath": "/tmp/whatever.bin",
	}
	res, err := hub.handleSend(ctx, sendReq)
	if err != nil {
		t.Fatalf("handleSend returned unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error result when both imagePath and filePath are given")
	}
}

func TestSendWithImagePathSavesReceivedImageLocallyAndReturnsPath(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := connectAs(t, ctx, hubA, connReq, "hubA"); err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	defer hubA.handleDisconnect(ctx, mcp.CallToolRequest{})

	hubB := NewHub()
	if res, err := connectAs(t, ctx, hubB, connReq, "hubB"); err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}
	defer hubB.handleDisconnect(ctx, mcp.CallToolRequest{})

	imgPath := filepath.Join(t.TempDir(), "pic.png")
	rawImage := []byte("not a real png, just test bytes")
	if err := os.WriteFile(imgPath, rawImage, 0o600); err != nil {
		t.Fatalf("write test image: %v", err)
	}

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "look at this", "imagePath": imgPath}
	if res, err := hubB.handleSend(ctx, sendReq); err != nil || res.IsError {
		t.Fatalf("send failed: err=%v result=%+v", err, res)
	}

	var res *mcp.CallToolResult
	deadlinePoll(t, func() bool {
		var err error
		res, err = hubA.handleReceive(ctx, mcp.CallToolRequest{})
		if err != nil {
			t.Fatalf("receive failed: %v", err)
		}
		return strings.Contains(textOf(res), "look at this")
	})

	text := textOf(res)
	if len(imagesOf(res)) != 0 {
		t.Fatalf("expected no inline image content blocks (mcp-hub-client saves to disk instead), got %+v", res.Content)
	}
	savedPath := extractSavedImagePath(t, text)
	got, err := os.ReadFile(savedPath)
	if err != nil {
		t.Fatalf("could not read saved image at %q: %v", savedPath, err)
	}
	if string(got) != string(rawImage) {
		t.Fatalf("saved image content mismatch: got %q, want %q", got, rawImage)
	}
	if filepath.Ext(savedPath) != ".png" {
		t.Fatalf("expected saved image to have a .png extension, got %q", savedPath)
	}

	// The saved file is cleaned up once the receiving connection disconnects.
	if _, err := hubA.handleDisconnect(context.Background(), mcp.CallToolRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(savedPath); !os.IsNotExist(err) {
		t.Fatalf("expected saved image to be removed after disconnect, stat err=%v", err)
	}
}

// extractSavedImagePath pulls the "saved to <path> (" filesystem path out
// of Hub.saveReceivedAttachments' annotation text.
func extractSavedImagePath(t *testing.T, text string) string {
	t.Helper()
	const marker = "saved to "
	i := strings.Index(text, marker)
	if i == -1 {
		t.Fatalf("expected a %q annotation in received text, got: %s", marker, text)
	}
	rest := text[i+len(marker):]
	end := strings.Index(rest, " (")
	if end == -1 {
		t.Fatalf("could not find end of saved path in: %s", rest)
	}
	return rest[:end]
}

func TestSendRejectsImagePathOverSizeLimit(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	imgPath := filepath.Join(t.TempDir(), "big.png")
	if err := os.WriteFile(imgPath, make([]byte, wire.MaxAttachmentRawBytes+1), 0o600); err != nil {
		t.Fatalf("write test image: %v", err)
	}

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "too big", "imagePath": imgPath}
	res, err := hub.handleSend(ctx, sendReq)
	if err != nil {
		t.Fatalf("handleSend returned unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error result for an oversized image")
	}
}

func TestSendRejectsUnsupportedImageExtension(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	imgPath := filepath.Join(t.TempDir(), "not-an-image.txt")
	if err := os.WriteFile(imgPath, []byte("hello"), 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "nope", "imagePath": imgPath}
	res, err := hub.handleSend(ctx, sendReq)
	if err != nil {
		t.Fatalf("handleSend returned unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error result for an unsupported extension")
	}
}

// TestReceiveResolvesReferenceFormAttachment covers a chat-relay-style
// server: a delivered msg carries only a token (no inline contentBytes),
// and the actual bytes must be fetched with a follow-up attachment
// request before mcp-hub-client's usual save-to-local-file handling can
// run — see wire.Attachment.IsReference and hubconn.Conn.RequestAttachment.
func TestReceiveResolvesReferenceFormAttachment(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		conn.WriteJSON(wire.Msg{
			Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "look", TS: "ts1",
			Attachments: []wire.Attachment{{Token: "att-3142", ContentType: "image/webp", Name: "shot.png", Kind: "image"}},
		})
		var req wire.AttachmentRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}
		conn.WriteJSON(wire.AttachmentData{
			Type: wire.TypeAttachmentData, Token: req.Token, Name: "shot.png",
			ContentType: "image/webp", ContentBytes: base64.StdEncoding.EncodeToString([]byte("bytes-here")),
		})
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, "550e8400-e29b-41d4-a716-446655440000")}
	if res, err := hub.handleConnect(context.Background(), connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(context.Background(), mcp.CallToolRequest{})

	var res *mcp.CallToolResult
	deadlinePoll(t, func() bool {
		var err error
		res, err = hub.handleReceive(context.Background(), mcp.CallToolRequest{})
		if err != nil {
			t.Fatalf("receive failed: %v", err)
		}
		return strings.Contains(textOf(res), "look")
	})

	savedPath := extractSavedImagePath(t, textOf(res))
	got, err := os.ReadFile(savedPath)
	if err != nil {
		t.Fatalf("could not read saved image at %q: %v", savedPath, err)
	}
	if string(got) != "bytes-here" {
		t.Fatalf("saved image content mismatch: got %q", got)
	}
	if filepath.Ext(savedPath) != ".webp" {
		t.Fatalf("expected a .webp extension (from the fetched attachmentData's contentType), got %q", savedPath)
	}
}

func TestConnectSendReceiveDisconnect(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	res, err := connectAs(t, ctx, hubA, connReq, "hubA")
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}

	hubB := NewHub()
	res, err = connectAs(t, ctx, hubB, connReq, "hubB")
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

func TestHubWaitErrorsWhenNotConnected(t *testing.T) {
	hub := NewHub()
	res, err := hub.handleWait(context.Background(), mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error when not connected")
	}
}

func TestHubWaitReturnsImmediatelyWhenAlreadyBuffered(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := connectAs(t, ctx, hubA, connReq, "hubA"); err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	defer hubA.handleDisconnect(ctx, mcp.CallToolRequest{})

	hubB := NewHub()
	if res, err := connectAs(t, ctx, hubB, connReq, "hubB"); err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}
	defer hubB.handleDisconnect(ctx, mcp.CallToolRequest{})

	// drain a's own roster catch-up (its rosterComplete/peerJoined-for-b)
	// before exercising hub_wait's actual target behavior below.
	deadlinePoll(t, func() bool { return hubA.conn.RosterComplete() })
	hubA.handleReceive(ctx, mcp.CallToolRequest{})

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "already there by the time we wait"}
	deadlinePoll(t, func() bool {
		res, err := hubB.handleSend(ctx, sendReq)
		return err == nil && !res.IsError
	})
	// give it a moment to actually land in hubA's buffer before we call
	// hub_wait, so this genuinely exercises the "already buffered" path
	// rather than racing the real wait-for-arrival path below.
	deadlinePoll(t, func() bool {
		hasEvents, _ := hubA.conn.Peek()
		return hasEvents
	})

	start := time.Now()
	res, err := hubA.handleWait(ctx, mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("wait failed: err=%v result=%+v", err, res)
	}
	if elapsed := time.Since(start); elapsed > waitPollInterval {
		t.Fatalf("expected an already-buffered event to return near-instantly, took %v", elapsed)
	}
	if !strings.Contains(textOf(res), "already there by the time we wait") {
		t.Fatalf("expected the buffered message, got: %s", textOf(res))
	}
	// The reminder to call hub_wait again must lead the result, not trail
	// it: hub_wait is a single blocking call with nothing to fall back on,
	// so if whatever's reading the result gets cut off partway through, a
	// trailing reminder is exactly the part most likely to be lost.
	if !strings.HasPrefix(textOf(res), "REMINDER:") {
		t.Fatalf("expected the hub_wait-again reminder to lead the result, got: %s", textOf(res))
	}
}

func TestHubWaitBlocksUntilMessageArrives(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := connectAs(t, ctx, hubA, connReq, "hubA"); err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	defer hubA.handleDisconnect(ctx, mcp.CallToolRequest{})

	hubB := NewHub()
	if res, err := connectAs(t, ctx, hubB, connReq, "hubB"); err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}
	defer hubB.handleDisconnect(ctx, mcp.CallToolRequest{})

	// drain a's own roster catch-up before exercising the wait-for-arrival
	// path below.
	deadlinePoll(t, func() bool { return hubA.conn.RosterComplete() })
	hubA.handleReceive(ctx, mcp.CallToolRequest{})

	resultCh := make(chan *mcp.CallToolResult, 1)
	go func() {
		res, err := hubA.handleWait(ctx, mcp.CallToolRequest{})
		if err != nil {
			t.Errorf("wait failed: %v", err)
			return
		}
		resultCh <- res
	}()

	// give handleWait a moment to actually start blocking before the
	// message is sent, so this exercises the wait-for-arrival path rather
	// than the already-buffered one covered above.
	time.Sleep(50 * time.Millisecond)

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "arrives while waiting"}
	deadlinePoll(t, func() bool {
		res, err := hubB.handleSend(ctx, sendReq)
		return err == nil && !res.IsError
	})

	select {
	case res := <-resultCh:
		if !strings.Contains(textOf(res), "arrives while waiting") {
			t.Fatalf("expected the message that arrived, got: %s", textOf(res))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hub_wait never returned after a message arrived")
	}
}

func TestHubWaitNewCallSupersedesInFlightOne(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := connectAs(t, ctx, hub, connReq, "hubA"); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})
	deadlinePoll(t, func() bool { return hub.conn.RosterComplete() })
	hub.handleReceive(ctx, mcp.CallToolRequest{}) // drain hub's own rosterComplete

	// b connects up front (not during the supersede dance below), so its
	// roster-join event is drained out of the way before either hub_wait
	// call starts — otherwise whichever call is actively polling would
	// devour that roster event instead of the real message sent later.
	hubB := NewHub()
	if res, err := connectAs(t, ctx, hubB, connReq, "hubB"); err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}
	defer hubB.handleDisconnect(ctx, mcp.CallToolRequest{})
	deadlinePoll(t, func() bool { hasEvents, _ := hub.conn.Peek(); return hasEvents })
	hub.handleReceive(ctx, mcp.CallToolRequest{}) // drain hub's peerJoined-for-b

	firstResultCh := make(chan *mcp.CallToolResult, 1)
	go func() {
		res, _ := hub.handleWait(ctx, mcp.CallToolRequest{})
		firstResultCh <- res
	}()

	// give the first call a moment to actually start blocking (and
	// register itself as h.waitCancel) before the second one supersedes it.
	time.Sleep(50 * time.Millisecond)

	secondResultCh := make(chan *mcp.CallToolResult, 1)
	go func() {
		res, _ := hub.handleWait(ctx, mcp.CallToolRequest{})
		secondResultCh <- res
	}()

	select {
	case res := <-firstResultCh:
		if !strings.Contains(textOf(res), "superseded by a newer hub_wait call") {
			t.Fatalf("expected the first call to report being superseded, got: %s", textOf(res))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the superseded hub_wait call never returned")
	}

	// the second (superseding) call must still be the one actually
	// listening — it should get the next real event, not sit forever
	// having "won" nothing.
	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "for the surviving call"}
	deadlinePoll(t, func() bool {
		res, err := hubB.handleSend(ctx, sendReq)
		return err == nil && !res.IsError
	})

	select {
	case res := <-secondResultCh:
		if !strings.Contains(textOf(res), "for the surviving call") {
			t.Fatalf("expected the surviving call to receive the next real event, got: %s", textOf(res))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the surviving hub_wait call never returned")
	}
}

func TestDisconnectedTextHintsHubCatchUp(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "hi", TS: "ts1", Cursor: "cursor-xyz"})
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, "550e8400-e29b-41d4-a716-446655440000")}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}

	deadlinePoll(t, func() bool { return hub.conn.LastSeenCursor() == "cursor-xyz" })

	deadline := time.Now().Add(2 * time.Second)
	for {
		res, _ := hub.handleReceive(ctx, mcp.CallToolRequest{})
		if strings.Contains(textOf(res), "disconnected") {
			if !strings.Contains(textOf(res), `"cursor-xyz"`) || !strings.Contains(textOf(res), "hub_catch_up()") {
				t.Fatalf("expected the disconnect text to hint at hub_catch_up() with the last seen cursor, got: %s", textOf(res))
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("never observed a disconnected result, last: %s", textOf(res))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestHubWaitReturnsOnDisconnect(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}

	// drain this peer's own rosterComplete (sent even with no existing
	// peers) before exercising the disconnect path below.
	deadlinePoll(t, func() bool { return hub.conn.RosterComplete() })
	hub.handleReceive(ctx, mcp.CallToolRequest{})

	resultCh := make(chan *mcp.CallToolResult, 1)
	go func() {
		res, _ := hub.handleWait(ctx, mcp.CallToolRequest{})
		resultCh <- res
	}()

	time.Sleep(50 * time.Millisecond)
	hub.conn.Close()

	select {
	case res := <-resultCh:
		if !strings.Contains(textOf(res), "disconnected") {
			t.Fatalf("expected a disconnected result, got: %s", textOf(res))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hub_wait never returned after the hub disconnected")
	}
}

func TestHubWaitRespectsContextCancellation(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	// drain this peer's own rosterComplete (sent even with no existing
	// peers) before exercising the cancellation path below.
	deadlinePoll(t, func() bool { return hub.conn.RosterComplete() })
	hub.handleReceive(ctx, mcp.CallToolRequest{})

	waitCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := hub.handleWait(waitCtx, mcp.CallToolRequest{})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected an error when the context is cancelled with nothing to deliver")
	}
	if elapsed > time.Second {
		t.Fatalf("expected handleWait to return promptly on cancellation, took %v", elapsed)
	}
}

func TestPeersToolReturnsRoster(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := connectAs(t, ctx, hubA, connReq, "hubA"); err != nil || res.IsError {
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
	res, err = connectAs(t, ctx, hubB, connReq, "hubB")
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

func TestConnectWithNameAndAgePublicKeyDistributedViaPeers(t *testing.T) {
	t.Skip("blocked on mcp-hub-server: hub_connect sends identity as Agent-Secret/"+
		"Agent-Name/Agent-Age-Public-Key headers, and wsserver still reads name/"+
		"agePublicKey/reconnectSecret as query parameters only, so nothing it echoes "+
		"back or resumes identity from ever arrives. Unskip when wsserver reads the "+
		"headers.")
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()
	pubkey := "age1scdm7mae5t68c9ch0sqfzlusqyflpgxlrgk3zwl44zwl9vvq2guqtdv4fk"

	// hubA and hubB get distinct project scopes (see connectAs) so hubB's
	// omitted secret doesn't resolve to hubA's own stored
	// one — that specific reuse mechanic (and its "was reused
	// automatically" note) has its own dedicated coverage in
	// TestReconnectAfterDisconnectReusesStoredSecretAndPeerID; this test's
	// job is just to verify a's roster actually reflects b's name/
	// agePublicKey, which needs an ordinary, non-colliding join.
	hubA := NewHub()
	connReqA := mcp.CallToolRequest{}
	connReqA.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := connectAs(t, ctx, hubA, connReqA, "hubA"); err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	defer hubA.handleDisconnect(ctx, mcp.CallToolRequest{})

	hubB := NewHub()
	connReqB := mcp.CallToolRequest{}
	connReqB.Params.Arguments = map[string]any{
		"link": hubLink(url, sessionID), "name": "Alice\nfake log line", "agePublicKey": pubkey,
	}
	res, err := connectAs(t, ctx, hubB, connReqB, "hubB")
	if err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}
	defer hubB.handleDisconnect(ctx, mcp.CallToolRequest{})
	for _, line := range strings.Split(textOf(res), "\n") {
		if strings.TrimSpace(line) == "fake log line" {
			t.Fatalf("expected the newline in the raw name to be stripped (no line injection), got line %q in: %s", line, textOf(res))
		}
	}
	if !strings.Contains(textOf(res), "sanitized") || !strings.Contains(textOf(res), "Alicefake log line") {
		t.Fatalf("expected b's connect result to confirm its sanitized display name, got: %s", textOf(res))
	}
	if !strings.Contains(textOf(res), pubkey) {
		t.Fatalf("expected b's connect result to confirm its age public key, got: %s", textOf(res))
	}

	var peersText string
	deadlinePoll(t, func() bool {
		res, err := hubA.handlePeers(ctx, mcp.CallToolRequest{})
		if err != nil {
			t.Fatalf("peers failed: %v", err)
		}
		peersText = textOf(res)
		return strings.Contains(peersText, hubB.conn.PeerID())
	})
	if !strings.Contains(peersText, "Alicefake log line") || !strings.Contains(peersText, pubkey) {
		t.Fatalf("expected a's peer listing to include b's (newline-stripped) name and agePublicKey, got: %s", peersText)
	}
}

func TestConnectRejectsMalformedAgePublicKey(t *testing.T) {
	url := startTestServer(t)
	ctx := context.Background()
	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{
		"link": hubLink(url, "550e8400-e29b-41d4-a716-446655440000"), "agePublicKey": "not-a-key",
	}
	res, err := hub.handleConnect(ctx, connReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected an error for a malformed agePublicKey")
	}
}

func TestConnectWithReconnectSecretReusesPeerIDNotAgePublicKey(t *testing.T) {
	t.Skip("blocked on mcp-hub-server: hub_connect sends identity as Agent-Secret/"+
		"Agent-Name/Agent-Age-Public-Key headers, and wsserver still reads name/"+
		"agePublicKey/reconnectSecret as query parameters only, so nothing it echoes "+
		"back or resumes identity from ever arrives. Unskip when wsserver reads the "+
		"headers.")
	t.Setenv("MCP_HUB_LOG_DIR", t.TempDir()) // isolate persisted secretToPeerID from other tests
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()
	pubkey := "age1scdm7mae5t68c9ch0sqfzlusqyflpgxlrgk3zwl44zwl9vvq2guqtdv4fk"
	const secret = "super-secret-reconnect-token"

	// keeps the session alive across the reconnect below.
	anchor := NewHub()
	anchorReq := mcp.CallToolRequest{}
	anchorReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := anchor.handleConnect(ctx, anchorReq); err != nil || res.IsError {
		t.Fatalf("connect anchor failed: err=%v result=%+v", err, res)
	}
	defer anchor.handleDisconnect(ctx, mcp.CallToolRequest{})

	first := NewHub()
	firstReq := mcp.CallToolRequest{}
	firstReq.Params.Arguments = map[string]any{
		"link": hubLink(url, sessionID), "agePublicKey": pubkey,
	}
	res, err := first.handleConnect(ctx, firstReq)
	if err != nil || res.IsError {
		t.Fatalf("connect first failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "identity") {
		t.Fatalf("expected first's connect result to describe its identity handling, got: %s", textOf(res))
	}
	firstPeerID := first.conn.PeerID()
	if _, err := first.handleDisconnect(ctx, mcp.CallToolRequest{}); err != nil {
		t.Fatalf("disconnect first: %v", err)
	}

	// reconnecting with the same stored secret (but the SAME agePublicKey,
	// which by itself must be irrelevant to reuse) must get the same peerId.
	second := NewHub()
	secondReq := mcp.CallToolRequest{}
	secondReq.Params.Arguments = map[string]any{
		"link": hubLink(url, sessionID), "agePublicKey": pubkey,
	}
	if res, err := second.handleConnect(ctx, secondReq); err != nil || res.IsError {
		t.Fatalf("connect second failed: err=%v result=%+v", err, res)
	}
	defer second.handleDisconnect(ctx, mcp.CallToolRequest{})
	if second.conn.PeerID() != firstPeerID {
		t.Fatalf("expected reconnecting to reuse peerID %q via its stored secret, got %q",
			firstPeerID, second.conn.PeerID())
	}
}

func TestConnectResultStatesExpectedPeerCountAndRosterNotification(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hubA := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	res, err := connectAs(t, ctx, hubA, connReq, "hubA")
	if err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	defer hubA.handleDisconnect(ctx, mcp.CallToolRequest{})
	if !strings.Contains(textOf(res), "No other peers are in this session yet") {
		t.Fatalf("expected a-no-existing-peers note, got: %s", textOf(res))
	}
	// Nothing is claimed about what other peers can SEE when neither a
	// name nor a key was given — there is nothing for them to see. The
	// missing name is still remarked on, since a peer everyone reads as a
	// bare UUID is a cost worth naming.
	if strings.Contains(textOf(res), "can see your") {
		t.Fatalf("expected no visible-identity note when neither name nor agePublicKey was given, got: %s", textOf(res))
	}
	if !strings.Contains(textOf(res), "without a display name") {
		t.Fatalf("expected the missing name to be remarked on, got: %s", textOf(res))
	}

	hubB := NewHub()
	res, err = connectAs(t, ctx, hubB, connReq, "hubB")
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
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := connectAs(t, ctx, hubA, connReq, "hubA"); err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	defer hubA.handleDisconnect(ctx, mcp.CallToolRequest{})

	hubB := NewHub()
	if res, err := connectAs(t, ctx, hubB, connReq, "hubB"); err != nil || res.IsError {
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
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
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
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8")}
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
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	res, err := connectAs(t, ctx, hubA, connReq, "hubA")
	if err != nil || res.IsError {
		t.Fatalf("connect a failed: err=%v result=%+v", err, res)
	}
	peerA := hubA.conn.PeerID()

	hubB := NewHub()
	if res, err := connectAs(t, ctx, hubB, connReq, "hubB"); err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}

	hubC := NewHub()
	if res, err := connectAs(t, ctx, hubC, connReq, "hubC"); err != nil || res.IsError {
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
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
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



func TestReconnectAfterDisconnectReusesStoredSecretAndPeerID(t *testing.T) {
	t.Skip("blocked on mcp-hub-server: hub_connect sends identity as Agent-Secret/"+
		"Agent-Name/Agent-Age-Public-Key headers, and wsserver still reads name/"+
		"agePublicKey/reconnectSecret as query parameters only, so nothing it echoes "+
		"back or resumes identity from ever arrives. Unskip when wsserver reads the "+
		"headers.")
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}

	res1, err := hub.handleConnect(ctx, connReq)
	if err != nil || res1.IsError {
		t.Fatalf("first connect failed: err=%v result=%+v", err, res1)
	}
	firstPeerID := hub.conn.PeerID()

	if res, err := hub.handleDisconnect(ctx, mcp.CallToolRequest{}); err != nil || res.IsError {
		t.Fatalf("disconnect failed: err=%v result=%+v", err, res)
	}

	res2, err := hub.handleConnect(ctx, connReq) // the same link, nothing else carried over
	if err != nil || res2.IsError {
		t.Fatalf("second connect failed: err=%v result=%+v", err, res2)
	}
	if hub.conn.PeerID() != firstPeerID {
		t.Fatalf("expected the same peerID %q to be reassigned via the auto-stored secret, got %q",
			firstPeerID, hub.conn.PeerID())
	}
	if !strings.Contains(textOf(res2), "previous identity here was resumed") {
		t.Fatalf("expected a note that the identity was resumed, got: %s", textOf(res2))
	}
	hub.handleDisconnect(ctx, mcp.CallToolRequest{})
}

func TestHandleListConnectionsShowsEntriesWithoutLeakingSecret(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}

	stored, ok := connstore.Get(connstore.Target{Link: hubLink(url, sessionID), Project: connstore.CurrentProject()})
	if !ok {
		t.Fatal("expected an entry to be stored")
	}

	res, err := hub.handleListConnections(ctx, mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("handleListConnections failed: err=%v result=%+v", err, res)
	}
	text := textOf(res)
	if !strings.Contains(text, url) || !strings.Contains(text, sessionID) || !strings.Contains(text, "(still marked open)") {
		t.Fatalf("expected the still-open entry listed, got: %s", text)
	}
	if strings.Contains(text, stored.ReconnectSecret) {
		t.Fatalf("expected the stored secret to never appear in hub_list_connections output, got: %s", text)
	}

	hub.handleDisconnect(ctx, mcp.CallToolRequest{})
	res2, err := hub.handleListConnections(ctx, mcp.CallToolRequest{})
	if err != nil || res2.IsError {
		t.Fatalf("second handleListConnections failed: err=%v result=%+v", err, res2)
	}
	// Scoped to this test's own entry line (identified by its own unique
	// url/port), not the whole listing — connstore is one shared file for
	// the whole test binary run (see TestMain), so other tests' own
	// still-open entries legitimately coexist here and aren't this
	// assertion's concern.
	ownLine := ""
	for _, line := range strings.Split(textOf(res2), "\n") {
		if strings.Contains(line, url) {
			ownLine = line
			break
		}
	}
	if ownLine == "" {
		t.Fatalf("expected to find this test's own entry in the listing, got: %s", textOf(res2))
	}
	if strings.Contains(ownLine, "(still marked open)") {
		t.Fatalf("expected this test's own entry to no longer be marked open after disconnect, got line: %s", ownLine)
	}
}

func TestHandleListConnectionsIncludesTeamsSessionsAndTopics(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440001"
	ctx := context.Background()
	project := connstore.CurrentProject()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	target := connstore.Target{Link: hubLink(url, sessionID), Project: project}
	entry, ok := connstore.Get(target)
	if !ok {
		t.Fatal("expected an entry to be stored")
	}
	entry.Topic = "Hub Session Topic"
	if err := connstore.Upsert(target, entry); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	teamsLink := "wss://teams.example/relay/join?c=list-connections-test"
	if err := connstore.SetTopic(connstore.Target{Link: teamsLink, Project: project}, "Teams Conversation Topic"); err != nil {
		t.Fatalf("SetTopic: %v", err)
	}

	res, err := hub.handleListConnections(ctx, mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("handleListConnections failed: err=%v result=%+v", err, res)
	}
	text := textOf(res)
	if !strings.Contains(text, url) || !strings.Contains(text, sessionID) {
		t.Fatalf("expected the hub entry listed, got: %s", text)
	}
	if !strings.Contains(text, `topic="Hub Session Topic"`) {
		t.Fatalf("expected the hub entry's topic shown, got: %s", text)
	}
	if !strings.Contains(text, teamsLink) {
		t.Fatalf("expected the teams session listed, got: %s", text)
	}
	if !strings.Contains(text, `topic="Teams Conversation Topic"`) {
		t.Fatalf("expected the teams relay entry's topic shown, got: %s", text)
	}
}

func TestStartupConnectionsNoteReflectsOpenEntries(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	if note := startupConnectionsNote(); note != "" {
		t.Fatalf("expected no note with an empty store, got: %s", note)
	}

	if err := connstore.Upsert(
		connstore.Target{Link: "wss://a/550e8400-e29b-41d4-a716-446655440000"},
		connstore.Entry{Connected: true},
	); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	note := startupConnectionsNote()
	if !strings.Contains(note, "1 session") || !strings.Contains(note, "hub_list_connections") {
		t.Fatalf("expected a note naming 1 open session, got: %s", note)
	}
}


func TestShutdownClosesActiveConnectionAndMarksStoreDisconnected(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}

	hub.Shutdown()

	conn, _ := hub.activeConn()
	if conn != nil {
		t.Fatal("expected Shutdown to clear the active connection")
	}
	stored, ok := connstore.Get(connstore.Target{Link: hubLink(url, sessionID), Project: connstore.CurrentProject()})
	if !ok {
		t.Fatal("expected the entry to still exist")
	}
	if stored.Connected {
		t.Fatal("expected Shutdown to mark the connstore entry disconnected")
	}
}

func TestShutdownWithoutAnyConnectionIsANoop(t *testing.T) {
	hub := NewHub()
	hub.Shutdown() // must not panic
}

func TestConnectPassesCreateTokenAsHeader(t *testing.T) {
	var gotHeader string
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("Hub-Create-Token")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
	}))
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{
		"link": hubLink(url, "550e8400-e29b-41d4-a716-446655440000"),
		"createToken": "prefix.secretvalue",
	}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	if gotHeader != "prefix.secretvalue" {
		t.Fatalf("expected the createToken to be sent as Hub-Create-Token, got %q", gotHeader)
	}
}

func TestConnectTwiceErrors(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()
	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}

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

func imagesOf(res *mcp.CallToolResult) []mcp.ImageContent {
	var images []mcp.ImageContent
	for _, c := range res.Content {
		if ic, ok := c.(mcp.ImageContent); ok {
			images = append(images, ic)
		}
	}
	return images
}

// TestPeersToolReportsDisconnectInsteadOfStaleRoster is a regression test:
// hub_peers used to read straight from Conn.Peers() with no check on
// whether the underlying connection was still alive, so a silent
// disconnect (server restart, network drop — anything short of a clean
// hub_disconnect()) left it confidently returning a roster from before the
// drop, with no indication anything was wrong. A second peer joins first
// so there's an actual roster to go stale if that bug were still present.
func TestPeersToolReportsDisconnectInsteadOfStaleRoster(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := connectAs(t, ctx, hub, connReq, "hubA"); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}

	hubB := NewHub()
	if res, err := connectAs(t, ctx, hubB, connReq, "hubB"); err != nil || res.IsError {
		t.Fatalf("connect b failed: err=%v result=%+v", err, res)
	}
	defer hubB.handleDisconnect(ctx, mcp.CallToolRequest{})
	deadlinePoll(t, func() bool {
		res, _ := hub.handlePeers(ctx, mcp.CallToolRequest{})
		return res != nil && strings.Contains(textOf(res), "Current peers")
	})

	conn, _ := hub.activeConn()
	conn.Close() // simulate a silent drop, not a clean hub_disconnect()
	deadlinePoll(t, func() bool { c, _ := hub.activeConn(); return c == nil })

	res, err := hub.handlePeers(ctx, mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("handlePeers returned an error: %v", err)
	}
	if strings.Contains(textOf(res), "Current peers") {
		t.Fatalf("expected no stale roster after disconnect, got: %s", textOf(res))
	}
	if !strings.Contains(textOf(res), "not connected") {
		t.Fatalf("expected a clear not-connected result, got: %s", textOf(res))
	}
	if c, w := hub.activeConn(); c != nil || w != nil {
		t.Fatalf("expected the dead connection to be torn down, got conn=%v waiter=%v", c, w)
	}
}

// TestSendToolReportsDisconnectInsteadOfAttemptingASend is the hub_send
// analog of TestPeersToolReportsDisconnectInsteadOfStaleRoster.
func TestSendToolReportsDisconnectInsteadOfAttemptingASend(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}

	conn, _ := hub.activeConn()
	conn.Close()
	deadlinePoll(t, func() bool { c, _ := hub.activeConn(); return c == nil })

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "hello"}
	res, err := hub.handleSend(ctx, sendReq)
	if err != nil {
		t.Fatalf("handleSend returned an error: %v", err)
	}
	if !strings.Contains(textOf(res), "not connected") {
		t.Fatalf("expected a clear not-connected result, got: %s", textOf(res))
	}
}

// TestConnectAfterSilentDisconnectDoesNotRequireExplicitDisconnect proves
// hub_connect self-heals from a stale, silently-dead connection instead of
// permanently refusing to reconnect until hub_disconnect is called on a
// connection that, in every observable sense, is already gone.
func TestConnectAfterSilentDisconnectDoesNotRequireExplicitDisconnect(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("first connect failed: err=%v result=%+v", err, res)
	}

	conn, _ := hub.activeConn()
	conn.Close()
	deadlinePoll(t, func() bool { c, _ := hub.activeConn(); return c == nil })

	res, err := hub.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("reconnect after a silent disconnect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})
}

// TestDisconnectDetectedAutomaticallyWithoutAnyToolCall proves the hub
// tears itself down the moment its connection dies — closing the wait
// socket and forgetting the connection — even with nothing actively
// blocked on it and no further tool call happening at all, rather than
// only reactively the next time some handler happens to check.
func TestDisconnectDetectedAutomaticallyWithoutAnyToolCall(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}

	conn, _ := hub.activeConn()
	conn.Close()

	deadlinePoll(t, func() bool {
		c, w := hub.activeConn()
		return c == nil && w == nil
	})
}

// TestSetCatchUpKeyLoadsNothingForAFreshKey proves an identity never seen
// before starts with no position — the common case for a brand-new
// session.
func TestSetCatchUpKeyLoadsNothingForAFreshKey(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	hub := NewHub()
	hub.setCatchUpKey(connstore.Target{Link: "wss://brand-new/550e8400-e29b-41d4-a716-446655440000"})

	hub.mu.Lock()
	got := hub.lastHandedOverCursor
	hub.mu.Unlock()
	if got != "" {
		t.Fatalf("expected no prior position for a fresh identity, got %q", got)
	}
}

// TestSetCatchUpKeyPersistsAcrossHubInstances proves the whole point of
// persisting via connstore rather than keeping this in memory only: a
// SECOND *Hub (standing in for a fresh process after a restart) calling
// setCatchUpKey with the same identity recovers the position the first
// Hub persisted, rather than starting over.
func TestSetCatchUpKeyPersistsAcrossHubInstances(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	target := connstore.Target{Link: "wss://persist-test/550e8400-e29b-41d4-a716-446655440000"}
	id := target

	first := NewHub()
	first.setCatchUpKey(id)
	if err := connstore.SetCatchUp(target, connstore.CatchUpState{Cursor: "cursor-1"}); err != nil {
		t.Fatalf("SetCatchUp: %v", err)
	}

	second := NewHub()
	second.setCatchUpKey(id)

	second.mu.Lock()
	got := second.lastHandedOverCursor
	second.mu.Unlock()
	if got != "cursor-1" {
		t.Fatalf("expected the second Hub to recover cursor-1 for the same identity, got %q", got)
	}
}

// TestSetCatchUpKeyIsolatesDifferentKeys proves two different identities
// never bleed into each other — the stale-cursor-bleed guard the old
// target-based mechanism used to provide, now provided by connstore's
// own hierarchical keying instead of a same-target comparison.
func TestSetCatchUpKeyIsolatesDifferentKeys(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	targetA := connstore.Target{Link: "wss://key-a/550e8400-e29b-41d4-a716-446655440000"}
	if err := connstore.SetCatchUp(targetA, connstore.CatchUpState{Cursor: "cursor-1"}); err != nil {
		t.Fatalf("SetCatchUp: %v", err)
	}

	hub := NewHub()
	targetB := connstore.Target{Link: "wss://key-b/550e8400-e29b-41d4-a716-446655440000"}
	hub.setCatchUpKey(targetB)

	hub.mu.Lock()
	got := hub.lastHandedOverCursor
	hub.mu.Unlock()
	if got != "" {
		t.Fatalf("expected key-b to have no position of its own, got %q", got)
	}
}

// TestTargetForLinkKeysOnTheWholeLinkAndProject proves the derivation
// every connection's stored identity uses. The credential is PART of the
// key: on a hub link the fragment is the session id, so keying on the
// address alone would collapse every session on one relay into a single
// entry sharing one identity and one read position. Project scoping is the
// other half — it keeps two agents on one machine from fighting over one
// peerId.
func TestTargetForLinkKeysOnTheWholeLinkAndProject(t *testing.T) {
	ctx := context.Background()
	t.Setenv("MCP_HUB_PROJECT_DIR", "/project/a")
	sessionA := targetForLink(ctx, "wss://relay.example/hub/join#session-a")
	sessionB := targetForLink(ctx, "wss://relay.example/hub/join#session-b")
	if sessionA == sessionB {
		t.Fatalf("expected two sessions on one relay to be distinct identities, both derived %+v", sessionA)
	}
	if again := targetForLink(ctx, "wss://relay.example/hub/join#session-a"); again != sessionA {
		t.Fatalf("expected the same link to derive the same identity, got %+v vs %+v", again, sessionA)
	}

	t.Setenv("MCP_HUB_PROJECT_DIR", "/project/b")
	if other := targetForLink(ctx, "wss://relay.example/hub/join#session-a"); other == sessionA {
		t.Fatalf("expected a different identity for a different project, got the same: %+v", other)
	}
}

func TestParseMentionsNilIsANoOp(t *testing.T) {
	mentions, err := parseMentions(nil)
	if err != nil || mentions != nil {
		t.Fatalf("expected (nil, nil), got (%v, %v)", mentions, err)
	}
}

func TestParseMentionsAcceptsEachIdentifierKind(t *testing.T) {
	raw := []any{
		map[string]any{"id": "dir-1"},
		map[string]any{"peerId": "550e8400-e29b-41d4-a716-446655440000"},
		map[string]any{"name": "Steffen", "text": "@Steffen"},
	}
	mentions, err := parseMentions(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mentions) != 3 || mentions[0].ID != "dir-1" ||
		mentions[1].PeerID != "550e8400-e29b-41d4-a716-446655440000" ||
		mentions[2].Name != "Steffen" || mentions[2].Text != "@Steffen" {
		t.Fatalf("unexpected mentions: %+v", mentions)
	}
}

func TestParseMentionsRejectsZeroIdentifiers(t *testing.T) {
	_, err := parseMentions([]any{map[string]any{"text": "@nobody"}})
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("expected an exactly-one error, got: %v", err)
	}
}

func TestParseMentionsRejectsTwoIdentifiers(t *testing.T) {
	_, err := parseMentions([]any{map[string]any{"id": "dir-1", "name": "Steffen"}})
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("expected an exactly-one error, got: %v", err)
	}
}

func TestParseMentionsRejectsNonArray(t *testing.T) {
	_, err := parseMentions("not an array")
	if err == nil {
		t.Fatal("expected an error for a non-array mentions value")
	}
}

func TestParseMentionsRejectsNonObjectEntry(t *testing.T) {
	_, err := parseMentions([]any{"not an object"})
	if err == nil {
		t.Fatal("expected an error for a non-object mentions entry")
	}
}





// Connecting with no display name leaves every other peer looking at a
// UUID, so the result says so rather than letting it pass unremarked.
func TestConnectWithoutANameSaysWhatThatCosts(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID)}
	res, err := hub.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})
	if !strings.Contains(textOf(res), "without a display name") {
		t.Fatalf("expected the result to name the cost of connecting anonymously, got: %s", textOf(res))
	}
}

func TestConnectWithANameDoesNotNagAboutIt(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440001"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": hubLink(url, sessionID), "name": "Claude Code (mcp-hub)"}
	res, err := hub.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})
	text := textOf(res)
	if strings.Contains(text, "without a display name") {
		t.Fatalf("expected no nag when a name was given, got: %s", text)
	}
	// The name is not asserted to come back here: this test's server is
	// wsserver, which reads a name only from a query parameter, and the
	// client no longer sends one. What matters is that passing a name
	// stops the nag, and that nothing renders a half-formed sentence when
	// the server echoes nothing.
	if strings.Contains(text, "can see your .") {
		t.Fatalf("expected no half-formed identity sentence, got: %s", text)
	}
}
