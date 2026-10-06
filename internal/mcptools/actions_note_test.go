package mcptools

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/wire"
)

// A connect result says which actions the conversation supports and which
// it refuses, so finding out never means trying an edit on a real message.
// A mirrored conversation states its formats too, alongside the rest.
func TestConnectSaysWhatTheConversationSupports(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	features := teamsTestFeatures()
	features["formats"] = json.RawMessage(`{"accepted":["text","markdown"],"default":"text"}`)
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000",
			ServerVersion: wire.ProtocolVersion, Features: features, ConversationKind: "channel", CanSend: true})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/teams/join#actions-note"

	ctx := context.Background()
	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": link}
	res, err := hub.handleConnect(ctx, req)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	t.Cleanup(func() { hub.handleDisconnect(ctx, connReqFor(testConn)) })

	text := textOf(res)
	for _, want := range []string{
		"Formats this conversation accepts (hub_send/hub_edit format): text, markdown.",
		"Supported here: reactions (hub_react); editing your messages (hub_edit); deleting your messages (hub_delete);",
		"NOT supported here, refused before sending: pins (hub_pin, hub_unpin, hub_pins).",
		"This mirrors a real chat conversation",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("connect result lacks %q:\n%s", want, text)
		}
	}
}
