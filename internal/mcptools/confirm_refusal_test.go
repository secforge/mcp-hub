package mcptools

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/wire"
)

// The incident this guards: a mistyped cursor was refused by a server that
// declares both ackReplies and correlation, but the refusal carried no
// request id. It never answered the waiting confirm, the confirm timed
// out, and hub_confirm said "persisted" — leaving the refused cursor as
// where the next catch-up would start. An unanswered confirm on such a
// server must leave the stored position exactly where it was.
func TestARefusalThatMissesTheConfirmDoesNotMoveTheStoredPosition(t *testing.T) {
	orig := hubconn.AckWaitTimeout
	hubconn.AckWaitTimeout = 150 * time.Millisecond
	t.Cleanup(func() { hubconn.AckWaitTimeout = orig })

	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
		joined.Features = map[string]json.RawMessage{
			"ackReplies":            json.RawMessage(`{}`),
			"messageAfter":          json.RawMessage(`{}`),
			wire.FeatureCorrelation: json.RawMessage(`{}`),
		}
		conn.WriteJSON(joined)
		for {
			var req struct {
				Type string `json:"type"`
			}
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if req.Type == string(wire.TypeAck) {
				// Refused WITHOUT echoing the request's id.
				conn.WriteJSON(wire.Error{Type: wire.TypeError, Code: "bad_ack_cursor",
					Message: "that cursor belongs to another conversation"})
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub/join#refusal-test"

	ctx := context.Background()
	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": link}
	if res, err := hub.handleConnect(ctx, req); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	t.Cleanup(func() { hub.handleDisconnect(ctx, connReqFor(testConn)) })
	id := sole(t, hub).catchUpID
	before, _, _ := connstore.GetCatchUp(id)

	confirm := mcp.CallToolRequest{}
	confirm.Params.Arguments = map[string]any{"connection": testConn, "cursor": "a-cursor-from-elsewhere"}
	res, _ := hub.handleConfirmReceived(ctx, confirm)
	if res == nil || !res.IsError || strings.Count(strings.ToLower(textOf(res)), "has not moved") != 1 {
		t.Fatalf("hub_confirm reported %+v; want a failure saying the position did not move", res)
	}
	after, _, _ := connstore.GetCatchUp(id)
	if after.Cursor != before.Cursor {
		t.Fatalf("the stored position moved %q -> %q on a confirm nothing accepted", before.Cursor, after.Cursor)
	}
}

// A confirm the server answers with ok:false is one whose cursor the held
// position already covers — a message delivered after a later one was
// confirmed. That is nothing to do, not a failure, and the result names
// the position the server holds.
func TestAnOkFalseConfirmIsAlreadyCovered(t *testing.T) {
	const held = "639267938090700000.57879"
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
		joined.Features = map[string]json.RawMessage{
			"ackReplies":            json.RawMessage(`{}`),
			"messageAfter":          json.RawMessage(`{}`),
			wire.FeatureCorrelation: json.RawMessage(`{}`),
		}
		conn.WriteJSON(joined)
		for {
			var req struct {
				Type string `json:"type"`
				ID   string `json:"id"`
			}
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if req.Type == string(wire.TypeAck) {
				no := false
				conn.WriteJSON(wire.Ack{Type: wire.TypeAck, ID: req.ID, AckCursor: held, OK: &no})
			}
		}
	}))
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub/join#ok-false-covered"

	ctx := context.Background()
	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": link}
	if res, err := hub.handleConnect(ctx, req); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	confirm := mcp.CallToolRequest{}
	confirm.Params.Arguments = map[string]any{"connection": testConn, "cursor": "639267938033310000.57882"}
	res, _ := hub.handleConfirmReceived(ctx, confirm)
	hub.handleDisconnect(ctx, connReqFor(testConn))
	srv.Close()

	text := textOf(res)
	if res == nil || res.IsError || !strings.Contains(text, "already covered") {
		t.Fatalf("an ok:false confirm was not reported as already covered: %+v", res)
	}
	if !strings.Contains(text, held) || strings.Contains(text, "..") {
		t.Errorf("the held position is missing or the sentence is mangled: %s", text)
	}
}
