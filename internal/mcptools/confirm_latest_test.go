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

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/wire"
)

// ackLatestServer answers ack{latest:true} with the position it landed on,
// and records what it was sent.
func ackLatestServer(t *testing.T, declare bool, sent chan<- map[string]any) string {
	return ackLatestServerAt(t, declare, sent, "newest.900")
}

func ackLatestServerAt(t *testing.T, declare bool, sent chan<- map[string]any, landed string) string {
	t.Helper()
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
		if declare {
			joined.Features[wire.FeatureAckLatest] = json.RawMessage(`{}`)
		}
		conn.WriteJSON(joined)
		for {
			var req map[string]any
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if req["type"] != string(wire.TypeAck) {
				continue
			}
			sent <- req
			ok, behind := true, 0
			id, _ := req["id"].(string)
			conn.WriteJSON(wire.Ack{Type: wire.TypeAck, ID: id, AckCursor: landed, OK: &ok, Behind: &behind})
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub/join#latest-test"
}

func TestConfirmLatestMovesTheStoredPositionToWhereTheServerLanded(t *testing.T) {
	sent := make(chan map[string]any, 4)
	link := ackLatestServer(t, true, sent)
	ctx := context.Background()
	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": link}
	if res, err := hub.handleConnect(ctx, req); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	t.Cleanup(func() { hub.handleDisconnect(ctx, connReqFor(testConn)) })
	id := sole(t, hub).catchUpID

	confirm := mcp.CallToolRequest{}
	confirm.Params.Arguments = map[string]any{"connection": testConn, "latest": true}
	res, _ := hub.handleConfirmReceived(ctx, confirm)
	if res == nil || res.IsError || !strings.Contains(textOf(res), "newest.900") {
		t.Fatalf("hub_confirm(latest) reported %+v", res)
	}
	frame := <-sent
	if frame["latest"] != true || frame["ackCursor"] != nil {
		t.Fatalf("the ack sent was %v, want latest:true and no cursor", frame)
	}
	after, _, _ := connstore.GetCatchUp(id)
	if after.Cursor != "newest.900" {
		t.Fatalf("stored position %q, want newest.900", after.Cursor)
	}
}

func TestConfirmLatestIsRefusedWhereTheServerDoesNotDeclareIt(t *testing.T) {
	sent := make(chan map[string]any, 4)
	link := ackLatestServer(t, false, sent)
	ctx := context.Background()
	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": link}
	if res, err := hub.handleConnect(ctx, req); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	t.Cleanup(func() { hub.handleDisconnect(ctx, connReqFor(testConn)) })
	confirm := mcp.CallToolRequest{}
	confirm.Params.Arguments = map[string]any{"connection": testConn, "latest": true}
	res, _ := hub.handleConfirmReceived(ctx, confirm)
	if res == nil || !res.IsError || !strings.Contains(textOf(res), "NOT moved") {
		t.Fatalf("hub_confirm(latest) on a server without ackLatest reported %+v", res)
	}
	select {
	case f := <-sent:
		t.Fatalf("an ack was sent anyway: %v", f)
	default:
	}
}

func TestARefusedConfirmLatestMovesNothing(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
		joined.Features = map[string]json.RawMessage{
			"ackReplies": json.RawMessage(`{}`), "messageAfter": json.RawMessage(`{}`),
			wire.FeatureCorrelation: json.RawMessage(`{}`), wire.FeatureAckLatest: json.RawMessage(`{}`),
		}
		conn.WriteJSON(joined)
		for {
			var req map[string]any
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if req["type"] == string(wire.TypeAck) {
				no := false
				id, _ := req["id"].(string)
				conn.WriteJSON(wire.Ack{Type: wire.TypeAck, ID: id, AckCursor: "held.1", OK: &no})
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub/join#latest-refused"
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
	confirm.Params.Arguments = map[string]any{"connection": testConn, "latest": true}
	res, _ := hub.handleConfirmReceived(ctx, confirm)
	if res == nil || !res.IsError || !strings.Contains(textOf(res), "REFUSED") {
		t.Fatalf("a refused hub_confirm(latest) reported %+v", res)
	}
	if after, _, _ := connstore.GetCatchUp(id); after.Cursor != before.Cursor {
		t.Fatalf("a refusal moved the stored position to %q", after.Cursor)
	}
}

func TestConfirmLatestWithNothingToReadMovesNothingAndSucceeds(t *testing.T) {
	sent := make(chan map[string]any, 4)
	link := ackLatestServerAt(t, true, sent, "")
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
	confirm.Params.Arguments = map[string]any{"connection": testConn, "latest": true}
	res, _ := hub.handleConfirmReceived(ctx, confirm)
	if res == nil || res.IsError || !strings.Contains(textOf(res), "nothing") {
		t.Fatalf("hub_confirm(latest) with nothing to read reported %+v", res)
	}
	if after, _, _ := connstore.GetCatchUp(id); after.Cursor != before.Cursor {
		t.Fatalf("the stored position moved to %q", after.Cursor)
	}
}
