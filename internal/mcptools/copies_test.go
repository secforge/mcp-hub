package mcptools

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/wire"
)

// Two copies of one platform thread, through two accounts: a message both
// receive is pushed once, and confirming it where it was pushed confirms
// it on the other copy too.
func TestAMessageOnTwoCopiesOfAThreadIsDeliveredOnce(t *testing.T) {
	in := newHarnessInbox(t)
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())

	var mu sync.Mutex
	acks := map[string][]string{} // connection key -> cursors acked
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("c")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		self := map[string]string{"a": "550e8400-e29b-41d4-a716-446655440000", "b": "550e8400-e29b-41d4-a716-446655440001"}[key]
		f := teamsTestFeatures()
		f[wire.FeatureThreadIdentity] = json.RawMessage(`{}`)
		f["ackReplies"] = json.RawMessage(`{}`)
		f[wire.FeatureCorrelation] = json.RawMessage(`{}`)
		zero := 0
		conn.WriteJSON(wire.Joined{Type: wire.TypeJoined, PeerID: self, ServerVersion: wire.ProtocolVersion,
			Features: f, Behind: &zero, Thread: "thread-1"})
		conn.WriteJSON(wire.Roster{Type: wire.TypeRoster, Members: []wire.RosterMember{{PeerID: self, Name: "t"}}})
		go func() {
			time.Sleep(300 * time.Millisecond)
			conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
				Text: "the same message", ExternalID: "m1", Cursor: "cur-" + key + "-1", TS: "2026-11-07T12:00:00Z"})
		}()
		for {
			var req map[string]any
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if req["type"] != string(wire.TypeAck) {
				continue
			}
			c, _ := req["ackCursor"].(string)
			id, _ := req["id"].(string)
			mu.Lock()
			acks[key] = append(acks[key], c)
			mu.Unlock()
			ok := true
			conn.WriteJSON(wire.Ack{Type: wire.TypeAck, ID: id, AckCursor: c, OK: &ok})
		}
	}))
	t.Cleanup(srv.Close)
	base := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c="

	h := NewHub()
	for _, k := range []string{"a", "b"} {
		req := mcp.CallToolRequest{}
		req.Params.Arguments = map[string]any{"as": "copy-" + k, "link": base + k + "#copies-" + uniqueConvID(), "name": "t"}
		if res, err := h.handleConnect(context.Background(), req); err != nil || res.IsError {
			t.Fatalf("connect %s: %v %s", k, err, textOf(res))
		}
		defer h.handleDisconnect(context.Background(), connReqFor("copy-"+k))
	}
	waitUntil(t, "the message", func() bool { return in.has("the same message") })
	time.Sleep(500 * time.Millisecond)
	in.mu.Lock()
	n := 0
	var pushedOn string
	for _, g := range in.got {
		if strings.Contains(g, "the same message") {
			n++
			if strings.Contains(g, "cur-a-1") {
				pushedOn = "a"
			} else {
				pushedOn = "b"
			}
		}
	}
	in.mu.Unlock()
	if n != 1 {
		t.Fatalf("the message was pushed %d times, want once", n)
	}
	other := map[string]string{"a": "b", "b": "a"}[pushedOn]

	confirm := mcp.CallToolRequest{}
	confirm.Params.Arguments = map[string]any{"connection": "copy-" + pushedOn, "cursor": "cur-" + pushedOn + "-1"}
	if res, _ := h.handleConfirmReceived(context.Background(), confirm); res == nil || res.IsError {
		t.Fatalf("confirm: %s", textOf(res))
	}
	waitUntil(t, "the copy confirmed on the other connection", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range acks[other] {
			if c == "cur-"+other+"-1" {
				return true
			}
		}
		return false
	})
}
