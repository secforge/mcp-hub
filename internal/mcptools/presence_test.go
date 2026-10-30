package mcptools

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/harness"
	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/wire"
)

// A reconnect leaves runs of joins and leaves in the history. A catch-up
// delivers each run as one delivery under its last entry's cursor, with
// every peer once and what it did in order, and leaves real messages as
// they are.
func TestARunOfPresenceEntriesIsDeliveredAsOne(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	const self = "550e8400-e29b-41d4-a716-446655440000"
	const other = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	presence := func(event, peer, name, cause string) *wire.Presence {
		return &wire.Presence{Event: event, Members: []wire.PresenceMember{{PeerID: peer, Name: name}}, Cause: cause}
	}
	history := []wire.Msg{
		{PeerID: other, Text: "before"},
		{PeerID: other, Text: "relay-chat left", Presence: presence("left", other, "relay-chat", "serverRestart")},
		{PeerID: self, Text: "mcp-hub left", Presence: presence("left", self, "mcp-hub", "")},
		{PeerID: other, Text: "relay-chat reconnected", Presence: presence("reconnected", other, "relay-chat", "")},
		{PeerID: self, Text: "mcp-hub reconnected", Presence: presence("reconnected", self, "mcp-hub", "")},
		{PeerID: other, Text: "after"},
		{PeerID: other, Text: "relay-chat left", Presence: presence("left", other, "relay-chat", "")},
	}
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		raw, _ := json.Marshal(wire.Joined{Type: wire.TypeJoined, PeerID: self,
			ServerVersion: wire.ProtocolVersion, Features: teamsTestFeatures(), ConversationKind: "group"})
		conn.WriteMessage(websocket.TextMessage, raw)
		for {
			var m wire.MessageAfter
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			next := 0
			if m.Cursor != "" {
				fmt.Sscanf(m.Cursor, "c%d", &next)
			}
			if next >= len(history) {
				conn.WriteJSON(wire.NewNoMoreMessages(wire.Anchor{At: m.At, Cursor: m.Cursor}))
				continue
			}
			h := history[next]
			h.Type, h.Cursor, h.TS, h.Historical = wire.TypeMsg, fmt.Sprintf("c%d", next+1),
				fmt.Sprintf("2026-10-29T18:00:0%dZ", next), true
			h.Answers = &wire.Anchor{At: m.At, Cursor: m.Cursor}
			conn.WriteJSON(h)
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc-" + uniqueConvID() + "#presence"
	ctx := context.Background()
	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": link}
	if res, err := hub.handleConnect(ctx, req); err != nil || res.IsError {
		t.Fatalf("connect: %v %+v", err, res)
	}
	defer hub.handleDisconnect(ctx, connReqFor(testConn))
	s, _ := hub.session(testConn)
	conn, _ := s.activeConn()
	rec := &recordingDeliverer{}
	s.pusher = harness.PusherForTesting(rec)

	res := s.walkCatchUpPush(conn, connstore.Target{}, wire.Anchor{}, catchUpWant{})
	if res.Err != nil || !res.CaughtUp || res.Delivered != len(history) || res.Deliveries != 4 {
		t.Fatalf("ending %+v", res)
	}
	var cursors []string
	for _, d := range rec.got {
		cursors = append(cursors, d.Cursor)
	}
	if fmt.Sprint(cursors) != "[c1 c5 c6 c7]" {
		t.Fatalf("deliveries under cursors %v", cursors)
	}
	run := rec.got[1].Body
	for _, want := range []string{"4 change(s)", "relay-chat (" + other + "): left at the server restart → reconnected",
		"mcp-hub (you): left → reconnected"} {
		if !strings.Contains(run, want) {
			t.Fatalf("the run lacks %q:\n%s", want, run)
		}
	}
	if strings.Contains(run, "before") || strings.Contains(run, "after") {
		t.Fatalf("a real message went into the run:\n%s", run)
	}
}

// A link's live "joined the channel" messages are merged into one push,
// and a real message after them is pushed as itself.
func TestLiveJoinMessagesArePushedAsOne(t *testing.T) {
	in := newHarnessInbox(t)
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	defer hubconn.SetRosterMergeWindowForTesting(300 * time.Millisecond)()
	const self = "550e8400-e29b-41d4-a716-446655440000"
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		raw, _ := json.Marshal(wire.Joined{Type: wire.TypeJoined, PeerID: self,
			ServerVersion: wire.ProtocolVersion, Features: teamsTestFeatures(), ConversationKind: "group"})
		conn.WriteMessage(websocket.TextMessage, raw)
		time.Sleep(200 * time.Millisecond)
		for i, name := range []string{"Leon", "Mikhail"} {
			id := fmt.Sprintf("6ba7b810-9dad-11d1-80b4-00c04fd430c%d", i)
			conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: id, Cursor: fmt.Sprintf("j%d", i),
				Text: name + " joined the channel", TS: "2026-10-30T16:26:14Z",
				Presence: &wire.Presence{Event: "joined", Members: []wire.PresenceMember{{PeerID: id, Name: name}}}})
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(600 * time.Millisecond)
		conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c9",
			Cursor: "m1", Text: "a real question", TS: "2026-10-30T16:27:00Z"})
		time.Sleep(2 * time.Second)
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc-" + uniqueConvID() + "#live-presence"
	h := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": link, "name": "t"}
	if res, err := h.handleConnect(context.Background(), req); err != nil || res.IsError {
		t.Fatalf("connect: %v %s", err, textOf(res))
	}
	defer h.handleDisconnect(context.Background(), connReqFor(testConn))
	waitUntil(t, "the real message", func() bool { return in.has("a real question") })
	in.mu.Lock()
	defer in.mu.Unlock()
	var joins, real int
	for _, g := range in.got {
		if strings.Contains(g, "joined") {
			joins++
			if !strings.Contains(g, "2 change(s) live") || !strings.Contains(g, "Leon") || !strings.Contains(g, "Mikhail") {
				t.Fatalf("the joins were not merged: %s", g)
			}
		}
		if strings.Contains(g, "a real question") {
			real++
		}
	}
	if joins != 1 || real != 1 {
		t.Fatalf("%d join pushes and %d message pushes, want 1 and 1:\n%s", joins, real, strings.Join(in.got, "\n---\n"))
	}
}
