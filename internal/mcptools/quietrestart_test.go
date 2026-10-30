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

	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/wire"
)

// An announced restart that comes back in time, with nothing but joins
// and leaves meanwhile, wakes nobody: nothing is pushed, and what it
// brought waits in front of the next tool result.
func TestARestartThatComesBackInTimeIsNotPushed(t *testing.T) {
	testQuietRestart(t, 2, false, []string{"Now 1 here: other (6ba7b810-9dad-11d1-80b4-00c04fd430c8).", "other (6ba7b810-9dad-11d1-80b4-00c04fd430c8): left at the server restart → reconnected",
		"Nothing else arrived meanwhile", "p2"})
}

// A backlog past the bound is not walked by itself: the one notice says
// what is waiting instead.
func TestARestartWithALargeBacklogLeavesTheCatchUpToTheReader(t *testing.T) {
	testQuietRestart(t, 150, true, []string{"150 messages arrived meanwhile", "call hub_catch_up"})
}

func testQuietRestart(t *testing.T, behindAfterRestart int, pushed bool, wants []string) {
	in := newHarnessInbox(t)
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	prevQuiet := restartQuiet
	restartQuiet = 300 * time.Millisecond
	defer func() { restartQuiet = prevQuiet }()

	const self = "550e8400-e29b-41d4-a716-446655440000"
	const other = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
	history := []wire.Msg{
		{PeerID: other, Text: "other left", Presence: &wire.Presence{Event: "left", Members: []wire.PresenceMember{{PeerID: other, Name: "other"}}, Cause: "serverRestart"}},
		{PeerID: other, Text: "other reconnected", Presence: &wire.Presence{Event: "reconnected", Members: []wire.PresenceMember{{PeerID: other, Name: "other"}}}},
	}
	var mu sync.Mutex
	joins := 0
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		mu.Lock()
		joins++
		first := joins == 1
		mu.Unlock()
		behind := 0
		if !first {
			behind = behindAfterRestart
		}
		raw, _ := json.Marshal(wire.Joined{Type: wire.TypeJoined, PeerID: self, ServerVersion: wire.ProtocolVersion,
			Features: teamsTestFeatures(), Behind: &behind})
		conn.WriteMessage(websocket.TextMessage, raw)
		conn.WriteJSON(wire.Roster{Type: wire.TypeRoster, Members: []wire.RosterMember{{PeerID: self, Name: "t"}, {PeerID: other, Name: "other"}}})
		if first {
			time.Sleep(400 * time.Millisecond)
			conn.WriteJSON(wire.ServerStopping{Type: wire.TypeServerStopping, ReconnectAfter: 1})
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseGoingAway, "restarting"))
			time.Sleep(30 * time.Millisecond)
			return
		}
		for {
			var m wire.MessageAfter
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			if m.Type != wire.TypeMessageAfter {
				continue
			}
			next := 0
			switch m.Cursor {
			case "p1":
				next = 1
			case "p2":
				next = 2
			}
			if m.Cursor == "" && m.At == "" {
				next = 0
			}
			if next >= len(history) {
				conn.WriteJSON(wire.NewNoMoreMessages(wire.Anchor{At: m.At, Cursor: m.Cursor}))
				continue
			}
			h := history[next]
			h.Type, h.Cursor, h.TS, h.Historical = wire.TypeMsg, []string{"p1", "p2"}[next], "2026-10-30T12:00:00Z", true
			h.Answers = &wire.Anchor{At: m.At, Cursor: m.Cursor}
			conn.WriteJSON(h)
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc-" + uniqueConvID() + "#quiet"

	h := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": link, "name": "t"}
	if res, err := h.handleConnect(context.Background(), req); err != nil || res.IsError {
		t.Fatalf("connect: %v %s", err, textOf(res))
	}
	defer h.handleDisconnect(context.Background(), connReqFor(testConn))
	waitUntil(t, "the first member list", func() bool { return in.has("already here") })
	in.mu.Lock()
	before := len(in.got)
	in.mu.Unlock()

	var note string
	waitUntil(t, "the notice about the restart", func() bool {
		if pushed {
			return in.has("Reconnected after the server")
		}
		note += h.takeAutoReconnectNote()
		return strings.Contains(note, "Reconnected after the server") && strings.Contains(note, wants[len(wants)-1])
	})
	time.Sleep(time.Second)
	in.mu.Lock()
	after := append([]string(nil), in.got[before:]...)
	in.mu.Unlock()
	want := 0
	if pushed {
		want = 1
	}
	if len(after) != want {
		t.Fatalf("a restart reached the reader as %d deliveries, want %d:\n%s", len(after), want, strings.Join(after, "\n---\n"))
	}
	if pushed {
		note = after[0]
	}
	for _, want := range append([]string{"Reconnected after the server's announced restart, away"}, wants...) {
		if !strings.Contains(note, want) {
			t.Fatalf("the notice lacks %q:\n%s", want, note)
		}
	}
	for _, unwanted := range []string{"DOWN", "shutting down", "RECONNECTED AUTOMATICALLY"} {
		if strings.Contains(note, unwanted) {
			t.Fatalf("the notice still says %q:\n%s", unwanted, note)
		}
	}
}

// A reconnect that is overdue is news: the drop is said then.
func TestAnOverdueReconnectSaysTheConnectionIsDown(t *testing.T) {
	prevGrace, prevMin := downGrace, downMin
	downGrace, downMin = 10*time.Millisecond, 10*time.Millisecond
	defer func() { downGrace, downMin = prevGrace, prevMin }()
	h := NewHub()
	s := &session{name: "relay", hub: h}
	s.reconnecting, s.reconnectGen = true, 3
	s.downLater(10*time.Millisecond, 3, "relay is DOWN")
	waitUntil(t, "the drop to be said", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.downNotified
	})
	if note := h.takeAutoReconnectNote(); !strings.Contains(note, "relay is DOWN") {
		t.Fatalf("note: %q", note)
	}

	back := &session{name: "relay", hub: h}
	back.reconnecting, back.reconnectGen = true, 1
	back.downLater(30*time.Millisecond, 1, "relay is DOWN")
	back.mu.Lock()
	back.reconnecting = false
	back.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	if back.downNotified || strings.Contains(h.takeAutoReconnectNote(), "DOWN") {
		t.Fatal("a reconnect in time still said the connection was down")
	}
}

// The member list inside the notice is the list itself, not a delivery
// with its own number and brackets.
func TestTheMemberListInTheRestartNoticeIsBare(t *testing.T) {
	got := strings.TrimSuffix(strings.TrimPrefix(hubconn.FormatEvent(hubconn.Event{Kind: "roster"}), "[hub: "), "]")
	if strings.HasPrefix(got, "[") || strings.HasSuffix(got, "]") || !strings.Contains(got, "alone") {
		t.Fatalf("line: %q", got)
	}
}
