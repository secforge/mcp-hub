package mcptools

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/wire"
)

func TestOnlyWhatThisConnectionDidCountsAsItsEcho(t *testing.T) {
	cases := []struct {
		name string
		ev   hubconn.Event
		want bool
	}{
		{"own message", hubconn.Event{Kind: "msg", Own: true}, true},
		{"own reaction", hubconn.Event{Kind: "reactionChanged", Own: true}, true},
		{"own edit", hubconn.Event{Kind: "messageEdited", Own: true}, true},
		{"same account, written elsewhere", hubconn.Event{Kind: "msg", OwnAccount: true}, false},
		{"someone else", hubconn.Event{Kind: "msg"}, false},
		{"own presence", hubconn.Event{Kind: "msg", Own: true, Presence: &wire.Presence{Event: "left"}}, false},
	}
	for _, c := range cases {
		if got := isOwnEcho(c.ev); got != c.want {
			t.Errorf("%s: isOwnEcho = %v, want %v", c.name, got, c.want)
		}
	}
}

// An echo the server sends unmarked — after a reconnect it no longer knows
// what this socket sent — is still known by the id its sendAck named, and
// not delivered unless asked for.
func TestAnUnmarkedEchoOfASendIsKnownByItsId(t *testing.T) {
	for _, want := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "ownEchoes"}[want], func(t *testing.T) {
			in := newHarnessInbox(t)
			t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
			const self = "550e8400-e29b-41d4-a716-446655440000"
			upgrader := websocket.Upgrader{}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				zero := 0
				conn.WriteJSON(wire.Joined{Type: wire.TypeJoined, PeerID: self, ServerVersion: wire.ProtocolVersion,
					Features: teamsTestFeatures(), Behind: &zero})
				conn.WriteJSON(wire.Roster{Type: wire.TypeRoster, Members: []wire.RosterMember{{PeerID: self, Name: "t"}}})
				for {
					var req map[string]any
					if err := conn.ReadJSON(&req); err != nil {
						return
					}
					if req["type"] != string(wire.TypeMsg) {
						continue
					}
					id, _ := req["id"].(string)
					conn.WriteJSON(wire.SendAck{Type: wire.TypeSendAck, ID: id, ExternalID: "x1", OK: true})
					time.Sleep(50 * time.Millisecond)
					conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: self, Text: "echo of my send",
						ExternalID: "x1", Cursor: "e1", TS: "2026-11-07T12:00:00Z"})
					time.Sleep(50 * time.Millisecond)
					conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
						Text: "a reply from someone", ExternalID: "x2", Cursor: "e2", TS: "2026-11-07T12:00:01Z"})
				}
			}))
			t.Cleanup(srv.Close)
			link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc-" + uniqueConvID() + "#echo"
			h := NewHub()
			req := mcp.CallToolRequest{}
			req.Params.Arguments = map[string]any{"as": testConn, "link": link, "name": "t", "ownEchoes": want}
			if res, err := h.handleConnect(context.Background(), req); err != nil || res.IsError {
				t.Fatalf("connect: %v %s", err, textOf(res))
			}
			defer h.handleDisconnect(context.Background(), connReqFor(testConn))
			send := mcp.CallToolRequest{}
			send.Params.Arguments = map[string]any{"connection": testConn, "text": "my send"}
			if res, _ := h.handleSend(context.Background(), send); res == nil || res.IsError {
				t.Fatalf("send: %s", textOf(res))
			}
			waitUntil(t, "the reply", func() bool { return in.has("a reply from someone") })
			if got := in.has("echo of my send"); got != want {
				t.Fatalf("echo delivered = %v, want %v", got, want)
			}
		})
	}
}
