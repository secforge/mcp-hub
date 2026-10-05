package mcptools

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/secforge/harness-transport/deliver"

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/harness"
	"github.com/secforge/mcp-hub/internal/wire"
)

type recordingDeliverer struct {
	mu   sync.Mutex
	got  []deliver.Delivery
	fail bool
}

func (r *recordingDeliverer) Deliver(_ context.Context, d deliver.Delivery) (deliver.Receipt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, d)
	return deliver.Receipt{Observation: deliver.ObservedNothing}, nil
}
func (r *recordingDeliverer) Available() (bool, string)    { return true, "" }
func (r *recordingDeliverer) MaxIntactBytes() (int, error) { return 1 << 20, nil }
func (r *recordingDeliverer) Fits(deliver.Delivery) (bool, int, error) {
	return true, 0, nil
}
func (r *recordingDeliverer) Adopt(map[string]any) error { return nil }
func (r *recordingDeliverer) Close() error               { return nil }

// A catch-up delivery says "more is waiting" only when another message
// really follows it. The last one before the server's "nothing further"
// carries no such claim; a walk stopped by its own limit cannot know, and
// says nothing either.
func TestCatchUpSaysMoreIsWaitingOnlyWhenAnotherMessageFollows(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	const backlog = 3
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000",
			ServerVersion: wire.ProtocolVersion, Features: teamsTestFeatures(), ConversationKind: "group"}
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		for {
			var m wire.MessageAfter
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			next := 1
			if m.Cursor != "" {
				fmt.Sscanf(m.Cursor, "c%d", &next)
				next++
			}
			if next > backlog {
				conn.WriteJSON(wire.NewNoMoreMessages(wire.Anchor{At: m.At, Cursor: m.Cursor}))
				continue
			}
			conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
				Cursor: fmt.Sprintf("c%d", next), Text: fmt.Sprintf("message %d", next),
				TS: "ts", Historical: true, Answers: &wire.Anchor{At: m.At, Cursor: m.Cursor}})
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc-" + uniqueConvID() + "#more-secret"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"as": testConn, "link": link}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, connReqFor(testConn))
	s, err := hub.session(testConn)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	conn, _ := s.activeConn()

	for _, tc := range []struct {
		name     string
		want     catchUpWant
		wantMore []bool
		caughtUp bool
	}{
		{"walked to the end", catchUpWant{}, []bool{true, true, false}, true},
		{"stopped by its own limit", catchUpWant{Messages: 2}, []bool{true, false}, false},
	} {
		rec := &recordingDeliverer{}
		s.pusher = harness.PusherForTesting(rec)
		res := s.walkCatchUpPush(conn, connstore.Target{}, wire.Anchor{}, tc.want)
		if res.Err != nil || res.CaughtUp != tc.caughtUp || res.Delivered != len(tc.wantMore) {
			t.Fatalf("%s: unexpected ending %+v", tc.name, res)
		}
		var more []bool
		for _, d := range rec.got {
			more = append(more, d.More)
		}
		if fmt.Sprint(more) != fmt.Sprint(tc.wantMore) {
			t.Errorf("%s: more flags %v, want %v", tc.name, more, tc.wantMore)
		}
	}
}
