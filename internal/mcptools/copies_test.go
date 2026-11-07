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

	"github.com/secforge/mcp-hub/internal/agenttest"
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
	if !in.has("this message is also in") {
		t.Fatal("the delivered message does not say it is also in the other copy")
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

// copyServer serves one connection per key: joined with thread, then the
// messages listed for that key at their delays. It records acks by key.
type copyServer struct {
	mu   sync.Mutex
	acks map[string][]string
	url  string
}

type timedMsg struct {
	after      time.Duration
	externalID string
	cursor     string
	text       string
}

func newCopyServer(t *testing.T, thread string, script map[string][]timedMsg) *copyServer {
	t.Helper()
	cs := &copyServer{acks: map[string][]string{}}
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("c")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		self := "550e8400-e29b-41d4-a716-4466554400" + map[string]string{"a": "00", "b": "01"}[key]
		f := teamsTestFeatures()
		f[wire.FeatureThreadIdentity] = json.RawMessage(`{}`)
		f["ackReplies"] = json.RawMessage(`{}`)
		f[wire.FeatureCorrelation] = json.RawMessage(`{}`)
		zero := 0
		conn.WriteJSON(wire.Joined{Type: wire.TypeJoined, PeerID: self, ServerVersion: wire.ProtocolVersion,
			Features: f, Behind: &zero, Thread: thread})
		conn.WriteJSON(wire.Roster{Type: wire.TypeRoster, Members: []wire.RosterMember{{PeerID: self, Name: "t"}}})
		var wmu sync.Mutex
		for _, m := range script[key] {
			m := m
			time.AfterFunc(m.after, func() {
				wmu.Lock()
				defer wmu.Unlock()
				conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
					Text: m.text, ExternalID: m.externalID, Cursor: m.cursor, TS: "2026-11-07T12:00:00Z"})
			})
		}
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
			cs.mu.Lock()
			cs.acks[key] = append(cs.acks[key], c)
			cs.mu.Unlock()
			ok := true
			wmu.Lock()
			conn.WriteJSON(wire.Ack{Type: wire.TypeAck, ID: id, AckCursor: c, OK: &ok})
			wmu.Unlock()
		}
	}))
	t.Cleanup(srv.Close)
	cs.url = "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c="
	return cs
}

func (cs *copyServer) acked(key, cursor string) bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, c := range cs.acks[key] {
		if c == cursor {
			return true
		}
	}
	return false
}

func connectCopy(t *testing.T, h *Hub, as, link string) {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": as, "link": link, "name": "t"}
	if res, err := h.handleConnect(context.Background(), req); err != nil || res.IsError {
		t.Fatalf("connect %s: %v %s", as, err, textOf(res))
	}
	t.Cleanup(func() { h.handleDisconnect(context.Background(), connReqFor(as)) })
}

// A thread value is the server's own word: the same value from two
// servers names two threads, and nothing is held back between them.
func TestTheSameThreadOnTwoServersIsNotACopy(t *testing.T) {
	in := newHarnessInbox(t)
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	one := newCopyServer(t, "thread-1", map[string][]timedMsg{"a": {{300 * time.Millisecond, "m1", "one-1", "from server one"}}})
	two := newCopyServer(t, "thread-1", map[string][]timedMsg{"b": {{400 * time.Millisecond, "m1", "two-1", "from server two"}}})
	h := NewHub()
	connectCopy(t, h, "copy-a", one.url+"a#s1-"+uniqueConvID())
	connectCopy(t, h, "copy-b", two.url+"b#s2-"+uniqueConvID())
	waitUntil(t, "both messages", func() bool { return in.has("from server one") && in.has("from server two") })
}

// A copy is not confirmed past a message its own connection handed over
// and the reader did not confirm.
func TestACopyIsNotConfirmedPastAnUnconfirmedMessage(t *testing.T) {
	in := newHarnessInbox(t)
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	cs := newCopyServer(t, "thread-1", map[string][]timedMsg{
		"a": {{300 * time.Millisecond, "m1", "a-1", "the shared message"}},
		"b": {
			{200 * time.Millisecond, "only-b", "b-0", "only on the second copy"},
			{600 * time.Millisecond, "m1", "b-1", "the shared message"},
		},
	})
	h := NewHub()
	connectCopy(t, h, "copy-a", cs.url+"a#c-"+uniqueConvID())
	connectCopy(t, h, "copy-b", cs.url+"b#c-"+uniqueConvID())
	waitUntil(t, "both messages", func() bool {
		return in.has("only on the second copy") && in.has("the shared message")
	})
	time.Sleep(600 * time.Millisecond)
	confirm := mcp.CallToolRequest{}
	confirm.Params.Arguments = map[string]any{"connection": "copy-a", "cursor": "a-1"}
	if res, _ := h.handleConfirmReceived(context.Background(), confirm); res == nil || res.IsError {
		t.Fatalf("confirm: %s", textOf(res))
	}
	time.Sleep(time.Second)
	if cs.acked("b", "b-1") {
		t.Fatal("the copy was confirmed past the second copy's own unconfirmed message")
	}
	// Once the reader confirms that message, the copy behind it follows.
	confirm.Params.Arguments = map[string]any{"connection": "copy-b", "cursor": "b-0"}
	if res, _ := h.handleConfirmReceived(context.Background(), confirm); res == nil || res.IsError {
		t.Fatalf("confirm b: %s", textOf(res))
	}
	waitUntil(t, "the copy confirmed after the reader's confirm", func() bool { return cs.acked("b", "b-1") })
}

// The conversation list says which account each conversation acts as.
func TestTheConversationListNamesTheAccount(t *testing.T) {
	g := agentGrant("conv-1", "Monitoring", wire.AccessReadWrite)
	g.Account = &wire.GrantAccount{ID: "ac_1", Name: "Steffen Heil", Channel: "teams"}
	srv := agenttest.New(t, g, agentGrant("conv-2", "Other", wire.AccessRead))
	h := NewHub()
	connectAgentForTest(t, h, srv)
	waitUntil(t, "both conversations", func() bool {
		return childNamed(t, h, "conv-1") != nil && childNamed(t, h, "conv-2") != nil
	})
	h.mu.Lock()
	ag := h.agents["desk"]
	h.mu.Unlock()
	list := strings.Join(ag.listing(listFilter{}), "\n")
	if !strings.Contains(list, `"Monitoring", teams group, as Steffen Heil`) {
		t.Fatalf("the list does not name the account:\n%s", list)
	}
	if strings.Contains(list, `"Other", teams group, as`) {
		t.Fatalf("a conversation without an account was given one:\n%s", list)
	}
	if got := childNamed(t, h, "conv-1").copyLabel(); !strings.Contains(got, "(as Steffen Heil)") {
		t.Fatalf("copy label %q does not name the account", got)
	}
}
