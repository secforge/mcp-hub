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

// startRelayTestServer starts a bare websocket server (not wsserver.NewHandler
// — teams_relay_connect deliberately doesn't dial that) that captures the
// handshake headers it received and sends a "joined" message, for exercising
// handleTeamsRelayConnect without a real chat-relay-style server.
func startRelayTestServer(t *testing.T) (link string, gotHeaders func() http.Header) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	var captured http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = r.Header.Clone()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	link = "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc#the-link-secret"
	return link, func() http.Header { return captured }
}

// startRelayTestServerWithJoined is startRelayTestServer but sending a
// caller-supplied "joined" message, for exercising the bridge-only fields
// (latestCursor, historyLimitMax, canSend, conversationKind, topic).
func startRelayTestServerWithJoined(t *testing.T, joined wire.Joined) (link string) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc#the-link-secret"
}

func TestTeamsRelayConnectSendsHeadersAndReturnsGuidance(t *testing.T) {
	link, headers := startRelayTestServer(t)
	ctx := context.Background()

	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"link": link, "name": "tester", "reconnectSecret": "resume-me"}
	res, err := hub.handleTeamsRelayConnect(ctx, req)
	if err != nil || res.IsError {
		t.Fatalf("teams_relay_connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	if got := headers().Get("Authorization"); got != "Bearer the-link-secret" {
		t.Fatalf("expected Authorization header, got %q", got)
	}
	if got := headers().Get("Reconnect-Secret"); got != "resume-me" {
		t.Fatalf("expected Reconnect-Secret header, got %q", got)
	}
	if got := headers().Get("Agent-Name"); got != "tester" {
		t.Fatalf("expected Agent-Name header, got %q", got)
	}

	text := textOf(res)
	if !strings.Contains(text, "Connected as peer") {
		t.Fatalf("expected a connected confirmation, got: %s", text)
	}
	if !strings.Contains(text, "chat-relay bridge session") {
		t.Fatalf("expected bridge-specific guidance, got: %s", text)
	}
	if !strings.Contains(text, "hub_catch_up()") {
		t.Fatalf("expected a mention of hub_catch_up, got: %s", text)
	}
}

// TestHubWaitDoesNotWakeOnOwnMessageAloneButDeliversItAlongside is the
// end-to-end regression test for own-message wake suppression: hub_wait
// must not return while only an own-send echo is buffered, and once a
// real (non-own) message arrives it must deliver both together.
func TestHubWaitDoesNotWakeOnOwnMessageAloneButDeliversItAlongside(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)

		own := wire.NewBroadcastMsg("550e8400-e29b-41d4-a716-446655440000", "my own echo", "ts1", nil, "", "", nil)
		own.Own = true
		own.ExternalID = "ext-1"
		conn.WriteJSON(own)

		time.Sleep(300 * time.Millisecond) // give handleWait time to observe it's not woken yet

		reply := wire.NewBroadcastMsg("550e8400-e29b-41d4-a716-446655440001", "their reply", "ts2", nil, "", "", nil)
		conn.WriteJSON(reply)

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc#the-link-secret"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	resultCh := make(chan *mcp.CallToolResult, 1)
	go func() {
		res, _ := hub.handleWait(ctx, mcp.CallToolRequest{})
		resultCh <- res
	}()

	select {
	case res := <-resultCh:
		t.Fatalf("expected hub_wait to stay blocked on an own-only buffer, but it returned: %s", textOf(res))
	case <-time.After(150 * time.Millisecond):
		// expected: still blocked, own message alone isn't wake-worthy
	}

	select {
	case res := <-resultCh:
		text := textOf(res)
		if !strings.Contains(text, "my own echo") || !strings.Contains(text, "their reply") {
			t.Fatalf("expected both the held-back own message and the reply delivered together, got: %s", text)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("hub_wait never returned after the real reply arrived")
	}
}

// startRelayTestServerCapturingClientMessages is startRelayTestServer but
// forwards every raw message the client sends after connecting onto a
// channel, for exercising outbound requests like hub_react/hub_edit.
func startRelayTestServerCapturingClientMessages(t *testing.T) (link string, gotRaw chan []byte) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	gotRaw = make(chan []byte, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			gotRaw <- msg
		}
	}))
	t.Cleanup(srv.Close)
	link = "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc#the-link-secret"
	return link, gotRaw
}

// startRelayTestServerEchoingAcks is startRelayTestServer but replies to a
// reaction/edit/msg request with a matching ack immediately — for
// exercising the synchronous claimNextAck path (SendAwaitingAck/
// ReactAwaitingAck/EditMessageAwaitingAck) end to end through the actual
// MCP tool handlers, rather than timing out waiting for one.
func startRelayTestServerEchoingAcks(t *testing.T) (link string) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			typ, err := wire.DecodeType(msg)
			if err != nil {
				continue
			}
			switch typ {
			case wire.TypeReaction:
				var req wire.Reaction
				json.Unmarshal(msg, &req)
				conn.WriteJSON(wire.ReactionAck{
					Type: wire.TypeReactionAck, ExternalID: req.ExternalID,
					Reaction: req.Reaction, Action: req.Action, OK: true,
				})
			case wire.TypeEdit:
				var req wire.Edit
				json.Unmarshal(msg, &req)
				conn.WriteJSON(wire.EditAck{Type: wire.TypeEditAck, ExternalID: req.ExternalID, OK: true})
			case wire.TypeDelete:
				var req wire.Delete
				json.Unmarshal(msg, &req)
				conn.WriteJSON(wire.DeleteAck{Type: wire.TypeDeleteAck, ExternalID: req.ExternalID, OK: true})
			case wire.TypeMsg:
				conn.WriteJSON(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "ext-echo", OK: true})
			}
		}
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc#the-link-secret"
}

func TestHubReactSendsReactionRequest(t *testing.T) {
	link, gotRaw := startRelayTestServerCapturingClientMessages(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	// This server never acks, so the wait genuinely times out here —
	// shorten it so the test doesn't pay the real AckWaitTimeout.
	orig := hubconn.AckWaitTimeout
	hubconn.AckWaitTimeout = 200 * time.Millisecond
	defer func() { hubconn.AckWaitTimeout = orig }()

	reactReq := mcp.CallToolRequest{}
	reactReq.Params.Arguments = map[string]any{"externalId": "ext-1", "reaction": "👍", "action": "add"}
	res, err := hub.handleReact(ctx, reactReq)
	if err != nil || res.IsError {
		t.Fatalf("hub_react failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "reaction request sent") || !strings.Contains(textOf(res), "no acknowledgement") {
		t.Fatalf("expected a timeout confirmation, got: %s", textOf(res))
	}

	select {
	case raw := <-gotRaw:
		var r wire.Reaction
		if err := json.Unmarshal(raw, &r); err != nil {
			t.Fatalf("unmarshal sent reaction: %v", err)
		}
		if r.ExternalID != "ext-1" || r.Reaction != "👍" || r.Action != "add" {
			t.Fatalf("unexpected reaction request on the wire: %+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the reaction request")
	}
}

func TestHubReactRejectsInvalidAction(t *testing.T) {
	link, _ := startRelayTestServer(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	reactReq := mcp.CallToolRequest{}
	reactReq.Params.Arguments = map[string]any{"externalId": "ext-1", "reaction": "👍", "action": "toggle"}
	res, err := hub.handleReact(ctx, reactReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected an error for an invalid action, got: %+v", res)
	}
}

func TestHubReactErrorsWhenNotConnected(t *testing.T) {
	hub := NewHub()
	res, err := hub.handleReact(context.Background(), mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError || !strings.Contains(textOf(res), "not connected") {
		t.Fatalf("expected a not-connected error, got: %+v", res)
	}
}

func TestHubEditSendsEditRequest(t *testing.T) {
	link, gotRaw := startRelayTestServerCapturingClientMessages(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	orig := hubconn.AckWaitTimeout
	hubconn.AckWaitTimeout = 200 * time.Millisecond
	defer func() { hubconn.AckWaitTimeout = orig }()

	editReq := mcp.CallToolRequest{}
	editReq.Params.Arguments = map[string]any{"externalId": "ext-1", "text": "corrected"}
	res, err := hub.handleEdit(ctx, editReq)
	if err != nil || res.IsError {
		t.Fatalf("hub_edit failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "edit request sent") || !strings.Contains(textOf(res), "no acknowledgement") {
		t.Fatalf("expected a timeout confirmation, got: %s", textOf(res))
	}

	select {
	case raw := <-gotRaw:
		var e wire.Edit
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatalf("unmarshal sent edit: %v", err)
		}
		if e.ExternalID != "ext-1" || e.Text != "corrected" {
			t.Fatalf("unexpected edit request on the wire: %+v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the edit request")
	}
}

func TestHubSendWithMentionsSendsMentionsOnTheWire(t *testing.T) {
	link, gotRaw := startRelayTestServerCapturingClientMessages(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	orig := hubconn.AckWaitTimeout
	hubconn.AckWaitTimeout = 200 * time.Millisecond
	defer func() { hubconn.AckWaitTimeout = orig }()

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{
		"text": "hi @Steffen",
		"mentions": []any{
			map[string]any{"peerId": "550e8400-e29b-41d4-a716-446655440000", "text": "@Steffen"},
		},
	}
	res, err := hub.handleSend(ctx, sendReq)
	if err != nil || res.IsError {
		t.Fatalf("hub_send failed: err=%v result=%+v", err, res)
	}

	select {
	case raw := <-gotRaw:
		var m wire.Msg
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("unmarshal sent msg: %v", err)
		}
		if len(m.Mentions) != 1 || m.Mentions[0].PeerID != "550e8400-e29b-41d4-a716-446655440000" || m.Mentions[0].Text != "@Steffen" {
			t.Fatalf("unexpected mentions on the wire: %+v", m.Mentions)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the send")
	}
}

func TestHubSendRejectsMentionWithoutExactlyOneIdentifier(t *testing.T) {
	link, _ := startRelayTestServer(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{
		"text": "hi",
		"mentions": []any{
			map[string]any{"peerId": "550e8400-e29b-41d4-a716-446655440000", "name": "Steffen"},
		},
	}
	res, err := hub.handleSend(ctx, sendReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError || !strings.Contains(textOf(res), "exactly one") {
		t.Fatalf("expected a client-side validation error for two identifiers set, got: %+v", res)
	}
}

func TestHubEditWithMentionsSendsMentionsOnTheWire(t *testing.T) {
	link, gotRaw := startRelayTestServerCapturingClientMessages(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	orig := hubconn.AckWaitTimeout
	hubconn.AckWaitTimeout = 200 * time.Millisecond
	defer func() { hubconn.AckWaitTimeout = orig }()

	editReq := mcp.CallToolRequest{}
	editReq.Params.Arguments = map[string]any{
		"externalId": "ext-1",
		"text":       "corrected @Steffen",
		"mentions":   []any{map[string]any{"name": "Steffen"}},
	}
	res, err := hub.handleEdit(ctx, editReq)
	if err != nil || res.IsError {
		t.Fatalf("hub_edit failed: err=%v result=%+v", err, res)
	}

	select {
	case raw := <-gotRaw:
		var e wire.Edit
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatalf("unmarshal sent edit: %v", err)
		}
		if len(e.Mentions) != 1 || e.Mentions[0].Name != "Steffen" {
			t.Fatalf("unexpected mentions on the wire: %+v", e.Mentions)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the edit request")
	}
}

// TestHubReactReportsAckDirectly is the core regression test for the
// synchronous-ack feature: hub_react must report the actual reactionAck
// (not a generic "sent" confirmation) when one arrives before
// AckWaitTimeout, so a caller learns the real outcome — including
// failure — directly from this call instead of having to separately poll
// wait/hub_receive/hub_wait.
func TestHubReactReportsAckDirectly(t *testing.T) {
	link := startRelayTestServerEchoingAcks(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	reactReq := mcp.CallToolRequest{}
	reactReq.Params.Arguments = map[string]any{"externalId": "ext-1", "reaction": "👍", "action": "add"}
	res, err := hub.handleReact(ctx, reactReq)
	if err != nil || res.IsError {
		t.Fatalf("hub_react failed: err=%v result=%+v", err, res)
	}
	text := textOf(res)
	if !strings.Contains(text, "acknowledged") || !strings.Contains(text, "ext-1") {
		t.Fatalf("expected the reactionAck reported directly, got: %s", text)
	}
	if strings.Contains(text, "reaction request sent") {
		t.Fatalf("expected the real ack, not the fallback confirmation text, got: %s", text)
	}
}

func TestHubEditReportsAckDirectly(t *testing.T) {
	link := startRelayTestServerEchoingAcks(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	editReq := mcp.CallToolRequest{}
	editReq.Params.Arguments = map[string]any{"externalId": "ext-1", "text": "corrected"}
	res, err := hub.handleEdit(ctx, editReq)
	if err != nil || res.IsError {
		t.Fatalf("hub_edit failed: err=%v result=%+v", err, res)
	}
	text := textOf(res)
	if !strings.Contains(text, "acknowledged") || !strings.Contains(text, "ext-1") {
		t.Fatalf("expected the editAck reported directly, got: %s", text)
	}
	if strings.Contains(text, "edit request sent") {
		t.Fatalf("expected the real ack, not the fallback confirmation text, got: %s", text)
	}
}

func TestHubDeleteReportsAckDirectly(t *testing.T) {
	link := startRelayTestServerEchoingAcks(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	delReq := mcp.CallToolRequest{}
	delReq.Params.Arguments = map[string]any{"externalId": "ext-1"}
	res, err := hub.handleDelete(ctx, delReq)
	if err != nil || res.IsError {
		t.Fatalf("hub_delete failed: err=%v result=%+v", err, res)
	}
	text := textOf(res)
	if !strings.Contains(text, "acknowledged") || !strings.Contains(text, "ext-1") {
		t.Fatalf("expected the deleteAck reported directly, got: %s", text)
	}
	if strings.Contains(text, "delete request sent") {
		t.Fatalf("expected the real ack, not the fallback confirmation text, got: %s", text)
	}
}

func TestHubDeleteSendsDeleteRequestAndFallsBackOnTimeout(t *testing.T) {
	link, gotRaw := startRelayTestServerCapturingClientMessages(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	orig := hubconn.AckWaitTimeout
	hubconn.AckWaitTimeout = 200 * time.Millisecond
	defer func() { hubconn.AckWaitTimeout = orig }()

	delReq := mcp.CallToolRequest{}
	delReq.Params.Arguments = map[string]any{"externalId": "ext-1"}
	res, err := hub.handleDelete(ctx, delReq)
	if err != nil || res.IsError {
		t.Fatalf("hub_delete failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "delete request sent") || !strings.Contains(textOf(res), "no acknowledgement") {
		t.Fatalf("expected a timeout confirmation, got: %s", textOf(res))
	}

	select {
	case raw := <-gotRaw:
		var d wire.Delete
		if err := json.Unmarshal(raw, &d); err != nil {
			t.Fatalf("unmarshal sent delete: %v", err)
		}
		if d.ExternalID != "ext-1" {
			t.Fatalf("unexpected delete request on the wire: %+v", d)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the delete request")
	}
}

func TestHubDeleteErrorsWhenNotConnected(t *testing.T) {
	hub := NewHub()
	res, err := hub.handleDelete(context.Background(), mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError || !strings.Contains(textOf(res), "not connected") {
		t.Fatalf("expected a not-connected error, got: %+v", res)
	}
}

func TestHubSendReportsAckDirectlyOnBridgeSession(t *testing.T) {
	link := startRelayTestServerEchoingAcks(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "hello bridge"}
	res, err := hub.handleSend(ctx, sendReq)
	if err != nil || res.IsError {
		t.Fatalf("hub_send failed: err=%v result=%+v", err, res)
	}
	text := textOf(res)
	if !strings.Contains(text, "acknowledged") {
		t.Fatalf("expected the sendAck reported directly on a bridge session, got: %s", text)
	}
	if text == "sent" {
		t.Fatal("expected the real ack, not the bare plain-session confirmation")
	}
}

// TestHubSendDoesNotWaitOnAPlainHubConnectSession proves the IsBridge gate:
// a normal hub_connect session must return its bare "sent" confirmation
// immediately, with no ack-wait latency, since mcp-hub-server never emits
// a sendAck at all.
func TestHubSendDoesNotWaitOnAPlainHubConnectSession(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"host": url, "sessionId": sessionID}
	if res, err := hub.handleConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	sendReq := mcp.CallToolRequest{}
	sendReq.Params.Arguments = map[string]any{"text": "hello"}
	start := time.Now()
	res, err := hub.handleSend(ctx, sendReq)
	elapsed := time.Since(start)
	if err != nil || res.IsError {
		t.Fatalf("handleSend failed: err=%v result=%+v", err, res)
	}
	if textOf(res) != "sent" {
		t.Fatalf(`expected the bare "sent" confirmation, got: %s`, textOf(res))
	}
	if elapsed > time.Second {
		t.Fatalf("expected no ack-wait latency on a plain session, took %v", elapsed)
	}
}

func TestHubEditErrorsWhenNotConnected(t *testing.T) {
	hub := NewHub()
	res, err := hub.handleEdit(context.Background(), mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError || !strings.Contains(textOf(res), "not connected") {
		t.Fatalf("expected a not-connected error, got: %+v", res)
	}
}

func TestTeamsRelayConnectSurfacesBridgeFields(t *testing.T) {
	topic := "Support chat"
	joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
	joined.CanSend = true
	joined.ConversationKind = "oneOnOne"
	joined.Topic = &topic
	link := startRelayTestServerWithJoined(t, joined)
	ctx := context.Background()

	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	res, err := hub.handleTeamsRelayConnect(ctx, req)
	if err != nil || res.IsError {
		t.Fatalf("teams_relay_connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	text := textOf(res)
	for _, want := range []string{
		"oneOnOne", `"Support chat"`,
		"Sending is currently permitted", "hub_catch_up()",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected result to mention %q, got: %s", want, text)
		}
	}
}

func TestTeamsRelayConnectNotesSendRefused(t *testing.T) {
	joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
	joined.CanSend = false
	link := startRelayTestServerWithJoined(t, joined)
	ctx := context.Background()

	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	res, err := hub.handleTeamsRelayConnect(ctx, req)
	if err != nil || res.IsError {
		t.Fatalf("teams_relay_connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	text := textOf(res)
	if !strings.Contains(text, "NOT currently permitted") {
		t.Fatalf("expected a sending-not-permitted note, got: %s", text)
	}
}

func TestTeamsRelayConnectRequiresReconnectSecret(t *testing.T) {
	link, _ := startRelayTestServer(t)
	ctx := context.Background()

	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"link": link}
	res, err := hub.handleTeamsRelayConnect(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError || !strings.Contains(textOf(res), "reconnectSecret") {
		t.Fatalf("expected a clear error about the missing reconnectSecret, got: %+v", res)
	}
}

func TestTeamsRelayConnectRejectsLinkWithoutFragment(t *testing.T) {
	ctx := context.Background()
	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{
		"link": "wss://example.com/relay/join?c=abc", "reconnectSecret": "x",
	}
	res, err := hub.handleTeamsRelayConnect(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected an error for a link with no #-delimited secret, got: %+v", res)
	}
}

// TestCatchUpWithNoPriorPositionAndNoBehindReportsNothingToCatchUp covers
// the common case for a fresh connection: no lastHandedOverCursor yet
// (nothing pulled before) and the server didn't report Behind (or
// reported 0) — there's nothing to walk or seek from, so this should
// say so rather than send a request at all.
func TestCatchUpWithNoPriorPositionAndNoBehindReportsNothingToCatchUp(t *testing.T) {
	link := startRelayTestServerWithJoined(t, wire.Joined{
		Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000", ServerVersion: wire.ProtocolVersion,
	})
	ctx := context.Background()
	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	res, err := hub.handleCatchUp(ctx, mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("hub_catch_up failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "nothing to catch up") {
		t.Fatalf("expected a nothing-to-catch-up result, got: %s", textOf(res))
	}
}

// TestCatchUpWithPriorPositionSendsMessageAfterWithCursor covers the
// normal walk case: a prior lastHandedOverCursor exists, so the tool
// should send messageAfter{cursor: ...} and, on the answering msg,
// return it as a single-message tool result and advance the persisted
// position to the new message's own cursor.
func TestCatchUpWithPriorPositionSendsMessageAfterWithCursor(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotReq := make(chan wire.MessageAfter, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000", ServerVersion: wire.ProtocolVersion}
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		var m wire.MessageAfter
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		gotReq <- m
		conn.WriteJSON(wire.Msg{
			Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "the next one", TS: "ts1",
			Historical: true, Cursor: "cursor-2", ExternalID: "ext-1",
			Answers: &wire.Anchor{Cursor: "cursor-1"},
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc#the-link-secret"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	hub.mu.Lock()
	hub.lastHandedOverCursor = "cursor-1"
	hub.mu.Unlock()

	res, err := hub.handleCatchUp(ctx, mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("hub_catch_up failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "the next one") {
		t.Fatalf("expected the returned message text, got: %s", textOf(res))
	}
	if !strings.Contains(textOf(res), "more may remain") {
		t.Fatalf("expected the more-may-remain hint, got: %s", textOf(res))
	}

	select {
	case m := <-gotReq:
		if m.Type != wire.TypeMessageAfter || m.Cursor != "cursor-1" || m.At != "" {
			t.Fatalf("unexpected request: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the messageAfter request")
	}

	hub.mu.Lock()
	got := hub.lastHandedOverCursor
	hub.mu.Unlock()
	if got != "cursor-2" {
		t.Fatalf("expected lastHandedOverCursor advanced to cursor-2, got %q", got)
	}
}

// TestCatchUpReportsCaughtUpOnNoMoreMessages covers the terminator: the
// server answering noMoreMessages should produce a distinct "caught up"
// result, and must NOT advance lastHandedOverCursor (nothing was handed
// over).
func TestCatchUpReportsCaughtUpOnNoMoreMessages(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000", ServerVersion: wire.ProtocolVersion}
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		var m wire.MessageAfter
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		conn.WriteJSON(wire.NewNoMoreMessages(wire.Anchor{Cursor: "cursor-1"}))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc#the-link-secret"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	hub.mu.Lock()
	hub.lastHandedOverCursor = "cursor-1"
	hub.mu.Unlock()

	res, err := hub.handleCatchUp(ctx, mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("hub_catch_up failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "caught up") {
		t.Fatalf("expected a caught-up result, got: %s", textOf(res))
	}

	hub.mu.Lock()
	got := hub.lastHandedOverCursor
	hub.mu.Unlock()
	if got != "cursor-1" {
		t.Fatalf("expected lastHandedOverCursor unchanged at cursor-1, got %q", got)
	}
}

// TestCatchUpSeeksWhenNoPriorCursorButBehindReported covers the seek
// fallback: no lastHandedOverCursor yet, but the server reported Behind
// > 0 at connect — the tool should send messageAfter{at: ...} rather
// than reporting "nothing to catch up".
func TestCatchUpSeeksWhenNoPriorCursorButBehindReported(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotReq := make(chan wire.MessageAfter, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.Joined{
			Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000", ServerVersion: wire.ProtocolVersion,
			Behind: 3000, BehindSince: "2026-09-01T09:12:00Z",
		}
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		var m wire.MessageAfter
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		gotReq <- m
		conn.WriteJSON(wire.NewNoMoreMessages(wire.Anchor{At: m.At}))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc#the-link-secret"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	res, err := hub.handleCatchUp(ctx, mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("hub_catch_up failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "seeking to recent context") {
		t.Fatalf("expected a seek note, got: %s", textOf(res))
	}

	select {
	case m := <-gotReq:
		if m.Type != wire.TypeMessageAfter || m.At == "" || m.Cursor != "" {
			t.Fatalf("expected an at-anchored seek request, got: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the messageAfter request")
	}
}

// TestTeamsRelayConnectSurfacesBehindWhenServerReportsIt proves the
// connect-time framing states "you were away" as a fact from
// wire.Joined.Behind/BehindSince, rather than leaving the model to
// notice it's missing something.
func TestTeamsRelayConnectSurfacesBehindWhenServerReportsIt(t *testing.T) {
	link := startRelayTestServerWithJoined(t, wire.Joined{
		Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000", ServerVersion: wire.ProtocolVersion,
		Behind: 3, BehindSince: "2026-09-01T09:12:00Z",
	})
	ctx := context.Background()
	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	res, err := hub.handleTeamsRelayConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	text := textOf(res)
	if !strings.Contains(text, "You were away: 3 message") || !strings.Contains(text, "hub_catch_up") {
		t.Fatalf("expected a behind note mentioning hub_catch_up, got: %s", text)
	}
}

// TestTeamsRelayConnectOmitsBehindWhenServerDoesNotReportIt confirms no
// note appears when a server doesn't set Behind (the common case today).
func TestTeamsRelayConnectOmitsBehindWhenServerDoesNotReportIt(t *testing.T) {
	link := startRelayTestServerWithJoined(t, wire.Joined{
		Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000", ServerVersion: wire.ProtocolVersion,
	})
	ctx := context.Background()
	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	res, err := hub.handleTeamsRelayConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	if strings.Contains(textOf(res), "You were away") {
		t.Fatalf("expected no behind note, got: %s", textOf(res))
	}
}

// TestCatchUpPersistsCursorAcrossHubInstances is the end-to-end version
// of TestSetCatchUpKeyPersistsAcrossHubInstances: a real hub_catch_up
// call against a live connection persists the returned message's cursor,
// and a second *Hub connecting to the same link+project recovers it as
// its starting anchor without ever receiving anything live itself.
func TestCatchUpPersistsCursorAcrossHubInstances(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotFirstReq := make(chan wire.MessageAfter, 1)
	link := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000", ServerVersion: wire.ProtocolVersion}
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		var m wire.MessageAfter
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		gotFirstReq <- m
		conn.WriteJSON(wire.Msg{
			Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "persisted one", TS: "ts1",
			Historical: true, Cursor: "cursor-persisted", ExternalID: "ext-1",
			Answers: &wire.Anchor{Cursor: "cursor-0"},
		})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	link = "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc#the-link-secret"
	ctx := context.Background()

	first := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := first.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	first.mu.Lock()
	first.lastHandedOverCursor = "cursor-0"
	first.mu.Unlock()

	if res, err := first.handleCatchUp(ctx, mcp.CallToolRequest{}); err != nil || res.IsError {
		t.Fatalf("hub_catch_up failed: err=%v result=%+v", err, res)
	}
	first.handleDisconnect(ctx, mcp.CallToolRequest{})

	select {
	case <-gotFirstReq:
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the first messageAfter request")
	}

	// A second, independent *Hub (standing in for a fresh process)
	// connecting to the same link+project should recover cursor-persisted
	// without any live traffic telling it — pure persistence.
	second := NewHub()
	key := catchUpKeyForRelay(ctx, link)
	second.setCatchUpKey(key)

	second.mu.Lock()
	got := second.lastHandedOverCursor
	second.mu.Unlock()
	if got != "cursor-persisted" {
		t.Fatalf("expected the second Hub to recover cursor-persisted, got %q", got)
	}
}

// TestCatchUpSkipsMessageAlreadyHandedOverViaHubReceive is the core
// dedup regression test: a message already shown to the model via
// hub_receive (a confirmed synchronous hand-over) must NOT be shown
// again by hub_catch_up — the walk should advance past it silently and
// return the next genuinely new message instead.
func TestCatchUpSkipsMessageAlreadyHandedOverViaHubReceive(t *testing.T) {
	upgrader := websocket.Upgrader{}
	var reqLog []wire.MessageAfter
	var reqMu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000", ServerVersion: wire.ProtocolVersion}
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)

		// Live traffic: a message the model will see via hub_receive
		// BEFORE ever calling hub_catch_up.
		live := wire.NewBroadcastMsg("6ba7b810-9dad-11d1-80b4-00c04fd430c8", "seen live first", "ts-live", nil, "", "", nil)
		live.Cursor = "cursor-live-1"
		live.ExternalID = "ext-live-1"
		conn.WriteJSON(live)

		for {
			var m wire.MessageAfter
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			reqMu.Lock()
			reqLog = append(reqLog, m)
			reqMu.Unlock()
			switch m.Cursor {
			case "cursor-0":
				// First catch-up call walks straight into the message
				// already seen live — must be skipped, not shown again.
				conn.WriteJSON(wire.Msg{
					Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "seen live first", TS: "ts-live",
					Historical: true, Cursor: "cursor-live-1", ExternalID: "ext-live-1",
					Answers: &wire.Anchor{Cursor: "cursor-0"},
				})
			case "cursor-live-1":
				// Second internal step: the genuinely new message beyond it.
				conn.WriteJSON(wire.Msg{
					Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "genuinely new", TS: "ts-new",
					Historical: true, Cursor: "cursor-new-1", ExternalID: "ext-new-1",
					Answers: &wire.Anchor{Cursor: "cursor-live-1"},
				})
			default:
				conn.WriteJSON(wire.NewNoMoreMessages(wire.Anchor{Cursor: m.Cursor}))
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc#the-link-secret"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	// The model reads live traffic first, via hub_receive — this is what
	// populates handedOverAhead with cursor-live-1.
	deadlinePoll(t, func() bool {
		conn, _ := hub.activeConn()
		hasEvents, _ := conn.Peek()
		return hasEvents
	})
	res, err := hub.handleReceive(ctx, mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("hub_receive failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "seen live first") {
		t.Fatalf("expected the live message via hub_receive, got: %s", textOf(res))
	}

	// Now catch up from a position before it — the walk should skip the
	// already-seen message and return "genuinely new" instead.
	hub.mu.Lock()
	hub.lastHandedOverCursor = "cursor-0"
	hub.mu.Unlock()

	res, err = hub.handleCatchUp(ctx, mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("hub_catch_up failed: err=%v result=%+v", err, res)
	}
	text := textOf(res)
	if strings.Contains(text, "seen live first") {
		t.Fatalf("expected the already-seen message to be skipped, not re-shown, got: %s", text)
	}
	if !strings.Contains(text, "genuinely new") {
		t.Fatalf("expected the genuinely new message, got: %s", text)
	}

	reqMu.Lock()
	got := len(reqLog)
	reqMu.Unlock()
	if got != 2 {
		t.Fatalf("expected exactly 2 internal messageAfter calls (skip + find), got %d: %+v", got, reqLog)
	}
}

// TestCatchUpSeekRecordsGapAndSurfacesItOnLaterCalls proves the seek-gap
// is persisted as ongoing state — recorded when a real seek happens
// (Behind above the threshold), and still mentioned on a LATER,
// unrelated hub_catch_up call that has nothing to do with creating it.
func TestCatchUpSeekRecordsGapAndSurfacesItOnLaterCalls(t *testing.T) {
	upgrader := websocket.Upgrader{}
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.Joined{
			Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000", ServerVersion: wire.ProtocolVersion,
			Behind: 3000, BehindSince: "2026-09-01T09:12:00Z",
		}
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		for {
			var m wire.MessageAfter
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			callCount++
			conn.WriteJSON(wire.NewNoMoreMessages(wire.Anchor{At: m.At, Cursor: m.Cursor}))
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc#the-link-secret"
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	// First call: performs the seek, records the gap.
	res, err := hub.handleCatchUp(ctx, mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("hub_catch_up (seek) failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "now recorded") {
		t.Fatalf("expected the seek's own result to mention the gap being recorded, got: %s", textOf(res))
	}

	// Second call: caught up (noMoreMessages), but the gap from the FIRST
	// call must still be mentioned — it wasn't walked, so it's not gone.
	res, err = hub.handleCatchUp(ctx, mcp.CallToolRequest{})
	if err != nil || res.IsError {
		t.Fatalf("hub_catch_up (second) failed: err=%v result=%+v", err, res)
	}
	text := textOf(res)
	if !strings.Contains(text, "caught up") {
		t.Fatalf("expected a caught-up result, got: %s", text)
	}
	if !strings.Contains(text, "2026-09-01T09:12:00Z") {
		t.Fatalf("expected the persisted gap to still be mentioned, got: %s", text)
	}
}

// TestBehindNoteSurfacesRecordedGapAtConnect proves the gap is also
// surfaced at connect time, not only from hub_catch_up itself — a fresh
// *Hub (a new process) connecting to a link whose key already has a
// persisted gap on record should mention it in the connect result.
func TestBehindNoteSurfacesRecordedGapAtConnect(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc#secret-1"
	ctx := context.Background()

	key := catchUpKeyForRelay(ctx, link)
	setCatchUpGap(key, "2026-09-01T09:12:00Z", "2026-09-04T15:00:00Z")

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	res, err := hub.handleTeamsRelayConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	text := textOf(res)
	if !strings.Contains(text, "still on record") || !strings.Contains(text, "2026-09-01T09:12:00Z") {
		t.Fatalf("expected the connect result to mention the recorded gap, got: %s", text)
	}
}
