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
	if !strings.Contains(text, "hub_history()") {
		t.Fatalf("expected a mention of hub_history, got: %s", text)
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

		own := wire.NewBroadcastMsg("550e8400-e29b-41d4-a716-446655440000", "my own echo", "ts1", nil, "", "")
		own.Own = true
		own.ExternalID = "ext-1"
		conn.WriteJSON(own)

		time.Sleep(300 * time.Millisecond) // give handleWait time to observe it's not woken yet

		reply := wire.NewBroadcastMsg("550e8400-e29b-41d4-a716-446655440001", "their reply", "ts2", nil, "", "")
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
	cursor := "cursor-9"
	topic := "Support chat"
	joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
	joined.LatestCursor = &cursor
	joined.HistoryLimitMax = 50
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
		"oneOnOne", `"Support chat"`, "cursor-9",
		"Sending is currently permitted", "capped at 50",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected result to mention %q, got: %s", want, text)
		}
	}
}

func TestTeamsRelayConnectNotesNoMessagesYetAndSendRefused(t *testing.T) {
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
	if !strings.Contains(text, "no messages yet") {
		t.Fatalf("expected a no-messages-yet note, got: %s", text)
	}
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

func TestHistoryToolSendsRequest(t *testing.T) {
	link, _ := startRelayTestServer(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	histReq := mcp.CallToolRequest{}
	histReq.Params.Arguments = map[string]any{"before": "cursor-1", "limit": 10}
	res, err := hub.handleHistory(ctx, histReq)
	if err != nil || res.IsError {
		t.Fatalf("hub_history failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "history request sent") {
		t.Fatalf("expected confirmation text, got: %s", textOf(res))
	}
}

func TestHistoryToolAfterSendsAfterRequestWhenSupported(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotHistory := make(chan wire.History, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		joined := wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000", ServerVersion: wire.ProtocolVersion, HistoryAfter: true}
		raw, _ := json.Marshal(joined)
		conn.WriteMessage(websocket.TextMessage, raw)
		var h wire.History
		if err := conn.ReadJSON(&h); err == nil {
			gotHistory <- h
		}
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

	histReq := mcp.CallToolRequest{}
	histReq.Params.Arguments = map[string]any{"after": "cursor-9", "limit": 5}
	res, err := hub.handleHistory(ctx, histReq)
	if err != nil || res.IsError {
		t.Fatalf("hub_history failed: err=%v result=%+v", err, res)
	}

	select {
	case h := <-gotHistory:
		if h.After != "cursor-9" || h.Before != "" || h.Limit != 5 {
			t.Fatalf("unexpected history request: %+v", h)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the history request")
	}
}

func TestHistoryToolAfterErrorsWhenNotSupported(t *testing.T) {
	link, _ := startRelayTestServer(t) // default Joined, HistoryAfter unset
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	histReq := mcp.CallToolRequest{}
	histReq.Params.Arguments = map[string]any{"after": "cursor-9"}
	res, err := hub.handleHistory(ctx, histReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError || !strings.Contains(textOf(res), "does not support after") {
		t.Fatalf("expected an error explaining after isn't supported, got: %+v", res)
	}
}

func TestHistoryToolRejectsBothBeforeAndAfter(t *testing.T) {
	link, _ := startRelayTestServer(t)
	ctx := context.Background()

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"link": link, "reconnectSecret": "resume-me"}
	if res, err := hub.handleTeamsRelayConnect(ctx, connReq); err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	defer hub.handleDisconnect(ctx, mcp.CallToolRequest{})

	histReq := mcp.CallToolRequest{}
	histReq.Params.Arguments = map[string]any{"before": "cursor-1", "after": "cursor-2"}
	res, err := hub.handleHistory(ctx, histReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError || !strings.Contains(textOf(res), "at most one") {
		t.Fatalf("expected an error rejecting both before and after, got: %+v", res)
	}
}

func TestHistoryToolErrorsWhenNotConnected(t *testing.T) {
	hub := NewHub()
	res, err := hub.handleHistory(context.Background(), mcp.CallToolRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError || !strings.Contains(textOf(res), "not connected") {
		t.Fatalf("expected a not-connected error, got: %+v", res)
	}
}
