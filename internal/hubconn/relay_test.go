package hubconn

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

// startRelayTestServer starts a bare websocket server that records the
// handshake headers it received, upgrades, sends a "joined" message, then
// hands the raw *websocket.Conn to onConnected (if given) for the test to
// drive further (e.g. sending a close frame with a specific code).
func startRelayTestServer(t *testing.T, onConnected func(*websocket.Conn)) (wsURL string, gotHeaders *http.Header) {
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
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
		// Declares that it answers each action with its own ack, and that
		// it replies to a standalone ack too — what makes waiting for
		// either worthwhile.
		joined.Features = map[string]json.RawMessage{
			"actionAcks": json.RawMessage("{}"),
			"ackReplies": json.RawMessage("{}"),
			"reactions":  json.RawMessage("{}"),
			"edit":       json.RawMessage("{}"),
			"delete":     json.RawMessage("{}"),
		}
		raw, _ := json.Marshal(joined)
		if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
			return
		}
		if onConnected != nil {
			onConnected(conn)
			return
		}
		// keep the connection open until the test's own cleanup tears down
		// the server, unless onConnected already decided to close it
		time.Sleep(2 * time.Second)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/relay/join?c=abc", &captured
}

func TestSendAwaitingAckOnPlainConnDoesNotWait(t *testing.T) {
	url := startTestServer(t)
	c, err := dialTest(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if c.WantsActionAcks() {
		t.Fatal("expected a server declaring no actionAcks feature to report WantsActionAcks() false")
	}
	start := time.Now()
	ev, ok, err := c.SendAwaitingAck("hello", "", nil, "", "", nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("SendAwaitingAck: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false on a plain connection (nothing to wait for), got event: %+v", ev)
	}
	if elapsed > time.Second {
		t.Fatalf("expected no ack-wait latency on a plain connection, took %v", elapsed)
	}
}

func TestSendAwaitingAckReturnsSendAckOnTeamsSession(t *testing.T) {
	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var m wire.Msg
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		conn.WriteJSON(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "ext-1", OK: true})
		time.Sleep(2 * time.Second)
	})
	c, err := Dial(link+"#secret", DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.SendAwaitingAck("hello", "", nil, "", "", nil)
	if err != nil {
		t.Fatalf("SendAwaitingAck: %v", err)
	}
	if !ok || ev.Kind != "sendAck" || ev.ExternalID != "ext-1" || !ev.ActionOK {
		t.Fatalf("expected the sendAck delivered directly, got ok=%v event=%+v", ok, ev)
	}
}

func TestSendAwaitingAckReturnsErrorEventOnRefusal(t *testing.T) {
	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var m wire.Msg
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		conn.WriteJSON(wire.Error{Type: wire.TypeError, Message: "refused", Code: "send_refused", Retryable: false})
		time.Sleep(2 * time.Second)
	})
	c, err := Dial(link+"#secret", DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.SendAwaitingAck("hello", "", nil, "", "", nil)
	if err != nil {
		t.Fatalf("SendAwaitingAck: %v", err)
	}
	if !ok || ev.Kind != "error" || ev.Code != "send_refused" {
		t.Fatalf("expected the refusal error delivered directly, got ok=%v event=%+v", ok, ev)
	}
}

func TestSendAwaitingAckTimesOutWithoutStealingLaterEvents(t *testing.T) {
	orig := AckWaitTimeout
	AckWaitTimeout = 100 * time.Millisecond
	defer func() { AckWaitTimeout = orig }()

	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var m wire.Msg
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		time.Sleep(300 * time.Millisecond) // arrives after AckWaitTimeout
		conn.WriteJSON(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "ext-late", OK: true})
		time.Sleep(2 * time.Second)
	})
	c, err := Dial(link+"#secret", DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	_, ok, err := c.SendAwaitingAck("hello", "", nil, "", "", nil)
	if err != nil {
		t.Fatalf("SendAwaitingAck: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false: the ack arrives after AckWaitTimeout")
	}

	// The late ack must still land in the general buffer once the claim
	// has been cancelled — never silently lost.
	deadline := time.Now().Add(2 * time.Second)
	var formatted string
	for time.Now().Before(deadline) {
		if hasEvents, _ := c.Peek(); hasEvents {
			formatted, _ = c.Drain()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(formatted, "ext-late") {
		t.Fatalf("expected the late sendAck to still arrive via the normal buffer, got: %q", formatted)
	}
}

func TestClaimNextAckDoesNotStealUnrelatedEvents(t *testing.T) {
	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var m wire.Msg
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		// An unrelated event arrives while the claim is pending — must
		// not be diverted, must still reach Peek/Drain normally.
		conn.WriteJSON(wire.NewRoster([]wire.RosterMember{
			{PeerID: "550e8400-e29b-41d4-a716-446655440099", Name: "Alice"}}))
		time.Sleep(50 * time.Millisecond)
		conn.WriteJSON(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "ext-1", OK: true})
		time.Sleep(2 * time.Second)
	})
	c, err := Dial(link+"#secret", DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.SendAwaitingAck("hello", "", nil, "", "", nil)
	if err != nil {
		t.Fatalf("SendAwaitingAck: %v", err)
	}
	if !ok || ev.Kind != "sendAck" {
		t.Fatalf("expected the sendAck claimed directly, got ok=%v event=%+v", ok, ev)
	}

	deadline := time.Now().Add(2 * time.Second)
	var formatted string
	for time.Now().Before(deadline) {
		if hasEvents, _ := c.Peek(); hasEvents {
			formatted, _ = c.Drain()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Alice arrives as part of the initial roster now — folded into one
	// line rather than delivered as her own event (see readLoop). What
	// this test is about is unchanged: the claimed ack did not swallow
	// her, and she still reaches the general buffer.
	if !strings.Contains(formatted, "Alice") {
		t.Fatalf("expected the unrelated roster to still reach the general buffer, got: %q", formatted)
	}
	if strings.Contains(formatted, "ext-1") {
		t.Fatalf("expected the claimed sendAck NOT to also appear in the general buffer, got: %q", formatted)
	}
}

func TestDeleteMessageAwaitingAckReturnsDeleteAckOnTeamsSession(t *testing.T) {
	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var d wire.Delete
		if err := conn.ReadJSON(&d); err != nil {
			return
		}
		conn.WriteJSON(wire.DeleteAck{Type: wire.TypeDeleteAck, ExternalID: d.ExternalID, OK: true})
		time.Sleep(2 * time.Second)
	})
	c, err := Dial(link+"#secret", DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.DeleteMessageAwaitingAck("ext-1")
	if err != nil {
		t.Fatalf("DeleteMessageAwaitingAck: %v", err)
	}
	if !ok || ev.Kind != "deleteAck" || ev.ExternalID != "ext-1" || !ev.ActionOK {
		t.Fatalf("expected the deleteAck delivered directly, got ok=%v event=%+v", ok, ev)
	}
}

func TestDeleteMessageOnPlainConnDoesNotWait(t *testing.T) {
	url := startTestServer(t)
	c, err := dialTest(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	start := time.Now()
	ev, ok, err := c.DeleteMessageAwaitingAck("ext-1")
	elapsed := time.Since(start)
	// mcp-hub-server ignores an unrecognized message type, so the write
	// itself succeeds; there's simply nothing to wait for.
	if err != nil {
		t.Fatalf("DeleteMessageAwaitingAck: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false on a plain connection, got event: %+v", ev)
	}
	if elapsed > time.Second {
		t.Fatalf("expected no ack-wait latency on a plain connection, took %v", elapsed)
	}
}

func TestDialSplitsFragmentAndSendsHeaders(t *testing.T) {
	base, headers := startRelayTestServer(t, nil)
	link := base + "#the-link-secret"

	c, err := Dial(link, DialOptions{ReconnectSecret: "resume-me", Name: "tester"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if got := headers.Get("Authorization"); got != "Bearer the-link-secret" {
		t.Fatalf("expected Authorization header with the link secret, got %q", got)
	}
	if got := headers.Get("Agent-Secret"); got != "resume-me" {
		t.Fatalf("expected Agent-Secret header, got %q", got)
	}
	if got := headers.Get("Agent-Name"); got != "tester" {
		t.Fatalf("expected Agent-Name header, got %q", got)
	}
}

func TestDialOmitsAgentNameHeaderWhenNameEmpty(t *testing.T) {
	base, headers := startRelayTestServer(t, nil)
	link := base + "#the-link-secret"

	c, err := Dial(link, DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if got := headers.Get("Agent-Name"); got != "" {
		t.Fatalf("expected no Agent-Name header when Name is empty, got %q", got)
	}
}

func TestDialSendsAgentIDHeaderWhenKnown(t *testing.T) {
	base, headers := startRelayTestServer(t, nil)
	link := base + "#the-link-secret"

	c, err := Dial(link, DialOptions{
		ReconnectSecret: "resume-me",
		AgentID:         "550e8400-e29b-41d4-a716-446655440042",
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if got := headers.Get("Agent-Id"); got != "550e8400-e29b-41d4-a716-446655440042" {
		t.Fatalf("expected the previously assigned peerId in Agent-Id, got %q", got)
	}
}

// A first-ever connect has no identity to ask for, so the header must be
// absent rather than empty — a server rejects an Agent-Id it cannot
// verify, which an empty value would trip.
func TestDialOmitsAgentIDHeaderWhenUnknown(t *testing.T) {
	base, headers := startRelayTestServer(t, nil)
	link := base + "#the-link-secret"

	c, err := Dial(link, DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if _, present := (*headers)["Agent-Id"]; present {
		t.Fatalf("expected no Agent-Id header at all on a first connect, got %q", headers.Get("Agent-Id"))
	}
}

func TestDialRejectsLinkWithoutFragment(t *testing.T) {
	if _, err := Dial("wss://example.com/relay/join?c=abc", DialOptions{ReconnectSecret: "x"}); err == nil {
		t.Fatal("expected an error for a link with no #-delimited secret")
	}
}

func TestDialRejectsEmptyReconnectSecret(t *testing.T) {
	if _, err := Dial("wss://example.com/relay/join?c=abc#secret", DialOptions{}); err == nil {
		t.Fatal("expected an error when reconnectSecret is empty")
	}
}

func TestDisconnectNoteReflectsKnownRelayCloseCode(t *testing.T) {
	base, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(4001, "revoked"))
	})
	link := base + "#the-link-secret"

	c, err := Dial(link, DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !c.Connected() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if c.Connected() {
		t.Fatal("expected the connection to be detected as closed")
	}
	if got := c.DisconnectNote(); got != " (revoked — do not reconnect)" {
		t.Fatalf("got %q", got)
	}
}

func TestDisconnectNoteEmptyForOrdinaryClose(t *testing.T) {
	base, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	})
	link := base + "#the-link-secret"

	c, err := Dial(link, DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !c.Connected() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := c.DisconnectNote(); got != "" {
		t.Fatalf("expected no note for an ordinary close, got %q", got)
	}
}

func TestRequestMessageAfterAwaitingReturnsMsgWithAnswers(t *testing.T) {
	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var m wire.MessageAfter
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		if m.Type != wire.TypeMessageAfter || m.Cursor != "cursor-1" {
			return
		}
		conn.WriteJSON(wire.Msg{
			Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "hi", TS: "ts1",
			Historical: true, Cursor: "cursor-2", ExternalID: "ext-1",
			Answers: &wire.Anchor{Cursor: "cursor-1"},
		})
		time.Sleep(2 * time.Second)
	})
	c, err := Dial(link+"#secret", DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.RequestMessageAfterAwaiting(wire.Anchor{Cursor: "cursor-1"})
	if err != nil {
		t.Fatalf("RequestMessageAfterAwaiting: %v", err)
	}
	if !ok || ev.Kind != "msg" || ev.Text != "hi" || ev.Answers == nil || ev.Answers.Cursor != "cursor-1" {
		t.Fatalf("expected the answering msg delivered directly, got ok=%v event=%+v", ok, ev)
	}
}

func TestRequestMessageAfterAwaitingReturnsNoMoreMessages(t *testing.T) {
	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var m wire.MessageAfter
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		conn.WriteJSON(wire.NewNoMoreMessages(wire.Anchor{Cursor: "cursor-1"}))
		time.Sleep(2 * time.Second)
	})
	c, err := Dial(link+"#secret", DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.RequestMessageAfterAwaiting(wire.Anchor{Cursor: "cursor-1"})
	if err != nil {
		t.Fatalf("RequestMessageAfterAwaiting: %v", err)
	}
	if !ok || ev.Kind != "noMoreMessages" || ev.Answers == nil || ev.Answers.Cursor != "cursor-1" {
		t.Fatalf("expected noMoreMessages delivered directly, got ok=%v event=%+v", ok, ev)
	}
}

func TestRequestMessageAfterAwaitingReturnsErrorEvent(t *testing.T) {
	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var m wire.MessageAfter
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		conn.WriteJSON(wire.Error{Type: wire.TypeError, Message: "bad anchor", Code: "bad_anchor", Retryable: false})
		time.Sleep(2 * time.Second)
	})
	c, err := Dial(link+"#secret", DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.RequestMessageAfterAwaiting(wire.Anchor{Cursor: "garbage"})
	if err != nil {
		t.Fatalf("RequestMessageAfterAwaiting: %v", err)
	}
	if !ok || ev.Kind != "error" || ev.Code != "bad_anchor" {
		t.Fatalf("expected bad_anchor error delivered directly, got ok=%v event=%+v", ok, ev)
	}
}

func TestOrdinaryLiveMsgIsNotDivertedToMessageAfterClaim(t *testing.T) {
	link, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		var m wire.MessageAfter
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		// An ordinary live msg with no Answers arrives first — must NOT
		// be diverted to the pending MessageAfter claim, which should
		// keep waiting until the real answer (with Answers set) shows up.
		conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "unrelated live", TS: "ts0"})
		time.Sleep(50 * time.Millisecond)
		conn.WriteJSON(wire.Msg{
			Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "the answer", TS: "ts1",
			Historical: true, Answers: &wire.Anchor{Cursor: "cursor-1"},
		})
		time.Sleep(2 * time.Second)
	})
	c, err := Dial(link+"#secret", DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.RequestMessageAfterAwaiting(wire.Anchor{Cursor: "cursor-1"})
	if err != nil {
		t.Fatalf("RequestMessageAfterAwaiting: %v", err)
	}
	if !ok || ev.Text != "the answer" {
		t.Fatalf("expected the answer with Answers set, got ok=%v event=%+v", ok, ev)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		events, _ := c.DrainEvents()
		for _, e := range events {
			if e.Text == "unrelated live" {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("expected the unrelated live msg to reach the general buffer, never saw it")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A server declaring actionAcks but not reactions/edit/delete used to get
// the request anyway, followed by a full AckWaitTimeout spent waiting for
// an answer it had already said it would not give. Two fields, two
// questions: reactions/edit/delete decide whether to send, actionAcks
// decides whether to wait.
func TestWriteActionsAreRefusedWhenTheServerDeclaresThemUnsupported(t *testing.T) {
	base, _ := startRelayTestServer(t, nil)
	c, err := Dial(base+"#the-link-secret", DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	// Declaring actionAcks says an answer is worth waiting for; it says
	// nothing about which actions exist. Strip those three and each one
	// must be refused rather than sent.
	c.mu.Lock()
	for _, f := range []string{"reactions", "edit", "delete"} {
		delete(c.features, f)
	}
	c.mu.Unlock()
	for _, tc := range []struct {
		action string
		call   func() error
	}{
		{"reactions", func() error { return c.React("ext-1", "👍", "add") }},
		{"edit", func() error { return c.EditMessage("ext-1", "new", nil, "", "", nil) }},
		{"delete", func() error { return c.DeleteMessage("ext-1") }},
	} {
		start := time.Now()
		err := tc.call()
		if err == nil {
			t.Fatalf("%s: expected a refusal, got none", tc.action)
		}
		if !strings.Contains(err.Error(), tc.action) {
			t.Fatalf("%s: expected the error to name the missing feature, got: %v", tc.action, err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("%s: expected an immediate refusal, took %v", tc.action, elapsed)
		}
	}
}

// The same actions must still go out against a server that declares them.
func TestWriteActionsAreSentWhenTheServerDeclaresThem(t *testing.T) {
	got := make(chan string, 3)
	base, _ := startRelayTestServer(t, func(conn *websocket.Conn) {
		for {
			var m map[string]any
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			got <- fmt.Sprint(m["type"])
		}
	})
	c, err := Dial(base+"#the-link-secret", DialOptions{ReconnectSecret: "resume-me"})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	if err := c.React("ext-1", "👍", "add"); err != nil {
		t.Fatalf("React: %v", err)
	}
	if err := c.EditMessage("ext-1", "new", nil, "", "", nil); err != nil {
		t.Fatalf("EditMessage: %v", err)
	}
	if err := c.DeleteMessage("ext-1"); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
	for _, want := range []string{"reaction", "edit", "delete"} {
		select {
		case kind := <-got:
			if kind != want {
				t.Fatalf("expected a %q request to reach the server, got %q", want, kind)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("expected a %q request to reach the server, nothing arrived", want)
		}
	}
}

// A history answer belongs to the request whose anchor it names, and to
// no other. Routing on "has an Answers field" alone means a late reply to
// a request that already timed out satisfies the NEXT one — and a
// catch-up caller then persists a cursor from a walk it never made,
// moving its reading position over messages nobody saw.
func TestAHistoryAnswerForAnotherAnchorDoesNotSatisfyThisRequest(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", ""))
		var m wire.MessageAfter
		if err := conn.ReadJSON(&m); err != nil {
			return
		}
		// The stale answer to a request this client already gave up on...
		conn.WriteJSON(wire.Msg{
			Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			Text: "answer to the OLD request", TS: "ts-old", Historical: true,
			Cursor: "cursor-old", Answers: &wire.Anchor{Cursor: "an-abandoned-anchor"},
		})
		time.Sleep(150 * time.Millisecond)
		// ...then the answer actually asked for.
		conn.WriteJSON(wire.Msg{
			Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			Text: "answer to THIS request", TS: "ts-new", Historical: true,
			Cursor: "cursor-new", Answers: &wire.Anchor{Cursor: "the-anchor-asked-for"},
		})
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := dialTest(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.RequestMessageAfterAwaiting(wire.Anchor{Cursor: "the-anchor-asked-for"})
	if err != nil {
		t.Fatalf("RequestMessageAfterAwaiting: %v", err)
	}
	if !ok {
		t.Fatal("expected the answer naming this request's own anchor to arrive")
	}
	if ev.Cursor != "cursor-new" {
		t.Fatalf("got another request's answer handed back: %+v", ev)
	}
}

// And a second history request, while one is still outstanding, is
// refused rather than silently displacing it: the displaced caller was
// never told and simply waited out its timeout, while the survivor took
// whatever answer arrived first.
func TestASecondHistoryRequestIsRefusedNotSilentlySwapped(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", ""))
		time.Sleep(3 * time.Second) // never answers
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := dialTest(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// The first request is JOINED before this test returns. Left running,
	// it reads AckWaitTimeout concurrently with whichever later test sets
	// it, which the race detector reports against that test.
	started := make(chan struct{})
	firstDone := make(chan struct{})
	defer func() {
		c.Close()
		<-firstDone
	}()
	go func() {
		defer close(firstDone)
		close(started)
		c.RequestMessageAfterAwaiting(wire.Anchor{Cursor: "first"})
	}()
	<-started
	time.Sleep(100 * time.Millisecond)

	_, _, err = c.RequestMessageAfterAwaiting(wire.Anchor{Cursor: "second"})
	if err == nil {
		t.Fatal("expected the second history request to be refused while the first is outstanding")
	}
	if !strings.Contains(err.Error(), "already") {
		t.Fatalf("expected the refusal to say one is already outstanding, got: %v", err)
	}
}

// An upgrade is not an answer. A server that accepts the socket and then
// says nothing left the handshake read blocked with no deadline yet set,
// so hub_connect hung indefinitely — the one failure a caller can neither
// report nor retry.
func TestAServerThatUpgradesAndSaysNothingDoesNotHangTheConnect(t *testing.T) {
	old := pongWait
	pongWait = 300 * time.Millisecond
	defer func() { pongWait = old }()

	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		time.Sleep(3 * time.Second) // never sends joined
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	done := make(chan error, 1)
	go func() {
		c, err := dialTest(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
		if c != nil {
			c.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the connect to fail rather than succeed against a silent server")
		}
		if !strings.Contains(err.Error(), "joined") {
			t.Fatalf("expected the failure to name what was missing, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connect hung against a server that upgraded and then said nothing")
	}
}

// The activity callback used to run ON the read loop's own goroutine, so
// anything it did that needed a REPLY could never get one: the reply
// could only be read by the loop that was waiting for the callback to
// return. Fetching a reference attachment during delivery is exactly
// that, and it did not merely fail — it held the socket unread for the
// whole deadline, ping handling included.
func TestAnActivityCallbackCanAwaitAReplyFromItsOwnConnection(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", ""))
		conn.WriteJSON(wire.Msg{
			Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
			Text: "look", TS: "ts1", Cursor: "cursor-1",
		})
		for {
			var req wire.AttachmentRequest
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			if req.Type != wire.TypeAttachment {
				continue
			}
			conn.WriteJSON(wire.AttachmentData{
				Type: wire.TypeAttachmentData, Token: req.Token, Name: "note.md",
				ContentType: "text/markdown", ContentBytes: "dGhlIGJ5dGVz",
			})
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := dialTest(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	fetched := make(chan string, 4)
	c.OnActivity(func() {
		if hasEvents, _ := c.Peek(); !hasEvents {
			return
		}
		c.Drain()
		ev, ok, err := c.RequestAttachment("att-1", nil)
		if err != nil || !ok {
			fetched <- ""
			return
		}
		fetched <- ev.AttachmentContentBytes
	})

	select {
	case got := <-fetched:
		if got != "dGhlIGJ5dGVz" {
			t.Fatalf("the callback could not fetch from its own connection, got %q", got)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("the callback never completed — it is waiting for a reply its own read loop must deliver")
	}
}
