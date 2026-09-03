package hubconn

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
	"github.com/secforge/mcp-hub/internal/wsserver"
)

func startTestServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(wsserver.NewHandler())
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func waitForActivity(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for activity")
	}
}

func TestDecodeEventExportedWrapperMatchesInternalDecode(t *testing.T) {
	raw, err := json.Marshal(wire.NewPeerJoined("550e8400-e29b-41d4-a716-446655440000", "Alice", ""))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ev, ok := DecodeEvent(raw)
	if !ok {
		t.Fatal("expected DecodeEvent to succeed")
	}
	if ev.Kind != "peerJoined" || ev.PeerID != "550e8400-e29b-41d4-a716-446655440000" || ev.Name != "Alice" {
		t.Fatalf("unexpected decoded event: %+v", ev)
	}
}

func TestDecodeEventCarriesReplyToAndReplyPreview(t *testing.T) {
	m := wire.NewBroadcastMsg("550e8400-e29b-41d4-a716-446655440000", "reply text", "ts", nil, "", "")
	m.ReplyTo = "ext-orig"
	m.ReplyPreview = "GT-158 pending item 2/3"
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ev, ok := DecodeEvent(raw)
	if !ok {
		t.Fatal("expected DecodeEvent to succeed")
	}
	if ev.ReplyTo != "ext-orig" || ev.ReplyPreview != "GT-158 pending item 2/3" {
		t.Fatalf("unexpected decoded event: %+v", ev)
	}
}

func TestDecodeEventCarriesHistoryCompleteCounts(t *testing.T) {
	hc := wire.HistoryComplete{Type: wire.TypeHistoryComplete, Count: 5, Oldest: "c1", Newest: "c5"}
	raw, err := json.Marshal(hc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ev, ok := DecodeEvent(raw)
	if !ok {
		t.Fatal("expected DecodeEvent to succeed")
	}
	if ev.Kind != "historyComplete" || ev.HistoryCount != 5 || ev.HistoryOldest != "c1" || ev.HistoryNewest != "c5" {
		t.Fatalf("unexpected decoded event: %+v", ev)
	}
}

func TestDecodeEventCarriesHistoryBegin(t *testing.T) {
	raw, err := json.Marshal(wire.NewHistoryBegin(5, "c1", "c5"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ev, ok := DecodeEvent(raw)
	if !ok {
		t.Fatal("expected DecodeEvent to succeed")
	}
	if ev.Kind != "historyBegin" || ev.HistoryCount != 5 || ev.HistoryOldest != "c1" || ev.HistoryNewest != "c5" {
		t.Fatalf("unexpected decoded event: %+v", ev)
	}
}

func TestDecodeEventCarriesMentions(t *testing.T) {
	m := wire.NewBroadcastMsg("550e8400-e29b-41d4-a716-446655440000", "hi @alice", "ts", nil, "", "")
	m.Mentions = []wire.Mention{{Name: "Alice", ID: "dir-1"}}
	m.MentionedMe = true
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ev, ok := DecodeEvent(raw)
	if !ok {
		t.Fatal("expected DecodeEvent to succeed")
	}
	if len(ev.Mentions) != 1 || ev.Mentions[0].ID != "dir-1" || !ev.MentionedMe {
		t.Fatalf("unexpected decoded event: %+v", ev)
	}
}

func TestDecodeEventMessageEditedCarriesReplyTo(t *testing.T) {
	m := wire.MessageEdited{
		Type: wire.TypeMessageEdited, ExternalID: "ext-1", Text: "corrected",
		ReplyTo: "ext-orig", ReplyPreview: "preview text",
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	ev, ok := DecodeEvent(raw)
	if !ok {
		t.Fatal("expected DecodeEvent to succeed")
	}
	if ev.ReplyTo != "ext-orig" || ev.ReplyPreview != "preview text" {
		t.Fatalf("unexpected decoded event: %+v", ev)
	}
}

func TestDialJoinsAndAssignsPeerID(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if c.PeerID() == "" {
		t.Fatal("expected a non-empty peerID")
	}
}

func TestDialReachesServerMountedUnderABasePath(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/hub/", http.StripPrefix("/hub", wsserver.NewHandler()))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	host := "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub"

	c, err := Dial(host, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if c.PeerID() == "" {
		t.Fatal("expected a non-empty peerID")
	}
}

func TestLastSeenCursorTracksMostRecentDeliveredMsg(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "first", TS: "ts1", Cursor: "cursor-1"})
		conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "second", TS: "ts2", Cursor: "cursor-2"})
		select {}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if c.LastSeenCursor() == "cursor-2" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected LastSeenCursor to become %q, got %q", "cursor-2", c.LastSeenCursor())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAckCursorPiggybacksOnSendAfterConsuming(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotMsg := make(chan wire.Msg, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "hi", TS: "ts1", Cursor: "cursor-1"})
		var m wire.Msg
		for {
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			if m.Type == wire.TypeMsg {
				gotMsg <- m
				return
			}
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for c.LastSeenCursor() != "cursor-1" {
		if time.Now().After(deadline) {
			t.Fatal("never saw cursor-1 buffered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, connected := c.Drain(); !connected {
		t.Fatal("expected still connected")
	}
	if c.LastConsumedCursor() != "cursor-1" {
		t.Fatalf("expected LastConsumedCursor cursor-1, got %q", c.LastConsumedCursor())
	}

	if err := c.Send("hello", nil, "", ""); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case m := <-gotMsg:
		if m.AckCursor != "cursor-1" {
			t.Fatalf("expected piggybacked ackCursor %q, got %q", "cursor-1", m.AckCursor)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the send")
	}
}

func TestAckCursorOmittedBeforeAnythingConsumed(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotMsg := make(chan wire.Msg, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		var m wire.Msg
		for {
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			if m.Type == wire.TypeMsg {
				gotMsg <- m
				return
			}
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.Send("hello", nil, "", ""); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case m := <-gotMsg:
		if m.AckCursor != "" {
			t.Fatalf("expected no ackCursor before anything was consumed, got %q", m.AckCursor)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the send")
	}
}

func TestAckLoopSendsStandaloneAckWhenIdleAndConsumedMoved(t *testing.T) {
	origAckIdleInterval := ackIdleInterval
	ackIdleInterval = 50 * time.Millisecond
	defer func() { ackIdleInterval = origAckIdleInterval }()

	upgrader := websocket.Upgrader{}
	gotAck := make(chan wire.Ack, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "hi", TS: "ts1", Cursor: "cursor-1"})
		for {
			var raw json.RawMessage
			if err := conn.ReadJSON(&raw); err != nil {
				return
			}
			typ, err := wire.DecodeType(raw)
			if err != nil || typ != wire.TypeAck {
				continue
			}
			var a wire.Ack
			json.Unmarshal(raw, &a)
			gotAck <- a
			return
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for c.LastSeenCursor() != "cursor-1" {
		if time.Now().After(deadline) {
			t.Fatal("never saw cursor-1 buffered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.Drain()

	select {
	case a := <-gotAck:
		if a.AckCursor != "cursor-1" {
			t.Fatalf("expected standalone ack for cursor-1, got %+v", a)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("never received a standalone ack after the idle interval")
	}
}

func TestAckReplyRejectionAdoptsServerReportedCursor(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "hi", TS: "ts1", Cursor: "cursor-1"})
		var m wire.Msg
		for {
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			if m.Type == wire.TypeMsg {
				conn.WriteJSON(wire.Ack{Type: wire.TypeAck, AckCursor: "cursor-server-actual", OK: false})
				return
			}
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for c.LastSeenCursor() != "cursor-1" {
		if time.Now().After(deadline) {
			t.Fatal("never saw cursor-1 buffered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.Drain()
	if err := c.Send("hello", nil, "", ""); err != nil {
		t.Fatalf("Send: %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for c.LastAckSentCursor() != "cursor-server-actual" {
		if time.Now().After(deadline) {
			t.Fatalf("expected LastAckSentCursor to adopt the server's reported cursor, got %q", c.LastAckSentCursor())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBadAckCursorErrorDisablesFurtherAcks(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8", Text: "hi", TS: "ts1", Cursor: "cursor-1"})
		var m wire.Msg
		for {
			if err := conn.ReadJSON(&m); err != nil {
				return
			}
			if m.Type == wire.TypeMsg {
				conn.WriteJSON(wire.Error{Type: wire.TypeError, Message: "bad ack cursor", Code: "bad_ack_cursor", Retryable: false})
			}
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for c.LastSeenCursor() != "cursor-1" {
		if time.Now().After(deadline) {
			t.Fatal("never saw cursor-1 buffered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.Drain()
	if err := c.Send("first", nil, "", ""); err != nil {
		t.Fatalf("Send: %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for !c.AckDisabled() {
		if time.Now().After(deadline) {
			t.Fatal("expected AckDisabled to become true after a bad_ack_cursor error")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := c.ackCursorForOutbound(); got != "" {
		t.Fatalf("expected no further ack cursors once disabled, got %q", got)
	}

	// The bad_ack_cursor error must never reach the model-visible buffer —
	// it's ack-plumbing internal, not something the model asked for.
	formatted, _ := c.Drain()
	if strings.Contains(formatted, "bad_ack_cursor") || strings.Contains(formatted, "bad ack cursor") {
		t.Fatalf("expected the bad_ack_cursor error to be filtered from the model-visible buffer, got: %s", formatted)
	}
}

// TestUnrelatedBadCursorErrorDoesNotDisableAcks proves the fix for a real
// bug: bad_cursor/bad_request are emitted by many unrelated request kinds
// on a bridge server (e.g. a malformed reaction, a history request naming
// both before and after) — only bad_ack/bad_ack_cursor are specific to the
// ack subsystem. Reacting to the generic codes would have let one
// malformed reaction silently and permanently kill read receipts for the
// rest of the session.
func TestUnrelatedBadCursorErrorDoesNotDisableAcks(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		// Simulates an unrelated request (e.g. a malformed reaction, or a
		// history request naming both before and after) being refused with
		// the same generic codes an ack failure could also use.
		conn.WriteJSON(wire.Error{Type: wire.TypeError, Message: "bad cursor", Code: "bad_cursor", Retryable: false})
		conn.WriteJSON(wire.Error{Type: wire.TypeError, Message: "bad request", Code: "bad_request", Retryable: false})
		select {}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if formatted, _ := c.Peek(); formatted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("never saw the errors buffered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	formatted, _ := c.Drain()
	if !strings.Contains(formatted, "bad_cursor") || !strings.Contains(formatted, "bad_request") {
		t.Fatalf("expected unrelated bad_cursor/bad_request errors to reach the model buffer normally, got: %s", formatted)
	}
	if c.AckDisabled() {
		t.Fatal("expected AckDisabled to remain false for errors unrelated to the ack subsystem")
	}
}

func TestCloseSendsNormalClosureCloseFrame(t *testing.T) {
	upgrader := websocket.Upgrader{}
	type closeInfo struct {
		code int
		text string
	}
	gotClose := make(chan closeInfo, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				if ce, ok := err.(*websocket.CloseError); ok {
					gotClose <- closeInfo{code: ce.Code, text: ce.Text}
				} else {
					gotClose <- closeInfo{code: -1}
				}
				return
			}
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.Close()

	select {
	case got := <-gotClose:
		if got.code != websocket.CloseNormalClosure {
			t.Fatalf("expected the server to see a normal-closure close frame (code %d), got %d",
				websocket.CloseNormalClosure, got.code)
		}
		if got.text == "" {
			t.Fatal("expected a non-empty close reason so the server's log line is self-explanatory")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never observed the connection closing")
	}
}

func TestCloseWaitsForFlushGraceAfterSuccessfulWrite(t *testing.T) {
	origGrace := closeFlushGrace
	closeFlushGrace = 100 * time.Millisecond
	defer func() { closeFlushGrace = origGrace }()

	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	start := time.Now()
	c.Close()
	elapsed := time.Since(start)
	if elapsed < closeFlushGrace {
		t.Fatalf("expected Close to wait at least %s for the frame to flush before closing, took %s",
			closeFlushGrace, elapsed)
	}
}

func TestCloseReturnsWriteControlErrorWhenFrameCannotBeSent(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	// Kill the underlying connection out from under Close, so its own
	// WriteControl call fails — proving that error is surfaced (not
	// silently discarded) rather than masked by the subsequent Close call
	// on the already-dead socket.
	c.ws.Close()

	if err := c.Close(); err == nil {
		t.Fatal("expected Close to surface the write error from an already-dead connection, got nil")
	}
}

func TestDialSendsCreateTokenHeaderWhenGiven(t *testing.T) {
	var gotHeader string
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Hub-Create-Token")
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{CreateToken: "abc123.secretvalue"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if gotHeader != "abc123.secretvalue" {
		t.Fatalf("expected X-Hub-Create-Token header %q, got %q", "abc123.secretvalue", gotHeader)
	}
}

func TestDialOmitsCreateTokenHeaderWhenNotGiven(t *testing.T) {
	var sawHeader bool
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawHeader = r.Header.Get("X-Hub-Create-Token") != ""
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if sawHeader {
		t.Fatal("expected no X-Hub-Create-Token header when none was given")
	}
}

func TestDialRejectsInvalidSessionID(t *testing.T) {
	url := startTestServer(t)
	if _, err := Dial(url, "not-a-uuid", DialOptions{}); err == nil {
		t.Fatal("expected an error for an invalid sessionId")
	}
}

func TestDialSendsCurrentProtocolVersionAsQueryParam(t *testing.T) {
	var gotQuery string
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	want := "v=" + strconv.Itoa(wire.ProtocolVersion)
	if gotQuery != want {
		t.Fatalf("got query %q, want %q", gotQuery, want)
	}
}

func TestDialCapturesServerVersion(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if c.ServerVersion() != wire.ProtocolVersion {
		t.Fatalf("got ServerVersion %d, want %d", c.ServerVersion(), wire.ProtocolVersion)
	}
}

func TestSystemPeerIDCapturedFromJoined(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		j := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
		j.SystemPeerID = "00000000-0000-0000-0000-000000000000"
		conn.WriteJSON(j)
		for {
			var raw json.RawMessage
			if err := conn.ReadJSON(&raw); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if c.SystemPeerID() != "00000000-0000-0000-0000-000000000000" {
		t.Fatalf("got SystemPeerID %q, want the all-zeros uuid", c.SystemPeerID())
	}
}

func TestMsgFromSystemPeerIsMarkedOperator(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		j := wire.NewJoined("6ba7b810-9dad-11d1-80b4-00c04fd430c8", 0, "", "")
		j.SystemPeerID = "00000000-0000-0000-0000-000000000000"
		conn.WriteJSON(j)
		conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "00000000-0000-0000-0000-000000000000", Text: "go ahead", TS: "ts1"})
		conn.WriteJSON(wire.Msg{Type: wire.TypeMsg, PeerID: "550e8400-e29b-41d4-a716-446655440000", Text: "ordinary peer", TS: "ts2"})
		for {
			var raw json.RawMessage
			if err := conn.ReadJSON(&raw); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	var events []Event
	for len(events) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("only saw %d events before timeout", len(events))
		}
		ev, _ := c.DrainEvents()
		events = append(events, ev...)
		if len(events) < 2 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if !events[0].IsOperator {
		t.Fatalf("expected the system-peer msg to be marked IsOperator, got: %+v", events[0])
	}
	if events[1].IsOperator {
		t.Fatalf("expected the ordinary-peer msg to NOT be marked IsOperator, got: %+v", events[1])
	}
}

func TestExpectedPeerCountMatchesJoinedPeerCount(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	if a.ExpectedPeerCount() != 0 {
		t.Fatalf("a: expected 0, got %d", a.ExpectedPeerCount())
	}

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()
	if b.ExpectedPeerCount() != 1 {
		t.Fatalf("b: expected 1, got %d", b.ExpectedPeerCount())
	}
}

func TestRosterCompleteImmediatelyWhenNoExistingPeers(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	activity := make(chan struct{}, 8)
	c.OnActivity(func() { activity <- struct{}{} })

	waitForActivity(t, activity) // the server's rosterComplete for an empty roster
	if !c.RosterComplete() {
		t.Fatal("expected RosterComplete to be true once the server's rosterComplete event arrives")
	}

	formatted, connected := c.Drain()
	if !connected {
		t.Fatal("expected still connected")
	}
	if !strings.Contains(formatted, "roster complete") {
		t.Fatalf("expected a rosterComplete notification already buffered, got: %q", formatted)
	}
}

func TestRosterCompleteBecomesTrueOnceCaughtUp(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()

	activity := make(chan struct{}, 8)
	b.OnActivity(func() { activity <- struct{}{} })

	if b.RosterComplete() {
		t.Fatal("expected RosterComplete to be false before the roster catch-up event arrives")
	}
	waitForActivity(t, activity) // b is told about a (roster entry)
	waitForActivity(t, activity) // b's rosterComplete

	if !b.RosterComplete() {
		t.Fatal("expected RosterComplete to be true after catching up on the existing roster")
	}
	formatted, connected := b.Drain()
	if !connected {
		t.Fatal("expected still connected")
	}
	if !strings.Contains(formatted, "roster complete") {
		t.Fatalf("expected a rosterComplete notification in the drained events, got: %q", formatted)
	}
}

func TestDialRejectsHostWithPath(t *testing.T) {
	url := startTestServer(t)
	if _, err := Dial(url+"/extra-path", "550e8400-e29b-41d4-a716-446655440000", DialOptions{}); err == nil {
		t.Fatal("expected Dial to reject a host containing a path")
	}
}

func TestSendAndReceiveBetweenTwoConns(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()

	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })
	waitForActivity(t, activity) // a's own rosterComplete (no peers yet)

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()

	waitForActivity(t, activity) // a sees b's peerJoined

	if err := b.Send("hello", nil, "", ""); err != nil {
		t.Fatalf("send: %v", err)
	}
	waitForActivity(t, activity) // a sees the message

	formatted, connected := a.Drain()
	if !connected {
		t.Fatal("expected still connected")
	}
	if !strings.Contains(formatted, "hello") {
		t.Fatalf("expected drained events to include the message, got: %s", formatted)
	}
}

func TestSendWithAttachmentsDeliversThem(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()

	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })
	waitForActivity(t, activity) // a's own rosterComplete

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()
	waitForActivity(t, activity) // a sees b's peerJoined

	attachments := []wire.Attachment{{ContentType: "image/png", ContentBytes: "aGVsbG8="}}
	if err := b.Send("a picture", attachments, "", ""); err != nil {
		t.Fatalf("send: %v", err)
	}
	waitForActivity(t, activity) // a sees the message

	events, connected := a.DrainEvents()
	if !connected {
		t.Fatal("expected still connected")
	}
	var found bool
	for _, ev := range events {
		if ev.Kind != "msg" {
			continue
		}
		if len(ev.Attachments) != 1 || ev.Attachments[0].ContentType != "image/png" ||
			ev.Attachments[0].ContentBytes != "aGVsbG8=" {
			t.Fatalf("unexpected attachments on msg event: %+v", ev.Attachments)
		}
		found = true
	}
	if !found {
		t.Fatalf("expected a msg event among drained events, got: %+v", events)
	}
}

func TestSendToDeliversOnlyToTargetAndMarksPrivate(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	activityA := make(chan struct{}, 8)
	a.OnActivity(func() { activityA <- struct{}{} })
	waitForActivity(t, activityA) // a's own rosterComplete (no peers yet)

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()
	activityB := make(chan struct{}, 8)
	b.OnActivity(func() { activityB <- struct{}{} })

	waitForActivity(t, activityA) // a sees b's peerJoined
	a.Drain()

	if err := b.SendTo("just for you", a.PeerID(), nil, "", ""); err != nil {
		t.Fatalf("sendTo: %v", err)
	}
	waitForActivity(t, activityA)

	formatted, connected := a.Drain()
	if !connected {
		t.Fatal("expected still connected")
	}
	if !strings.Contains(formatted, "just for you") || !strings.Contains(formatted, "PRIVATE") {
		t.Fatalf("expected a private message wrapper, got: %s", formatted)
	}
}

func TestSendToUnknownPeerSurfacesAsErrorEvent(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer a.Close()
	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })
	waitForActivity(t, activity) // a's own rosterComplete (no peers yet)

	if err := a.SendTo("hello?", "00000000-0000-0000-0000-000000000000", nil, "", ""); err != nil {
		t.Fatalf("sendTo: %v", err)
	}
	waitForActivity(t, activity)

	formatted, connected := a.Drain()
	if !connected {
		t.Fatal("expected still connected")
	}
	if !strings.Contains(formatted, "[HUB ERROR]") {
		t.Fatalf("expected an error event, got: %s", formatted)
	}
}

func TestPeersTracksRosterAsPeersJoinAndLeave(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })
	waitForActivity(t, activity) // a's own rosterComplete (no peers yet)

	if peers := a.Peers(); len(peers) != 0 {
		t.Fatalf("expected no peers before anyone else joins, got %v", peers)
	}

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	waitForActivity(t, activity) // a sees b's peerJoined

	peers := a.Peers()
	if len(peers) != 1 || peers[0].ID != b.PeerID() {
		t.Fatalf("expected [%s], got %v", b.PeerID(), peers)
	}

	c, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial c: %v", err)
	}
	defer c.Close()
	waitForActivity(t, activity) // a sees c's peerJoined

	peers = a.Peers()
	if len(peers) != 2 {
		t.Fatalf("expected 2 peers, got %v", peers)
	}

	b.Close()
	waitForActivity(t, activity) // a sees b's peerLeft

	peers = a.Peers()
	if len(peers) != 1 || peers[0].ID != c.PeerID() {
		t.Fatalf("expected just [%s] after b left, got %v", c.PeerID(), peers)
	}
}

func TestPeekAndDrainReflectDisconnect(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	b, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	b.Close()
	// server closing the connection propagates as a peerLeft to a, then
	// (separately) a's own socket must be closed by the test to observe its
	// own disconnect:
	a.Close()

	// a's readLoop goroutine notices the close asynchronously (it's still
	// mid-flight on whatever roster/peerJoined/peerLeft traffic arrived
	// before the close), so poll briefly instead of checking Peek()
	// immediately.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, connected := a.Peek(); !connected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected Peek to eventually report disconnected after Close")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSilentDropIsDetectedViaReadDeadline proves the fix for a real gap: a
// server that goes silent without ever closing the TCP connection (no data,
// no ping, no FIN/RST — e.g. a killed process behind a still-open socket,
// or a network partition) used to be invisible to the client forever, since
// ws.ReadMessage blocked with no deadline. Peek/Drain would report
// connected indefinitely and anything sent in that window was silently
// lost. With a read deadline that only pongWait resets, a silent server
// must be noticed once that deadline elapses.
func TestSilentDropIsDetectedViaReadDeadline(t *testing.T) {
	origPongWait, origWriteWait := pongWait, writeWait
	pongWait = 60 * time.Millisecond
	writeWait = 30 * time.Millisecond
	defer func() { pongWait, writeWait = origPongWait, origWriteWait }()

	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		// go silent forever — no more frames, no ping, no close.
		select {}
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, connected := c.Peek(); !connected {
			note := c.DisconnectNote()
			if note == "" {
				t.Fatal("expected DisconnectNote to explain a pongWait timeout, got empty string")
			}
			if !strings.Contains(note, `last frame seen was "joined"`) {
				t.Fatalf("expected the note to name the last frame seen (the initial joined message, since the server went silent after it), got %q", note)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("expected the silent server to eventually be detected as disconnected")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

const testAgePublicKey = "age1scdm7mae5t68c9ch0sqfzlusqyflpgxlrgk3zwl44zwl9vvq2guqtdv4fk"

func TestDialRejectsMalformedAgePublicKey(t *testing.T) {
	url := startTestServer(t)
	_, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{AgePublicKey: "not-a-key"})
	if err == nil {
		t.Fatal("expected an error for a malformed agePublicKey")
	}
}

func TestDialEchoesBackSanitizedNameAndAgePublicKey(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000",
		DialOptions{Name: "Alice\n\x1b[31m", AgePublicKey: testAgePublicKey})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if c.Name() != "Alice[31m" {
		t.Fatalf("expected control chars/newlines stripped from name, got %q", c.Name())
	}
	if c.AgePublicKey() != testAgePublicKey {
		t.Fatalf("got AgePublicKey %q, want %q", c.AgePublicKey(), testAgePublicKey)
	}
}

func TestPeekSuppressesOwnReactionChangedAndMessageEdited(t *testing.T) {
	c := &Conn{}
	c.buffer = []Event{
		{Kind: "reactionChanged", ExternalID: "ext-1", Own: true},
		{Kind: "messageEdited", ExternalID: "ext-2", Own: true},
		{Kind: "messageDeleted", ExternalID: "ext-3", Own: true},
	}
	if hasEvents, _ := c.Peek(); hasEvents {
		t.Fatal("expected own reactionChanged/messageEdited to be suppressed like own msg")
	}
	c.buffer = append(c.buffer, Event{Kind: "peerJoined", PeerID: "peer-1"})
	if hasEvents, _ := c.Peek(); !hasEvents {
		t.Fatal("expected a non-own event to make the buffer wake-worthy")
	}
}

func TestPeekTreatsNonOwnReactionChangedAsWakeWorthy(t *testing.T) {
	c := &Conn{buffer: []Event{{Kind: "reactionChanged", ExternalID: "ext-1", PeerID: "peer-1"}}}
	if hasEvents, _ := c.Peek(); !hasEvents {
		t.Fatal("expected a reactionChanged from someone else to be wake-worthy")
	}
}

func TestPeekSuppressesOwnMessagesUntilANonOwnEventArrives(t *testing.T) {
	c := &Conn{}
	c.buffer = []Event{
		{Kind: "msg", PeerID: "peer-1", Text: "own 1", Own: true},
		{Kind: "msg", PeerID: "peer-1", Text: "own 2", Own: true},
	}
	if hasEvents, connected := c.Peek(); hasEvents || !connected {
		t.Fatalf("expected own-only buffer to not be wake-worthy, got hasEvents=%v connected=%v", hasEvents, connected)
	}

	c.buffer = append(c.buffer, Event{Kind: "msg", PeerID: "peer-2", Text: "their reply"})
	if hasEvents, connected := c.Peek(); !hasEvents || !connected {
		t.Fatalf("expected a non-own event to make the buffer wake-worthy, got hasEvents=%v connected=%v", hasEvents, connected)
	}

	formatted, connected := c.Drain()
	if !connected {
		t.Fatal("expected still connected")
	}
	for _, want := range []string{"own 1", "own 2", "their reply"} {
		if !strings.Contains(formatted, want) {
			t.Fatalf("expected Drain to return everything buffered together, missing %q in: %s", want, formatted)
		}
	}
}

func TestPeekWakesOnOwnMessageOverflowEvenWithNoReply(t *testing.T) {
	c := &Conn{}
	for i := 0; i < maxPendingOwnMessages-1; i++ {
		c.buffer = append(c.buffer, Event{Kind: "msg", PeerID: "peer-1", Own: true})
	}
	if hasEvents, _ := c.Peek(); hasEvents {
		t.Fatalf("expected %d own messages to still be suppressed (cap is %d)", maxPendingOwnMessages-1, maxPendingOwnMessages)
	}
	c.buffer = append(c.buffer, Event{Kind: "msg", PeerID: "peer-1", Own: true})
	if hasEvents, _ := c.Peek(); !hasEvents {
		t.Fatalf("expected reaching the %d-message cap to become wake-worthy with no reply", maxPendingOwnMessages)
	}
}

func TestPeekTreatsNonMsgEventsAsAlwaysWakeWorthy(t *testing.T) {
	for _, e := range []Event{
		{Kind: "sendAck", ExternalID: "ext-1", ActionOK: true},
		{Kind: "error", Text: "nope"},
		{Kind: "peerJoined", PeerID: "peer-1"},
		{Kind: "rosterComplete"},
	} {
		c := &Conn{buffer: []Event{e}}
		if hasEvents, _ := c.Peek(); !hasEvents {
			t.Fatalf("expected a bare %q event to be wake-worthy on its own, got hasEvents=false", e.Kind)
		}
	}
}

func TestPeekReportsDisconnectedEvenWithOnlySuppressedOwnMessages(t *testing.T) {
	c := &Conn{closed: true}
	c.buffer = []Event{{Kind: "msg", PeerID: "peer-1", Own: true}}
	if hasEvents, connected := c.Peek(); connected {
		t.Fatalf("expected disconnected to still report connected=false regardless of buffer contents, got hasEvents=%v connected=%v", hasEvents, connected)
	}
}

func TestBufferCarriesHistoricalFlagAndErrorCodeThroughToDrain(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		historical := wire.NewBroadcastMsg("550e8400-e29b-41d4-a716-446655440001", "old news", "ts", nil, "", "")
		historical.Historical = true
		conn.WriteJSON(historical)
		conn.WriteJSON(wire.NewHistoryComplete())
		conn.WriteJSON(wire.Error{Type: wire.TypeError, Message: "nope", Code: "revoked", Retryable: false})
		conn.WriteJSON(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "ext-1", OK: true})
		conn.WriteJSON(wire.ReactionChanged{
			Type: wire.TypeReactionChanged, ExternalID: "ext-2",
			PeerID: "550e8400-e29b-41d4-a716-446655440002", Reaction: "👍", Label: "Like", Action: "add",
		})
		conn.WriteJSON(wire.MessageEdited{Type: wire.TypeMessageEdited, ExternalID: "ext-3", Text: "corrected"})
		conn.WriteJSON(wire.ReactionAck{Type: wire.TypeReactionAck, ExternalID: "ext-4", Reaction: "👀", Action: "add", OK: true})
		conn.WriteJSON(wire.EditAck{Type: wire.TypeEditAck, ExternalID: "ext-5", OK: true})
		conn.WriteJSON(wire.MessageDeleted{Type: wire.TypeMessageDeleted, ExternalID: "ext-6", Cursor: "cursor-6"})
		conn.WriteJSON(wire.DeleteAck{Type: wire.TypeDeleteAck, ExternalID: "ext-7", OK: true})
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// Accumulate across repeated Drain calls until the last of the 10
	// rapid-fire server writes has actually been processed and buffered,
	// rather than draining as soon as merely the *first* one shows up —
	// under -race's scheduling overhead in particular, that first-event
	// trigger reliably fires well before the read loop has caught up on
	// the rest, making a single Peek-then-Drain a real, reproducible
	// flake rather than a hypothetical one.
	var formatted string
	var connected bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		chunk, stillConnected := c.Drain()
		formatted += chunk
		connected = stillConnected
		if strings.Contains(formatted, "delete acknowledged") || !connected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !connected {
		t.Fatalf("expected still connected, got formatted=%q", formatted)
	}
	if !strings.Contains(formatted, "[HUB HISTORY") {
		t.Fatalf("expected the historical msg to render distinctly, got: %s", formatted)
	}
	if !strings.Contains(formatted, "[hub: history request complete]") {
		t.Fatalf("expected historyComplete to render, got: %s", formatted)
	}
	if !strings.Contains(formatted, "code=revoked, retryable=false") {
		t.Fatalf("expected the error's code/retryable to render, got: %s", formatted)
	}
	if !strings.Contains(formatted, "send acknowledged") || !strings.Contains(formatted, "ext-1") {
		t.Fatalf("expected the sendAck to render, got: %s", formatted)
	}
	if !strings.Contains(formatted, "added a 👍 (Like) reaction") || !strings.Contains(formatted, "ext-2") {
		t.Fatalf("expected the reactionChanged to render, got: %s", formatted)
	}
	if !strings.Contains(formatted, "HUB MESSAGE EDITED") || !strings.Contains(formatted, "corrected") {
		t.Fatalf("expected the messageEdited to render, got: %s", formatted)
	}
	if !strings.Contains(formatted, "reaction add acknowledged") || !strings.Contains(formatted, "ext-4") {
		t.Fatalf("expected the reactionAck to render, got: %s", formatted)
	}
	if !strings.Contains(formatted, "edit acknowledged") || !strings.Contains(formatted, "ext-5") {
		t.Fatalf("expected the editAck to render, got: %s", formatted)
	}
	if !strings.Contains(formatted, "message deleted") || !strings.Contains(formatted, "ext-6") || !strings.Contains(formatted, "cursor-6") {
		t.Fatalf("expected the messageDeleted to render, got: %s", formatted)
	}
	if !strings.Contains(formatted, "delete acknowledged") || !strings.Contains(formatted, "ext-7") {
		t.Fatalf("expected the deleteAck to render, got: %s", formatted)
	}
}

func TestRequestHistorySendsHistoryMessage(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotHistory := make(chan wire.History, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		var h wire.History
		if err := conn.ReadJSON(&h); err == nil {
			gotHistory <- h
		}
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.RequestHistory("cursor-9", 25); err != nil {
		t.Fatalf("RequestHistory: %v", err)
	}

	select {
	case h := <-gotHistory:
		if h.Before != "cursor-9" || h.Limit != 25 {
			t.Fatalf("unexpected history request: %+v", h)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the history request")
	}
}

func TestRequestHistoryAfterSendsAfterField(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotHistory := make(chan wire.History, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.Joined{Type: wire.TypeJoined, PeerID: "550e8400-e29b-41d4-a716-446655440000", ServerVersion: wire.ProtocolVersion, HistoryAfter: true})
		var h wire.History
		if err := conn.ReadJSON(&h); err == nil {
			gotHistory <- h
		}
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if !c.HistoryAfterSupported() {
		t.Fatal("expected HistoryAfterSupported to be true when joined.historyAfter is set")
	}
	if err := c.RequestHistoryAfter("cursor-9", 25); err != nil {
		t.Fatalf("RequestHistoryAfter: %v", err)
	}

	select {
	case h := <-gotHistory:
		if h.After != "cursor-9" || h.Before != "" || h.Limit != 25 {
			t.Fatalf("unexpected history request: %+v", h)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the history request")
	}
}

func TestHistoryAfterSupportedFalseByDefault(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if c.HistoryAfterSupported() {
		t.Fatal("expected HistoryAfterSupported to be false against a plain mcp-hub-server")
	}
}

func TestReactSendsReactionMessage(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotReaction := make(chan wire.Reaction, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		var rq wire.Reaction
		if err := conn.ReadJSON(&rq); err == nil {
			gotReaction <- rq
		}
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.React("ext-1", "👍", "add"); err != nil {
		t.Fatalf("React: %v", err)
	}

	select {
	case rq := <-gotReaction:
		if rq.ExternalID != "ext-1" || rq.Reaction != "👍" || rq.Action != "add" {
			t.Fatalf("unexpected reaction request: %+v", rq)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the reaction request")
	}
}

func TestEditMessageSendsEditMessage(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotEdit := make(chan wire.Edit, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		var e wire.Edit
		if err := conn.ReadJSON(&e); err == nil {
			gotEdit <- e
		}
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.EditMessage("ext-1", "corrected", nil, "", ""); err != nil {
		t.Fatalf("EditMessage: %v", err)
	}

	select {
	case e := <-gotEdit:
		if e.ExternalID != "ext-1" || e.Text != "corrected" {
			t.Fatalf("unexpected edit request: %+v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the edit request")
	}
}

func TestEditMessageSendsAttachments(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotEdit := make(chan wire.Edit, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		var e wire.Edit
		if err := conn.ReadJSON(&e); err == nil {
			gotEdit <- e
		}
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	attachments := []wire.Attachment{{ContentType: "image/png", ContentBytes: "aGVsbG8="}}
	if err := c.EditMessage("ext-1", "corrected", attachments, "", ""); err != nil {
		t.Fatalf("EditMessage: %v", err)
	}

	select {
	case e := <-gotEdit:
		if len(e.Attachments) != 1 || e.Attachments[0].ContentType != "image/png" {
			t.Fatalf("expected attachments on the edit request, got: %+v", e.Attachments)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the edit request")
	}
}

func TestSendWithReplyToSetsField(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotMsg := make(chan wire.Msg, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		var m wire.Msg
		if err := conn.ReadJSON(&m); err == nil {
			gotMsg <- m
		}
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.Send("reply text", nil, "", "ext-orig"); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case m := <-gotMsg:
		if m.ReplyTo != "ext-orig" {
			t.Fatalf("expected replyTo=ext-orig on the sent message, got: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the send")
	}
}

func TestEditMessageSendsReplyTo(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotEdit := make(chan wire.Edit, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		var e wire.Edit
		if err := conn.ReadJSON(&e); err == nil {
			gotEdit <- e
		}
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.EditMessage("ext-1", "corrected", nil, "", "ext-orig"); err != nil {
		t.Fatalf("EditMessage: %v", err)
	}

	select {
	case e := <-gotEdit:
		if e.ReplyTo != "ext-orig" {
			t.Fatalf("expected replyTo=ext-orig on the edit request, got: %+v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the edit request")
	}
}

func TestRequestAttachmentReturnsFetchedBytes(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		var req wire.AttachmentRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}
		if req.Type != wire.TypeAttachment || req.Token != "att-3142" {
			return
		}
		conn.WriteJSON(wire.AttachmentData{
			Type: wire.TypeAttachmentData, Token: "att-3142", Name: "shot.png",
			ContentType: "image/webp", ContentBytes: "aGVsbG8=",
		})
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.RequestAttachment("att-3142")
	if err != nil {
		t.Fatalf("RequestAttachment: %v", err)
	}
	if !ok {
		t.Fatal("expected a reply within AckWaitTimeout")
	}
	if ev.Kind != "attachmentData" || ev.AttachmentToken != "att-3142" ||
		ev.AttachmentContentType != "image/webp" || ev.AttachmentContentBytes != "aGVsbG8=" {
		t.Fatalf("unexpected attachmentData event: %+v", ev)
	}
}

func TestRequestAttachmentSurfacesServerError(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", ""))
		var req wire.AttachmentRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}
		e := wire.NewError("no such attachment")
		e.Code = "not_found"
		conn.WriteJSON(e)
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.RequestAttachment("does-not-exist")
	if err != nil {
		t.Fatalf("RequestAttachment: %v", err)
	}
	if !ok {
		t.Fatal("expected a reply within AckWaitTimeout")
	}
	if ev.Kind != "error" || ev.Code != "not_found" {
		t.Fatalf("expected a not_found error event, got: %+v", ev)
	}
}

func TestBridgeFieldsOnJoinedFlowThroughToAccessors(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		cursor := "cursor-9"
		topic := "Support chat"
		j := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", 0, "", "")
		j.LatestCursor = &cursor
		j.HistoryLimitMax = 50
		j.CanSend = true
		j.ConversationKind = "oneOnOne"
		j.Topic = &topic
		conn.WriteJSON(j)
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()

	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	c, err := Dial(url, "6ba7b810-9dad-11d1-80b4-00c04fd430c8", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if got := c.LatestCursor(); got == nil || *got != "cursor-9" {
		t.Fatalf("got LatestCursor %v", got)
	}
	if c.HistoryLimitMax() != 50 {
		t.Fatalf("got HistoryLimitMax %d", c.HistoryLimitMax())
	}
	if !c.CanSend() {
		t.Fatal("expected CanSend true")
	}
	if c.ConversationKind() != "oneOnOne" {
		t.Fatalf("got ConversationKind %q", c.ConversationKind())
	}
	if got := c.Topic(); got == nil || *got != "Support chat" {
		t.Fatalf("got Topic %v", got)
	}
}

func TestBridgeFieldsAreZeroForAnOrdinaryConnect(t *testing.T) {
	url := startTestServer(t)
	c, err := Dial(url, "550e8400-e29b-41d4-a716-446655440000", DialOptions{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if got := c.LatestCursor(); got != nil {
		t.Fatalf("expected nil LatestCursor for mcp-hub-server, got %v", *got)
	}
	if c.HistoryLimitMax() != 0 || c.CanSend() || c.ConversationKind() != "" || c.Topic() != nil {
		t.Fatalf("expected all bridge fields zero, got HistoryLimitMax=%d CanSend=%v "+
			"ConversationKind=%q Topic=%v", c.HistoryLimitMax(), c.CanSend(), c.ConversationKind(), c.Topic())
	}
}

func TestPeersReportsNameAndAgePublicKeyOfOthers(t *testing.T) {
	url := startTestServer(t)
	sessionID := "550e8400-e29b-41d4-a716-446655440000"

	a, err := Dial(url, sessionID, DialOptions{})
	if err != nil {
		t.Fatalf("dial a: %v", err)
	}
	defer a.Close()
	activity := make(chan struct{}, 8)
	a.OnActivity(func() { activity <- struct{}{} })
	waitForActivity(t, activity) // a's own rosterComplete (no peers yet)

	b, err := Dial(url, sessionID, DialOptions{Name: "Alice", AgePublicKey: testAgePublicKey})
	if err != nil {
		t.Fatalf("dial b: %v", err)
	}
	defer b.Close()
	waitForActivity(t, activity) // a sees b's peerJoined

	peers := a.Peers()
	if len(peers) != 1 || peers[0].Name != "Alice" || peers[0].AgePublicKey != testAgePublicKey {
		t.Fatalf("expected b's name/agePublicKey to be reported, got %+v", peers)
	}
}
