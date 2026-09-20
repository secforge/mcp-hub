package hubconn

// Drop into internal/hubconn. Both FAIL at 94ab26e.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

// Finding 1: a late answer that lands while NO claim is pending never spends
// the debt expectLateAnswer recorded, so the next prompt answer is spent
// against it and that caller times out — and its own timeout re-adds the
// debt, so every later request of the kind times out too.
func TestALateAnswerDebtIsSpentWhenTheAnswerArrivesUnclaimed(t *testing.T) {
	prev := AckWaitTimeout
	AckWaitTimeout = 150 * time.Millisecond
	defer func() { AckWaitTimeout = prev }()

	upgrader := websocket.Upgrader{}
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
		joined.Features = map[string]json.RawMessage{"actionAcks": json.RawMessage(`{}`)}
		ws.WriteJSON(joined)
		// ONE WRITER AT A TIME — the held answer goes out on its own
		// goroutine while the loop answers later frames, and a websocket
		// permits one concurrent writer. Added here by mcp-hub; the same
		// fixture race this suite fixed elsewhere the same day.
		var writeMu sync.Mutex
		write := func(v any) {
			writeMu.Lock()
			defer writeMu.Unlock()
			ws.WriteJSON(v)
		}
		n := 0
		for {
			var frame map[string]any
			if err := ws.ReadJSON(&frame); err != nil {
				return
			}
			if frame["type"] != string(wire.TypeMsg) {
				continue
			}
			n++
			text, _ := frame["text"].(string)
			if n == 1 {
				// A's answer, held until A has given up and NOBODY is waiting.
				go func() {
					<-release
					write(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "ack-for-" + text, OK: true})
				}()
				continue
			}
			// Every later send is answered promptly with its own ack.
			write(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "ack-for-" + text, OK: true})
		}
	}))
	defer srv.Close()

	c, err := Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"#late", DialOptions{ReconnectSecret: "s"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// A times out.
	if _, ok, err := c.SendAwaitingAck("first", "", nil, "", "", nil); err != nil || ok {
		t.Fatalf("expected the first send to time out, got ok=%v err=%v", ok, err)
	}
	// A's late answer arrives while nothing is pending.
	close(release)
	time.Sleep(50 * time.Millisecond)

	// B, C, D each get their own prompt ack. Each must be answered.
	for _, name := range []string{"second", "third", "fourth"} {
		ev, ok, err := c.SendAwaitingAck(name, "", nil, "", "", nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !ok {
			t.Fatalf("%s: timed out although the server answered it promptly — the late-answer debt "+
				"from the first send was never spent (the late ack arrived with no claim pending) and "+
				"is now being charged to every later send", name)
		}
		if ev.ExternalID != "ack-for-"+name {
			t.Fatalf("%s: got someone else's ack %q", name, ev.ExternalID)
		}
	}
}

// Finding 2: a confirm the server REFUSES must not leave the refused cursor
// as this connection's consumed/acked position, or the next outbound message
// piggybacks the refused cursor again.
func TestARefusedConfirmDoesNotBecomeThePiggybackedCursor(t *testing.T) {
	upgrader := websocket.Upgrader{}
	gotMsg := make(chan wire.Msg, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
		joined.Features = map[string]json.RawMessage{"ackReplies": json.RawMessage(`{}`)}
		ws.WriteJSON(joined)
		// ONE WRITER AT A TIME — the held answer goes out on its own
		// goroutine while the loop answers later frames, and a websocket
		// permits one concurrent writer. Added here by mcp-hub; the same
		// fixture race this suite fixed elsewhere the same day.
		var writeMu sync.Mutex
		write := func(v any) {
			writeMu.Lock()
			defer writeMu.Unlock()
			ws.WriteJSON(v)
		}
		for {
			var frame map[string]any
			if err := ws.ReadJSON(&frame); err != nil {
				return
			}
			switch frame["type"] {
			case string(wire.TypeAck):
				write(wire.Error{Type: wire.TypeError, Code: "bad_ack_cursor",
					Message: "that cursor is not from this conversation"})
			case string(wire.TypeMsg):
				var m wire.Msg
				b, _ := json.Marshal(frame)
				_ = json.Unmarshal(b, &m)
				gotMsg <- m
			}
		}
	}))
	defer srv.Close()

	c, err := Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"#refused", DialOptions{ReconnectSecret: "s"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if _, err := c.ConfirmReceived("foreign-cursor"); err == nil {
		t.Fatal("expected the confirm to be reported as refused")
	}
	if got := c.LastConsumedCursor(); got == "foreign-cursor" {
		t.Errorf("refused cursor adopted as lastConsumed: %q", got)
	}
	if got := c.LastAckSentCursor(); got == "foreign-cursor" {
		t.Errorf("refused cursor recorded as lastAckSent: %q", got)
	}
	if err := c.Send("hello", nil, "", "", nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	select {
	case m := <-gotMsg:
		if m.AckCursor == "foreign-cursor" {
			t.Fatalf("the next send piggybacked the cursor the server had just REFUSED: %q", m.AckCursor)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never received the send")
	}
}
