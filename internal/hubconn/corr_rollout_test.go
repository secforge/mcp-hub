package hubconn

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

// A server that DECLARES correlation but has not yet echoed the id on this
// kind — every server mid-rollout — must not leave a caller worse off than
// one that never declared it at all.
//
// Declaring the feature turns off the late-answer debt, and an id-less
// answer now matches by kind alone (which is what keeps attachments and
// terminators working). Together those give back the exact
// misattribution the debt existed to prevent, with the mitigation
// switched off because the server promised something it has not finished
// delivering.
func TestALateIDLessAckIsNotHandedToTheNextCallerOnACorrelatingServer(t *testing.T) {
	upgrader := websocket.Upgrader{}
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
		joined.Features = map[string]json.RawMessage{
			wire.FeatureCorrelation: json.RawMessage(`{"maxLength":128}`),
			"actionAcks":            json.RawMessage(`{}`),
		}
		ws.WriteJSON(joined)
		var wmu sync.Mutex
		write := func(v any) { wmu.Lock(); defer wmu.Unlock(); ws.WriteJSON(v) }
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
				// This kind is not echoing the id yet: the ack carries
				// none, and is held until nobody is waiting for it.
				go func() {
					<-release
					write(wire.SendAck{Type: wire.TypeSendAck, ExternalID: "ack-for-" + text, OK: true})
				}()
				continue
			}
			// The second send is never answered, so anything it receives
			// came from the first.
		}
	}))
	defer srv.Close()

	c, err := Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"#rollout", DialOptions{ReconnectSecret: "s"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if _, ok, err := c.SendAwaitingAck("first", "", nil, "", "", nil); err != nil || ok {
		t.Fatalf("expected the first send to time out, got ok=%v err=%v", ok, err)
	}

	done := make(chan Event, 1)
	go func() {
		ev, _, _ := c.SendAwaitingAck("second", "", nil, "", "", nil)
		done <- ev
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)

	select {
	case ev := <-done:
		if ev.ExternalID == "ack-for-first" {
			t.Fatal("the second send was handed the FIRST send's ack and externalId. The server " +
				"declares correlation, so the late-answer debt is switched off; it has not echoed " +
				"the id on this kind, so the answer matches by kind alone. A server mid-rollout " +
				"leaves this client worse off than one that never declared the feature.")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the second send never returned")
	}
}
