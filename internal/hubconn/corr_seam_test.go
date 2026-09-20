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

// Against a CORRELATING server, a history answer must be matched by the id
// the request carried, exactly as an ack and an error already are — not by
// the anchor it echoes.
//
// The anchor cannot separate two requests that asked the same question,
// and asking the same question twice is the documented recovery: a
// timed-out catch-up is retried with the same anchor. So request 1's late
// answer matches request 2's anchor and is handed over as request 2's own
// outcome, which is the misattribution the correlation id was added to
// end — still live in the one path that mints an id and then ignores it.
func TestAHistoryAnswerIsMatchedByItsCorrelationIDNotItsAnchor(t *testing.T) {
	upgrader := websocket.Upgrader{}
	var mu sync.Mutex
	var firstID string
	second := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
		joined.Features = map[string]json.RawMessage{
			wire.FeatureCorrelation: json.RawMessage(`{"maxLength":128}`),
			"messageAfter":          json.RawMessage(`{}`),
		}
		ws.WriteJSON(joined)

		n := 0
		for {
			var m wire.MessageAfter
			if err := ws.ReadJSON(&m); err != nil {
				return
			}
			if m.Type != wire.TypeMessageAfter {
				continue
			}
			n++
			if n == 1 {
				// Request 1: recorded and deliberately left unanswered
				// until request 2 is waiting.
				mu.Lock()
				firstID = m.ID
				mu.Unlock()
				continue
			}
			// Request 2 is now the outstanding claim. Answer request
			// ONE's id, with the anchor both of them share.
			mu.Lock()
			id := firstID
			mu.Unlock()
			ws.WriteJSON(wire.Msg{
				Type: wire.TypeMsg, ID: id,
				PeerID: "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
				Text:   "the answer to the FIRST request", TS: "ts", Cursor: "cursor-1",
				Historical: true,
				Answers:    &wire.Anchor{Cursor: "anchor-shared"},
			})
			close(second)
		}
	}))
	defer srv.Close()

	c, err := Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"#corr", DialOptions{ReconnectSecret: "s"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if !c.correlates {
		t.Fatal("expected the connection to correlate")
	}

	anchor := wire.Anchor{Cursor: "anchor-shared"}

	// Request 1 times out with its answer still in flight.
	if _, ok, err := c.RequestMessageAfterAwaiting(anchor); err != nil || ok {
		t.Fatalf("expected the first history request to time out, got ok=%v err=%v", ok, err)
	}

	// Request 2 asks the same question — the documented retry.
	ev, ok, err := c.RequestMessageAfterAwaiting(anchor)
	if err != nil {
		t.Fatalf("second request: %v", err)
	}
	if ok {
		mu.Lock()
		owner := firstID
		mu.Unlock()
		t.Fatalf("the second history request was handed the answer to the FIRST (id %q, text %q) — "+
			"a history claim is matched by the anchor it echoes rather than by the correlation id "+
			"it minted and sent, so a late answer is attributed to whoever asked the same question "+
			"next. Acks and errors on this same connection are matched by id.", owner, ev.Text)
	}

	select {
	case <-second:
	case <-time.After(2 * time.Second):
		t.Fatal("the server never answered the second request")
	}
}
