package mcptools

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/secforge/mcp-hub/internal/wire"
)

func TestPartialReadMustNotClaimComplete(t *testing.T) {
	calls := 0
	link, _ := startFilterServer(t, nil, func(wire.MessageAfter) filterServerReply {
		calls++
		if calls == 1 {
			return filterServerReply{msg: "first message"}
		}
		return filterServerReply{errCode: "unavailable"}
	})
	h := connectForFilters(t, link)
	res := readWith(t, h, map[string]any{"at": "2026-09-19T00:00:00Z", "limit": 3})
	var text string
	for _, c := range res.Content {
		if v, ok := c.(mcp.TextContent); ok {
			text += v.Text
		}
	}
	if strings.Contains(text, "everything there was") {
		t.Fatalf("partial read falsely claimed completeness: %s", text)
	}
}

func TestDisconnectCancelsInFlightReconnect(t *testing.T) {
	up := websocket.Upgrader{}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		if calls.Add(1) == 2 {
			close(entered)
			<-release
		}
		ws.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "review", ""))
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer ts.Close()
	link := "ws" + strings.TrimPrefix(ts.URL, "http") + "#review"
	h := connectForFilters(t, link)
	defer h.Shutdown()
	s := sole(t, h)
	c, _, _ := s.clearActiveConn()
	c.OnActivity(nil)
	c.Close()
	done := make(chan struct{})
	go func() { s.reconnectOnce(link, "review", 0, 1, false, 0); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reconnect did not start")
	}
	h.handleDisconnect(context.Background(), connReqFor(testConn))
	unblock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnect did not finish")
	}
	restored, _ := s.activeConn()
	if restored != nil {
		s.mu.Lock()
		s.redialLink = ""
		s.mu.Unlock()
		restored.OnActivity(nil)
		restored.Close()
		t.Fatal("explicit disconnect completed, but in-flight reconnect installed a live orphan connection")
	}
}

func TestSameCursorOnTwoConnectionsCanBeConfirmed(t *testing.T) {
	h := NewHub()
	a, err := h.open("a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := h.open("b")
	if err != nil {
		t.Fatal(err)
	}
	a.noteDelivered("same-cursor")
	b.noteDelivered("same-cursor")
	if other := h.cursorBelongsElsewhere(a, "same-cursor"); other != "" {
		t.Fatalf("valid cursor attributed only to %s even though a delivered it too", other)
	}
}
