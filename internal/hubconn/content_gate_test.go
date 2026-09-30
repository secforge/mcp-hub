package hubconn

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

// The socket-level gate, which every send and edit passes through —
// including a plain reply's "#hub replyTo=" directive, which never reaches
// the tool-level check.
func TestSendRefusesUndeclaredContentBeforeWriting(t *testing.T) {
	upgrader := websocket.Upgrader{}
	wrote := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
		joined.Features = map[string]json.RawMessage{"messageAfter": json.RawMessage(`{}`)}
		ws.WriteJSON(joined)
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
			wrote <- struct{}{}
		}
	}))
	defer srv.Close()

	c, err := Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"#gate", DialOptions{ReconnectSecret: "s"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.Send("x", nil, "", "ext-1", nil); err == nil || !strings.Contains(err.Error(), `"replyTo"`) {
		t.Errorf("replyTo: want a refusal naming the feature, got %v", err)
	}
	if err := c.SendTo("x", "550e8400-e29b-41d4-a716-446655440001", nil, "", "", []wire.Mention{{Name: "a"}}); err == nil || !strings.Contains(err.Error(), `"mentions"`) {
		t.Errorf("mentions: want a refusal naming the feature, got %v", err)
	}
	att := []wire.Attachment{{ContentType: "image/png", ContentBytes: "AA=="}}
	if err := c.Send("x", att, "", "", nil); err == nil || !strings.Contains(err.Error(), `"attachments"`) {
		t.Errorf("attachments: want a refusal naming the feature, got %v", err)
	}
	select {
	case <-wrote:
		t.Fatal("a refused message reached the server")
	default:
	}
}
