package hubconn

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

// A correlating server that does not echo the id on attachmentData (which
// is what chat-relay ships today) must still be able to answer a fetch.
func TestAnAttachmentAnswerWithNoIDStillReachesItsRequest(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		joined := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
		joined.Features = map[string]json.RawMessage{
			wire.FeatureCorrelation: json.RawMessage(`{"maxLength":128}`),
			"attachments":           json.RawMessage(`{}`),
		}
		ws.WriteJSON(joined)
		for {
			var req wire.AttachmentRequest
			if err := ws.ReadJSON(&req); err != nil {
				return
			}
			if req.Type != wire.TypeAttachment {
				continue
			}
			// Answers the token, echoes NO correlation id.
			ws.WriteJSON(wire.AttachmentData{
				Type: wire.TypeAttachmentData, Token: req.Token,
				ContentType:  "text/plain",
				ContentBytes: base64.StdEncoding.EncodeToString([]byte("hello")),
			})
		}
	}))
	defer srv.Close()

	c, err := Dial("ws"+strings.TrimPrefix(srv.URL, "http")+"#corr3", DialOptions{ReconnectSecret: "s"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ev, ok, err := c.RequestAttachment("att-1", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if !ok {
		t.Fatal("the attachment answer never reached its request — it carries no correlation id, " +
			"and the ack branch demands one whenever the claim holds one, so every attachment " +
			"fetch against a correlating server times out and the bytes are discarded")
	}
	if ev.AttachmentToken != "att-1" {
		t.Fatalf("wrong token: %q", ev.AttachmentToken)
	}
}
