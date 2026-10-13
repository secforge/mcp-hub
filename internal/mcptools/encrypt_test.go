package mcptools

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/hubenc"
	"github.com/secforge/mcp-hub/internal/wire"
)

// hub_connect with a keyFile joins an encrypted session, says so, and
// remembers the file's path — not the key — for reconnects.
func TestConnectWithAKeyFileJoinsTheEncryptedSession(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	k, _ := hubenc.NewKey()
	keyFile := filepath.Join(t.TempDir(), "session.key")
	if err := os.WriteFile(keyFile, []byte(k+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	key, _ := hubenc.ParseKey(k)
	salt, _ := hubenc.NewSalt()
	check, _ := hubenc.Check(key, salt)

	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		j := wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "", "")
		j.Encryption = &wire.Encryption{V: 1, Salt: salt, Check: check}
		ws.WriteJSON(j)
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)
	link := "ws" + strings.TrimPrefix(srv.URL, "http") + "/hub/join#enc-connect"

	ctx := context.Background()
	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": link, "keyFile": keyFile}
	res, err := hub.handleConnect(ctx, req)
	if err != nil || res.IsError {
		t.Fatalf("connect: %v %s", err, textOf(res))
	}
	t.Cleanup(func() { hub.handleDisconnect(ctx, connReqFor(testConn)) })
	if !strings.Contains(textOf(res), "END-TO-END ENCRYPTED with the key in "+keyFile) {
		t.Errorf("connect result does not say the session is encrypted:\n%s", textOf(res))
	}
	if strings.Contains(textOf(res), k) {
		t.Fatal("the key itself appeared in the connect result")
	}
	stored, _, _ := connstore.Get(sole(t, hub).catchUpID)
	if stored.KeyFile != keyFile {
		t.Errorf("stored keyFile %q, want %q", stored.KeyFile, keyFile)
	}
	raw, _ := os.ReadFile(filepath.Join(os.Getenv("MCP_HUB_CONNSTORE_DIR"), "state.json"))
	if strings.Contains(string(raw), k) {
		t.Fatal("the key was written to the connection store")
	}
}
