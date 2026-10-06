package mcptools

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/wire"
)

// A link the server refuses outright at the upgrade — revoked while this
// client was away, claimed by another client, or gone — gets the same
// answer on every dial, so automatic reconnect stops instead of retrying
// for ever. A server error while it restarts is still retried.
func TestAReconnectRefusedAtTheUpgradeIsNotRetried(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   reconnectOutcome
	}{
		{http.StatusUnauthorized, reconnectFatal},
		{http.StatusNotFound, reconnectFatal},
		{http.StatusBadGateway, reconnectRetry},
	} {
		up := websocket.Upgrader{}
		var refuse atomic.Bool
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if refuse.Load() {
				http.Error(w, "no", tc.status)
				return
			}
			ws, err := up.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer ws.Close()
			ws.WriteJSON(wire.NewJoined("550e8400-e29b-41d4-a716-446655440000", "review", ""))
			for {
				if _, _, err := ws.ReadMessage(); err != nil {
					return
				}
			}
		}))
		link := "ws" + strings.TrimPrefix(ts.URL, "http") + "#refused"
		h := connectForFilters(t, link)
		s := sole(t, h)
		c, _, _ := s.clearActiveConn()
		c.OnActivity(nil)
		c.Close()

		refuse.Store(true)
		if got := s.reconnectOnce(link, "review", 0, 1, false, 0); got != tc.want {
			t.Errorf("status %d: reconnect outcome %v, want %v", tc.status, got, tc.want)
		}
		h.Shutdown()
		ts.Close()
	}
}
