package mcptools

import (
	"strings"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/harness"
)

// In push mode the return of a dropped connection is announced to the
// reader, so the refusal while it is down must not claim nobody will say.
func TestTheReconnectingRefusalSaysTheReturnIsAnnouncedInPushMode(t *testing.T) {
	codexPushOnly.Store(true)
	t.Cleanup(func() { codexPushOnly.Store(false) })
	h := NewHub()
	s, err := h.open("t")
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.pusher = harness.PusherForTesting(nil)
	s.reconnectAt = time.Now().Add(time.Minute)
	s.mu.Unlock()
	text := textOf(s.notConnected())
	if strings.Contains(text, "NOT be told") || !strings.Contains(text, "RECONNECTED notice") {
		t.Fatalf("push-mode refusal:\n%s", text)
	}
}
