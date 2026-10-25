package hubconn

import (
	"fmt"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/agenttest"
	"github.com/secforge/mcp-hub/internal/wire"
)

// One conversation not reading for a while must not stop the agent socket
// from reading: the others' frames and the keepalive pings that hold
// every conversation's deadline arrive through that same socket.
func TestAConversationThatStopsReadingDoesNotStallTheOthers(t *testing.T) {
	srv := agenttest.New(t, grant("conv-1", "Alpha", wire.AccessReadWrite),
		grant("conv-2", "Beta", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	busy := openConv(t, a, "conv-1")
	other := openConv(t, a, "conv-2")
	busy.mu.Lock()
	for i := 0; i < 200; i++ {
		srv.Msg("conv-1", agenttest.PeerFor("conv-1"), fmt.Sprintf("flood %d", i), fmt.Sprintf("c1.%d", i))
	}
	srv.Msg("conv-2", agenttest.PeerFor("conv-2"), "still here", "c2.1")
	done := make(chan struct{})
	go func() {
		waitEvents(t, other, func(evs []Event) bool { return hasText(evs, "still here") })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		busy.mu.Unlock()
		t.Fatal("a conversation that stopped reading stalled the agent socket for the others")
	}
	busy.mu.Unlock()
	waitEvents(t, busy, func(evs []Event) bool { return hasText(evs, "flood 199") })
}
