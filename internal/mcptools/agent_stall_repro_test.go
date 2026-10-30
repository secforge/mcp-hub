package mcptools

import (
	"fmt"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/agenttest"
	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/wire"
)

// With the delivery window full and the server pinging as usual, no
// conversation of an agent connection may end: they live as long as the
// agent socket does.
func TestAFullDeliveryWindowDoesNotEndConversations(t *testing.T) {
	in := newHarnessInbox(t)
	defer hubconn.SetPongWaitForTesting(600 * time.Millisecond)()
	var grants []wire.Assignment
	for i := 0; i < 20; i++ {
		grants = append(grants, agentGrant(fmt.Sprintf("conv-%d", i), fmt.Sprintf("Chat %d", i), wire.AccessReadWrite))
	}
	srv := agenttest.New(t, grants...)
	srv.PingEvery = 100 * time.Millisecond
	h := NewHub()
	connectAgentForTest(t, h, srv)
	before := map[string]any{}
	for _, g := range grants {
		c, _ := childNamed(t, h, g.Conv).activeConn()
		before[g.Conv] = c
	}
	for i := 0; i < 60; i++ {
		srv.Msg(fmt.Sprintf("conv-%d", i%20), agentPeer, fmt.Sprintf("message %d", i), fmt.Sprintf("m-%d", i))
	}
	waitUntil(t, "the first push", func() bool { return in.has("message 0") })
	time.Sleep(4 * time.Second)
	for _, g := range grants {
		c, _ := childNamed(t, h, g.Conv).activeConn()
		if c == nil || before[g.Conv] != any(c) || !c.Connected() {
			reason := ""
			if c != nil {
				reason = c.EndReason()
			}
			t.Fatalf("%s ended while the window was full (%s)", g.Conv, reason)
		}
	}
}
