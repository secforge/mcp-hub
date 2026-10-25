package mcptools

import (
	"fmt"
	"testing"

	"github.com/secforge/mcp-hub/internal/agenttest"
	"github.com/secforge/mcp-hub/internal/wire"
)

func TestEveryConversationComesBackAfterAnAnnouncedRestart(t *testing.T) {
	var grants []wire.Assignment
	for i := 0; i < 68; i++ {
		grants = append(grants, agentGrant(fmt.Sprintf("conv-%d", i), fmt.Sprintf("Chat %d", i), wire.AccessReadWrite))
	}
	srv := agenttest.New(t, grants...)
	h := NewHub()
	connectAgentForTest(t, h, srv)
	srv.Send(map[string]any{"type": "serverStopping", "reconnectAfter": 1})
	srv.CloseAgent(1012, "restart")
	waitUntil(t, "the agent to reconnect", func() bool { return srv.Dials() == 2 })
	waitUntil(t, "every conversation open again", func() bool {
		for _, g := range grants {
			s := childNamed(t, h, g.Conv)
			c, _ := s.activeConn()
			if c == nil || !c.Connected() {
				return false
			}
		}
		return true
	})
}

// A conversation that already had a session and could not be reopened
// with its agent is retried until it is open, not left marked
// "reconnecting" for good.
func TestAConversationWhoseReopenFailsIsRetriedUntilOpen(t *testing.T) {
	srv := agenttest.New(t, agentGrant("conv-1", "Alpha", wire.AccessReadWrite),
		agentGrant("conv-2", "Beta", wire.AccessReadWrite))
	h := NewHub()
	connectAgentForTest(t, h, srv)
	srv.RefuseOpen("conv-2", "unavailable")
	srv.Drop()
	waitUntil(t, "the agent to reconnect", func() bool { return srv.Dials() == 2 && srv.Opens("conv-2") >= 2 })
	srv.RefuseOpen("conv-2", "")
	s := childNamed(t, h, "conv-2")
	waitUntil(t, "conv-2 open again", func() bool {
		c, _ := s.activeConn()
		return c != nil && c.Connected()
	})
}
