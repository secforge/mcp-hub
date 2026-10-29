package hubconn

import (
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/agenttest"
	"github.com/secforge/mcp-hub/internal/wire"
)

// A conversation whose own keepalive deadline runs out while the agent
// socket stays up has ended, and opening it again on the same agent
// socket must work: its old entry may not stand in the way.
func TestAConversationThatTimedOutCanBeOpenedAgain(t *testing.T) {
	srv := agenttest.New(t, grant("conv-1", "Alpha", wire.AccessReadWrite))
	a := dialAgent(t, srv)
	c := openConv(t, a, "conv-1")
	a.mu.Lock()
	v := a.convs["conv-1"]
	a.mu.Unlock()
	if err := v.SetReadDeadline(time.Now()); err != nil {
		t.Fatal(err)
	}
	waitDisconnected(t, c)
	again, err := a.Open("conv-1", nil)
	if err != nil {
		t.Fatalf("reopening after a timeout: %v", err)
	}
	if !again.Connected() {
		t.Fatal("the reopened conversation is not connected")
	}
}
