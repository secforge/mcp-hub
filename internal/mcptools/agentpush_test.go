package mcptools

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/secforge/harness-transport/deliver"
	"github.com/secforge/harness-transport/udsmsg"

	"github.com/secforge/mcp-hub/internal/agenttest"
	"github.com/secforge/mcp-hub/internal/wire"
)

// harnessInbox stands in for the Claude Code session: it receives what
// the client pushes.
type harnessInbox struct {
	mu   sync.Mutex
	got  []string
	from []string
}

func newHarnessInbox(t *testing.T) *harnessInbox {
	t.Helper()
	in := &harnessInbox{}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	srv, err := udsmsg.Listen(udsmsg.Config{
		Path:                   dir + "/inbox.sock",
		AllowUnidentifiedPeers: true,
		Handler: udsmsg.Handler{OnUser: func(_ context.Context, _ *udsmsg.Peer, f *udsmsg.Frame) {
			if f.Message == nil {
				return
			}
			in.mu.Lock()
			in.got = append(in.got, f.Message.Content)
			in.from = append(in.from, f.From)
			in.mu.Unlock()
		}},
	})
	if err != nil {
		t.Fatalf("inbox: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	srv.Start(ctx)
	t.Cleanup(func() {
		cancel()
		srv.Close()
	})
	t.Setenv(deliver.EnvClaudeSocket, srv.Path())
	t.Setenv(deliver.EnvClaudeToken, srv.PeerToken())
	return in
}

func (in *harnessInbox) has(text string) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	for _, g := range in.got {
		if strings.Contains(g, text) {
			return true
		}
	}
	return false
}

func TestAMessageInAConversationIsPushed(t *testing.T) {
	in := newHarnessInbox(t)
	srv := agenttest.New(t, agentGrant("conv-1", "Alpha", wire.AccessReadWrite))
	h := NewHub()
	connectAgentForTest(t, h, srv)
	srv.Msg("conv-1", agentPeer, "first live message", "c-1")
	waitUntil(t, "the push", func() bool { return in.has("first live message") })
}

func TestAConversationSharedAtRuntimeIsPushed(t *testing.T) {
	in := newHarnessInbox(t)
	srv := agenttest.New(t, agentGrant("conv-1", "Alpha", wire.AccessReadWrite))
	h := NewHub()
	connectAgentForTest(t, h, srv)
	srv.Assign(agentGrant("conv-new", "Neu", wire.AccessRead))
	waitUntil(t, "the new conversation", func() bool { return srv.IsOpen("conv-new") })
	srv.Msg("conv-new", agentPeer, "message in the new one", "n-1")
	waitUntil(t, "the push from the new conversation", func() bool {
		return in.has("message in the new one")
	})
}

func TestAConversationReopenedAfterAServerRestartIsPushed(t *testing.T) {
	in := newHarnessInbox(t)
	srv := agenttest.New(t, agentGrant("conv-1", "Alpha", wire.AccessReadWrite))
	h := NewHub()
	connectAgentForTest(t, h, srv)
	srv.Assign(agentGrant("conv-new", "Neu", wire.AccessRead))
	waitUntil(t, "the new conversation", func() bool { return srv.IsOpen("conv-new") })
	dials := srv.Dials()
	srv.Drop()
	waitUntil(t, "the reconnect", func() bool {
		return srv.Dials() > dials && srv.IsOpen("conv-new") && srv.IsOpen("conv-1")
	})
	srv.Msg("conv-new", agentPeer, "after the restart", "n-2")
	srv.Msg("conv-1", agentPeer, "also after the restart", "c-2")
	waitUntil(t, "the pushes after the restart", func() bool {
		return in.has("after the restart") && in.has("also after the restart")
	})
}
