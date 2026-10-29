package mcptools

import (
	"context"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/agenttest"
	"github.com/secforge/mcp-hub/internal/wire"
)

// A session whose harness shows no pushes never sees the startup notice,
// so the restart is said where it does look: the failing call.
func TestAConnectionThePreviousRunHeldIsNamedInTheError(t *testing.T) {
	h := NewHub()
	h.previousRun = []string{"debugger", "relay"}
	_, err := h.session("debugger")
	if err == nil || !strings.Contains(err.Error(), "RESTARTED") || !strings.Contains(err.Error(), "debugger, relay") {
		t.Fatalf("error: %v", err)
	}
	h.mu.Lock()
	h.restoredLocked("debugger")
	h.restoredLocked("relay")
	h.mu.Unlock()
	if _, err := h.session("typo"); err == nil || strings.Contains(err.Error(), "RESTARTED") {
		t.Fatalf("after everything was restored: %v", err)
	}
}

func TestAConfirmNamingAConnectionButNoCursorSaysHowToFreeTheWindow(t *testing.T) {
	srv := agenttest.New(t, agentGrant("conv-1", "Alpha", wire.AccessReadWrite))
	h := NewHub()
	connectAgentForTest(t, h, srv)
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"connection": childNamed(t, h, "conv-1").name}
	res, _ := h.handleConfirmReceived(context.Background(), req)
	if !res.IsError || !strings.Contains(textOf(res), "NEITHER connection nor cursor") {
		t.Fatalf("result: %s", textOf(res))
	}
}
