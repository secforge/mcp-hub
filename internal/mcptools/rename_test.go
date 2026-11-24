package mcptools

import (
	"context"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/agenttest"
	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/wire"
)

func renameReq(from, to string) mcp.CallToolRequest {
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"connection": from, "as": to}
	return req
}

func TestARenamedConnectionKeepsItsIdentity(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	link, _ := startRelayTestServer(t)
	ctx := context.Background()
	h := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": "old", "link": link}
	if res, err := h.handleConnect(ctx, req); err != nil || res.IsError {
		t.Fatalf("connect: %v %s", err, textOf(res))
	}
	defer h.handleDisconnect(ctx, connReqFor("new"))
	before, _, _ := connstore.Get(connstore.Target{Project: connstore.CurrentProject(), Link: link})

	for _, bad := range []string{"Bad Name", "old"} {
		res, _ := h.handleRename(ctx, renameReq("old", bad))
		if bad == "old" && !strings.Contains(textOf(res), "nothing changed") {
			t.Fatalf("same name: %s", textOf(res))
		}
		if bad != "old" && !res.IsError {
			t.Fatalf("invalid name accepted: %s", textOf(res))
		}
	}
	res, err := h.handleRename(ctx, renameReq("old", "new"))
	if err != nil || res.IsError || !strings.Contains(textOf(res), `is now "new"`) {
		t.Fatalf("rename: %v %s", err, textOf(res))
	}
	if _, err := h.session("old"); err == nil {
		t.Fatal("the old name still answers")
	}
	s, err := h.session("new")
	if err != nil {
		t.Fatal(err)
	}
	if c, _ := s.activeConn(); c == nil || !c.Connected() {
		t.Fatal("not connected under the new name")
	}
	after, _, _ := connstore.Get(connstore.Target{Project: connstore.CurrentProject(), Link: link})
	if after.ReconnectSecret != before.ReconnectSecret || after.ReconnectSecret == "" || after.LocalName != "new" {
		t.Fatalf("identity not kept: before %+v after %+v", before, after)
	}
}

func TestARenamedAgentRenamesItsConversations(t *testing.T) {
	srv := agenttest.New(t, agentGrant("conv-1", "Alpha", wire.AccessReadWrite),
		agentGrant("conv-2", "Beta", wire.AccessReadWrite))
	h := NewHub()
	connectAgentForTest(t, h, srv)
	ctx := context.Background()
	before := childNamed(t, h, "conv-2")
	res, err := h.handleRename(ctx, renameReq(before.name, "beta"))
	if err != nil || res.IsError || !strings.Contains(textOf(res), `is now "beta"`) {
		t.Fatalf("child rename: %v %s", err, textOf(res))
	}
	if s := childNamed(t, h, "conv-2"); s.name != "beta" {
		t.Fatalf("child is %q", s.name)
	} else if c, _ := s.activeConn(); c == nil || !c.Connected() {
		t.Fatal("renamed child not open")
	}
	if _, err := h.session(before.name); err == nil {
		t.Fatal("the old child name still answers")
	}
	if res, _ := h.handleRename(ctx, renameReq("beta", "desk")); !res.IsError {
		t.Fatalf("a taken name was accepted: %s", textOf(res))
	}
	if res, _ := h.handleRename(ctx, renameReq("desk", "a-very-long-agent-name")); !res.IsError {
		t.Fatalf("long agent name accepted: %s", textOf(res))
	}
	res, err = h.handleRename(ctx, renameReq("desk", "front"))
	if err != nil || res.IsError {
		t.Fatalf("rename: %v %s", err, textOf(res))
	}
	t.Cleanup(func() { h.handleDisconnect(ctx, connReqFor("front")) })
	waitUntil(t, "the conversations under their names", func() bool {
		one, two := false, false
		for _, s := range h.allSessions() {
			switch s.conv {
			case "conv-1":
				one = strings.HasPrefix(s.name, "front-")
			case "conv-2":
				two = s.name == "beta"
			}
		}
		return one && two
	})
}
