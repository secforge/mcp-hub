package mcptools

import (
	"context"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/connstore"
)

func connectForRemove(t *testing.T, hub *Hub, name, link string, extra map[string]any) {
	t.Helper()
	req := mcp.CallToolRequest{}
	args := map[string]any{"as": name, "link": link}
	for k, v := range extra {
		args[k] = v
	}
	req.Params.Arguments = args
	if res, err := hub.handleConnect(context.Background(), req); err != nil || res.IsError {
		t.Fatalf("connect %s failed: err=%v result=%+v", name, err, res)
	}
}

func disconnectForRemove(t *testing.T, hub *Hub, name string, remove bool) string {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"connection": name, "remove": remove}
	res, err := hub.handleDisconnect(context.Background(), req)
	if err != nil || res.IsError {
		t.Fatalf("disconnect %s failed: err=%v result=%+v", name, err, res)
	}
	return textOf(res)
}

func callRemove(hub *Hub, args map[string]any) *mcp.CallToolResult {
	req := mcp.CallToolRequest{}
	req.Params.Arguments = args
	res, _ := hub.handleRemove(context.Background(), req)
	return res
}

// A plain disconnect keeps the stored identity — that is what lets the
// next connect resume it. remove: true deletes it, and says so.
func TestDisconnectRemovesTheStoredEntryOnlyWhenAsked(t *testing.T) {
	link, _ := startRelayTestServer(t)
	ctx := context.Background()
	hub := NewHub()

	connectForRemove(t, hub, testConn, link, nil)
	disconnectForRemove(t, hub, testConn, false)
	if _, ok, _ := connstore.Get(targetForLink(ctx, link)); !ok {
		t.Fatal("a plain disconnect removed the stored entry")
	}

	connectForRemove(t, hub, testConn, link, nil)
	text := disconnectForRemove(t, hub, testConn, true)
	if !strings.Contains(text, "stored entry is removed") {
		t.Errorf("the result does not say the entry was removed: %s", text)
	}
	if e, ok, _ := connstore.Get(targetForLink(ctx, link)); ok {
		t.Fatalf("remove: true left the entry behind: %+v", e)
	}
}

// remove: true deletes the entry the connection actually used, so a
// projectDir override is honoured rather than the default scope searched.
func TestDisconnectRemoveHonoursAProjectDirOverride(t *testing.T) {
	link, _ := startRelayTestServer(t)
	hub := NewHub()
	override := "/does-not-exist/remove-scope"

	connectForRemove(t, hub, testConn, link, map[string]any{"projectDir": override})
	scoped := connstore.Target{Link: link, Project: override}
	if _, ok, _ := connstore.Get(scoped); !ok {
		t.Fatal("setup: nothing stored under the override")
	}
	disconnectForRemove(t, hub, testConn, true)
	if e, ok, _ := connstore.Get(scoped); ok {
		t.Fatalf("the override's entry survived remove: true: %+v", e)
	}
}

func TestHubRemoveDeletesAStoredEntryByNameOrLink(t *testing.T) {
	linkA, _ := startRelayTestServer(t)
	linkB, _ := startRelayTestServer(t)
	ctx := context.Background()
	hub := NewHub()

	connectForRemove(t, hub, "alpha", linkA, nil)
	connectForRemove(t, hub, "beta", linkB, nil)

	disconnectForRemove(t, hub, "alpha", false)
	disconnectForRemove(t, hub, "beta", false)

	if res := callRemove(hub, map[string]any{"connection": "alpha"}); res.IsError {
		t.Fatalf("remove by name failed: %s", textOf(res))
	}
	if _, ok, _ := connstore.Get(targetForLink(ctx, linkA)); ok {
		t.Fatal("remove by name left the entry")
	}
	if res := callRemove(hub, map[string]any{"link": linkB}); res.IsError {
		t.Fatalf("remove by link failed: %s", textOf(res))
	}
	if _, ok, _ := connstore.Get(targetForLink(ctx, linkB)); ok {
		t.Fatal("remove by link left the entry")
	}
	if res := callRemove(hub, map[string]any{"connection": "alpha"}); !res.IsError {
		t.Fatalf("removing something already gone was not reported as no match: %s", textOf(res))
	}
}

func TestHubRemoveRefusesAnAmbiguousNameAndBadArguments(t *testing.T) {
	linkA, _ := startRelayTestServer(t)
	linkB, _ := startRelayTestServer(t)
	hub := NewHub()

	// Two links, each last opened as "same".
	connectForRemove(t, hub, "same", linkA, nil)
	disconnectForRemove(t, hub, "same", false)
	connectForRemove(t, hub, "same", linkB, nil)
	disconnectForRemove(t, hub, "same", false)

	if res := callRemove(hub, map[string]any{"connection": "same"}); !res.IsError ||
		!strings.Contains(textOf(res), "pass link") {
		t.Fatalf("an ambiguous name was not refused: %s", textOf(res))
	}
	if res := callRemove(hub, map[string]any{}); !res.IsError {
		t.Fatal("hub_remove with neither argument was not refused")
	}
	if res := callRemove(hub, map[string]any{"connection": "same", "link": linkA}); !res.IsError {
		t.Fatal("hub_remove with both arguments was not refused")
	}
}

// A name that is open means that connection, as it does for every tool:
// it is disconnected and its entry removed — from the scope it was opened
// under, a projectDir override included. A link held by an open
// connection is handled the same way.
func TestHubRemoveOfAnOpenConnectionDisconnectsAndRemovesIt(t *testing.T) {
	linkA, _ := startRelayTestServer(t)
	linkB, _ := startRelayTestServer(t)
	ctx := context.Background()
	hub := NewHub()
	override := "/does-not-exist/open-remove-scope"

	connectForRemove(t, hub, "alpha", linkA, map[string]any{"projectDir": override})
	res := callRemove(hub, map[string]any{"connection": "alpha"})
	if res.IsError || !strings.Contains(textOf(res), "disconnected first") ||
		!strings.Contains(textOf(res), "stored entry is removed") {
		t.Fatalf("removing an open connection by name did not disconnect and remove it: %s", textOf(res))
	}
	if _, err := hub.session("alpha"); err == nil {
		t.Fatal("the connection is still open")
	}
	if e, ok, _ := connstore.Get(connstore.Target{Link: linkA, Project: override}); ok {
		t.Fatalf("the entry under the override survived: %+v", e)
	}

	connectForRemove(t, hub, "beta", linkB, nil)
	if res := callRemove(hub, map[string]any{"link": linkB}); res.IsError ||
		!strings.Contains(textOf(res), "disconnected first") {
		t.Fatalf("removing an open connection by link did not disconnect it: %s", textOf(res))
	}
	if _, err := hub.session("beta"); err == nil {
		t.Fatal("the connection held by that link is still open")
	}
	if e, ok, _ := connstore.Get(targetForLink(ctx, linkB)); ok {
		t.Fatalf("the entry survived: %+v", e)
	}
}
