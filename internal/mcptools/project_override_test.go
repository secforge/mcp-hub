package mcptools

import (
	"context"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/connstore"
)

// An explicit projectDir scopes the WHOLE connection, not only the
// identity lookup at connect: the secret, the peer id and every later
// write — the confirmed read position here, and reconnects through the
// stored target — land under the override, and nothing lands under the
// default scope.
func TestProjectDirScopesEveryWriteForThatConnection(t *testing.T) {
	link, _ := startRelayTestServer(t)
	ctx := context.Background()
	override := "/does-not-exist/override-scope"

	hub := NewHub()
	connReq := mcp.CallToolRequest{}
	connReq.Params.Arguments = map[string]any{"as": testConn, "link": link, "projectDir": override + "/"}
	res, err := hub.handleConnect(ctx, connReq)
	if err != nil || res.IsError {
		t.Fatalf("connect failed: err=%v result=%+v", err, res)
	}
	if !strings.Contains(textOf(res), "SCOPE OVERRIDDEN") {
		t.Errorf("the connect result does not say the scope was overridden:\n%s", textOf(res))
	}

	sess := sole(t, hub)
	sess.mu.Lock()
	redial, catchUp := sess.redialTarget, sess.catchUpID
	sess.mu.Unlock()
	if redial.Project != override || catchUp.Project != override {
		t.Fatalf("the session holds redial=%q catchUp=%q, want both %q — a reconnect or a "+
			"position write would otherwise go to another scope", redial.Project, catchUp.Project, override)
	}

	list, _ := hub.handleListConnections(ctx, mcp.CallToolRequest{})
	if !strings.Contains(textOf(list), "scope overridden: "+override) {
		t.Errorf("hub_list_connections does not show the override on the open connection:\n%s", textOf(list))
	}

	confirmReq := mcp.CallToolRequest{}
	confirmReq.Params.Arguments = map[string]any{"connection": testConn, "cursor": "cursor-confirmed"}
	if res, err := hub.handleConfirmReceived(ctx, confirmReq); err != nil || res.IsError {
		t.Fatalf("hub_confirm failed: err=%v result=%+v", err, res)
	}
	hub.handleDisconnect(ctx, connReqFor(testConn))

	scoped := connstore.Target{Link: link, Project: override}
	entry, ok, err := connstore.Get(scoped)
	if err != nil || !ok || entry.PeerID == "" || entry.ReconnectSecret == "" {
		t.Fatalf("no identity stored under the override: entry=%+v ok=%v err=%v", entry, ok, err)
	}
	if cs, ok, _ := connstore.GetCatchUp(scoped); !ok || cs.Cursor != "cursor-confirmed" {
		t.Fatalf("the confirmed position did not land under the override: %+v ok=%v", cs, ok)
	}
	if def, ok, _ := connstore.Get(targetForLink(ctx, link)); ok {
		t.Fatalf("something was written under the default scope too: %+v", def)
	}
}

func TestProjectDirMustBeAbsolute(t *testing.T) {
	link, _ := startRelayTestServer(t)
	hub := NewHub()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": testConn, "link": link, "projectDir": "relative/dir"}
	res, err := hub.handleConnect(context.Background(), req)
	if err != nil || !res.IsError || !strings.Contains(textOf(res), "absolute") {
		t.Fatalf("a relative projectDir was not refused: err=%v result=%+v", err, res)
	}
	if len(hub.allSessions()) != 0 {
		t.Fatal("a refused connect kept its name")
	}
}
