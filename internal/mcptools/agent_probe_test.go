package mcptools

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

// TestAgentProbe connects to a real agent server, for the joint probe with
// chat-relay. It runs only when MCP_HUB_AGENT_PROBE_LINK is set, and it
// sends only into the conversations named in MCP_HUB_AGENT_PROBE_SEND
// (comma-separated connection names), one message each.
//
//	MCP_HUB_AGENT_PROBE_LINK=wss://localhost:4081/agent/join#… \
//	SSL_CERT_FILE=/path/to/local-ca.pem \
//	go test ./internal/mcptools -run TestAgentProbe -v
func TestAgentProbe(t *testing.T) {
	link := os.Getenv("MCP_HUB_AGENT_PROBE_LINK")
	if link == "" {
		t.Skip("MCP_HUB_AGENT_PROBE_LINK not set")
	}
	t.Setenv("MCP_HUB_PROJECT_DIR", "/agent-probe")
	// The server binds the agent's secret on the first connect, so every
	// probe run has to present the same one: a store that outlives the run.
	if store := os.Getenv("MCP_HUB_AGENT_PROBE_STORE"); store != "" {
		t.Setenv("MCP_HUB_CONNSTORE_DIR", store)
	}
	h := NewHub()
	ctx := context.Background()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"as": "probe", "link": link, "name": "Claude Code (agent probe)"}
	res, err := h.handleConnect(ctx, req)
	if err != nil || res.IsError {
		t.Fatalf("connect: %v %s", err, textOf(res))
	}
	defer h.handleDisconnect(ctx, connReqFor("probe"))
	t.Logf("CONNECT RESULT:\n%s", textOf(res))

	list, _ := h.handleListConnections(ctx, mcp.CallToolRequest{})
	t.Logf("LIST:\n%s", textOf(list))

	for _, s := range h.allSessions() {
		if s.agent == nil {
			t.Fatalf("an ordinary session %q is open in a probe of an agent link", s.name)
		}
		conn, _ := s.activeConn()
		if conn == nil || !conn.Connected() {
			t.Errorf("%s is not connected", s.name)
			continue
		}
		t.Logf("%s: peer %s, access %q, behind %d, historyFrom %v", s.name, conn.PeerID(),
			conn.Access(), conn.Behind(), conn.HistoryFrom())
	}

	for _, s := range h.allSessions() {
		for i := 0; i < 30; i++ {
			res, err := h.handleCatchUp(ctx, connReqFor(s.name))
			text := textOf(res)
			first, _, _ := strings.Cut(text, "\n")
			t.Logf("CATCH-UP %s #%d: err=%v %.300s", s.name, i, err, first)
			if err != nil || res.IsError || strings.Contains(text, "caught up") ||
				strings.Contains(text, "nothing to catch up") {
				break
			}
		}
	}

	for _, name := range strings.Split(os.Getenv("MCP_HUB_AGENT_PROBE_SEND"), ",") {
		if name = strings.TrimSpace(name); name == "" {
			continue
		}
		res, err := h.handleSend(ctx, sendReq(name, "agent probe: hello from "+name+" at "+
			time.Now().UTC().Format(time.RFC3339)))
		t.Logf("SEND %s: err=%v isError=%v %s", name, err, res.IsError, textOf(res))
	}

	wait := 20 * time.Second
	if v, err := time.ParseDuration(os.Getenv("MCP_HUB_AGENT_PROBE_WAIT")); err == nil {
		wait = v
	}
	t.Logf("watching for %s …", wait)
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		for _, s := range h.allSessions() {
			conn, _ := s.activeConn()
			if conn == nil {
				continue
			}
			evs, _ := conn.DrainEvents()
			for _, e := range evs {
				t.Logf("EVENT on %s: kind=%s from=%s cursor=%s text=%q", s.name, e.Kind, e.PeerID, e.Cursor, e.Text)
			}
		}
		if note := h.takeAutoReconnectNote(); note != "" {
			t.Logf("NOTE: %s", note)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
