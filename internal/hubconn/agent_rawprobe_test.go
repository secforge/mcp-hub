package hubconn

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/wire"
)

// TestAgentRawReadOnlyProbe sends ONE raw msg frame past the client's own
// read-only gate, into one conversation, so a live server's read_only
// refusal can be seen. It runs only when all three variables are set, and
// it refuses to send unless the server itself describes the conversation
// as an untitled read-only hub session.
func TestAgentRawReadOnlyProbe(t *testing.T) {
	link := os.Getenv("MCP_HUB_AGENT_RAW_PROBE_LINK")
	secret := os.Getenv("MCP_HUB_AGENT_RAW_PROBE_SECRET")
	conv := os.Getenv("MCP_HUB_AGENT_RAW_PROBE_CONV")
	if link == "" || secret == "" || conv == "" {
		t.Skip("raw probe variables not set")
	}
	_, a, err := DialAny(link, DialOptions{ReconnectSecret: secret, Name: "Claude Code (agent probe)"})
	if err != nil || a == nil {
		t.Fatalf("dial: %v (agent=%v)", err, a)
	}
	defer a.Close()
	var grant *wire.Assignment
	for _, it := range a.Assignments() {
		if it.Conv == conv {
			it := it
			grant = &it
		}
	}
	if grant == nil {
		t.Fatalf("%s is not among the assignments", conv)
	}
	if grant.Access != wire.AccessRead || grant.Title != "" || grant.Channel != "hub" {
		t.Fatalf("refusing to send: the server describes %s as %+v, not an untitled read-only hub session", conv, *grant)
	}
	c, err := a.Open(conv, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !c.ReadOnly() || c.ConversationKind() != "" {
		t.Fatalf("refusing to send: joined says access %q kind %q", c.Access(), c.ConversationKind())
	}
	inner, _ := json.Marshal(wire.NewOutgoingMsg("raw probe: this must be refused read_only"))
	raw, err := wire.WrapConv(conv, inner)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.writeRaw(raw); err != nil {
		t.Fatalf("write: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		evs, _ := c.DrainEvents()
		for _, e := range evs {
			t.Logf("EVENT kind=%s code=%s retryable=%v text=%q", e.Kind, e.Code, e.Retryable, e.Text)
			if e.Kind == "error" && e.Code == "read_only" {
				t.Logf("the server refused it with read_only")
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("no read_only refusal within 5s")
}
