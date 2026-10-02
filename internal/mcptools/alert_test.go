package mcptools

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/connstore"
)

// A client that cannot know its scope at startup — no MCP_HUB_PROJECT_DIR,
// no roots answered yet, as for a Codex caller — reports a previous run's
// connections on the FIRST tool call instead, in that call's result. Before,
// it said nothing at all, ever: a connection that ended with its process
// was gone without a word.
func TestTheStartupReportIsDeferredUntilTheScopeIsKnown(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	t.Setenv("MCP_HUB_PROJECT_DIR", "")
	ResetScopeCacheForTesting()
	t.Cleanup(ResetScopeCacheForTesting)
	target := connstore.Target{Link: "wss://example/hub/join#deferred", Project: connstore.CurrentProject()}
	if err := connstore.Upsert(target, connstore.Entry{
		LocalName: "relay", LastConnectedAt: time.Now().UTC(), Connected: true,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	h := NewHub()
	h.reportAbandonedConnections()
	if note := h.takeAutoReconnectNote(); note != "" {
		t.Fatalf("reported before the scope was known: %s", note)
	}
	handler := h.withReconnectNote(func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("tool output"), nil
	})
	res, _ := handler(context.Background(), mcp.CallToolRequest{})
	if got := textOf(res); !strings.Contains(got, "relay") || !strings.Contains(got, "NOT restored") {
		t.Fatalf("the first tool call's result does not carry the deferred report:\n%s", got)
	}
	res, _ = handler(context.Background(), mcp.CallToolRequest{})
	if got := textOf(res); strings.Contains(got, "NOT restored") {
		t.Fatalf("the report was said twice:\n%s", got)
	}
}

// Without a push channel an alert still reaches the reader — queued for
// the next tool call, which is the only route left.
func TestAnAlertWithoutAPushChannelIsQueued(t *testing.T) {
	h := NewHub()
	s, err := h.open("relay")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s.alert("Connection \"relay\" is DOWN.")
	if note := h.takeAutoReconnectNote(); !strings.Contains(note, "is DOWN") {
		t.Fatalf("the alert was lost: %q", note)
	}
}

// Notices about things that happen OUTSIDE a tool call must be alerts, so
// they are pushed where the harness takes deliveries. Asserted against the
// source, as the startup notice is, because the push half needs a live
// harness socket this suite deliberately does not have.
func TestOutOfCallNoticesArePushedNotOnlyQueued(t *testing.T) {
	src, err := os.ReadFile("tools.go")
	if err != nil {
		t.Fatalf("reading tools.go: %v", err)
	}
	for _, phrase := range []string{
		`is DOWN\.`,
		`ENDED and will NOT be reconnected`,
		`RECONNECTED AUTOMATICALLY`,
		`Automatic reconnect dialled successfully but could`,
	} {
		re := regexp.MustCompile(`s\.(alert|note)\(fmt\.Sprintf\("(?:[^"\\]|\\.)*` + phrase)
		m := re.FindStringSubmatch(string(src))
		if m == nil {
			t.Errorf("no notice matching %q found", phrase)
			continue
		}
		if m[1] != "alert" {
			t.Errorf("the %q notice is queued only (s.note), so it waits for a tool call that may never come", phrase)
		}
	}
}
