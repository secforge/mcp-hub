package mcptools

import (
	"testing"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/harness"
)

// testConn is the name every test connects under. One name is enough:
// these tests exercise one connection's behaviour, and the multi-
// connection rules have their own tests.
const testConn = "t"

// sole returns the one open session. Tests may resolve a connection this
// way because a test knows what it connected; a tool call may not, which
// is what Hub.forRequest is for.
func sole(t *testing.T, h *Hub) *session {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.sessions) != 1 {
		t.Fatalf("expected exactly one open connection, have %d %v", len(h.sessions), h.names())
	}
	for _, s := range h.sessions {
		return s
	}
	return nil
}

// mustOpen registers a session without connecting it, for the tests that
// exercise per-connection state directly rather than over a live socket.
func mustOpen(t *testing.T, h *Hub) *session {
	t.Helper()
	s, err := h.open(testConn)
	if err != nil {
		t.Fatalf("opening a session: %v", err)
	}
	return s
}

// connReqFor builds a request naming a connection and nothing else, for
// the tools that take no other argument.
func connReqFor(name string) mcp.CallToolRequest {
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"connection": name}
	return req
}

// harnessAvailableForTest reports whether this test process has a harness
// to bind an inbox against. The suite clears the environment by default
// (see TestMain), so the answer is normally no — and a test that asserts
// on addresses has to say which case it is in rather than reading an
// absent inbox as a failure.
func harnessAvailableForTest() bool { return harness.PushMode() }
