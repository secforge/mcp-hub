package harness

import (
	"os"
	"testing"
)

// The attribution is what the model sees naming who spoke, and the field
// is truncated in display — so it has to be short, and its distinguishing
// part has to come last only if it survives. "mcp:<server>" puts the kind
// first and the identity where a reader looks for it.
func TestTheSenderNameIsShortAndNamesTheServer(t *testing.T) {
	t.Setenv(EnvServerName, "mcp-hub2")
	if got := senderName(); got != "mcp:mcp-hub2" {
		t.Fatalf("senderName() = %q, want %q", got, "mcp:mcp-hub2")
	}
	if len(senderName()) > 24 {
		t.Errorf("attribution %q is long enough to be truncated in display", senderName())
	}
}

// The harness tells an MCP server nothing about its registered alias, so
// an unset name must still produce something meaningful rather than
// "mcp:" or the executable's path.
func TestAnUnsetServerNameStillNamesSomething(t *testing.T) {
	os.Unsetenv(EnvServerName)
	if got := senderName(); got != "mcp:mcp-hub" {
		t.Fatalf("senderName() = %q, want the product default", got)
	}
}

func TestSurroundingWhitespaceDoesNotLeakIntoTheAttribution(t *testing.T) {
	t.Setenv(EnvServerName, "  mcp-hub2  ")
	if got := senderName(); got != "mcp:mcp-hub2" {
		t.Fatalf("senderName() = %q, want the trimmed name", got)
	}
}
