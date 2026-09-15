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
	if got := senderName(""); got != "mcp:mcp-hub2" {
		t.Fatalf("senderName() = %q, want %q", got, "mcp:mcp-hub2")
	}
	if len(senderName("")) > 24 {
		t.Errorf("attribution %q is long enough to be truncated in display", senderName(""))
	}
}

// The harness tells an MCP server nothing about its registered alias, so
// an unset name must still produce something meaningful rather than
// "mcp:" or the executable's path.
func TestAnUnsetServerNameStillNamesSomething(t *testing.T) {
	os.Unsetenv(EnvServerName)
	if got := senderName(""); got != "mcp:mcp-hub" {
		t.Fatalf("senderName() = %q, want the product default", got)
	}
}

func TestSurroundingWhitespaceDoesNotLeakIntoTheAttribution(t *testing.T) {
	t.Setenv(EnvServerName, "  mcp-hub2  ")
	if got := senderName(""); got != "mcp:mcp-hub2" {
		t.Fatalf("senderName() = %q, want the trimmed name", got)
	}
}

// The harness may stamp the server's configured name into request
// metadata. Taking it from there means the attribution is right without
// anyone configuring anything — which is the outcome worth having, since
// a name that must be set by hand is a name that will be wrong on the
// entry someone forgot.
func TestAServerNameOfferedByTheHarnessIsAdopted(t *testing.T) {
	os.Unsetenv(EnvServerName)
	if got := senderName(learnServerName(map[string]any{"serverName": "mcp-hub2"})); got != "mcp:mcp-hub2" {
		t.Fatalf("senderName = %q, want the harness-offered name", got)
	}
	if got := learnServerName(map[string]any{"progressToken": "abc", "threadId": "t1"}); got != "" {
		t.Fatalf("learnServerName invented %q out of unrelated metadata", got)
	}
}

// An explicit setting is the operator saying what this entry is called,
// which outranks anything inferred from a key matched by shape.
func TestAnExplicitNameOutranksOneOfferedByTheHarness(t *testing.T) {
	t.Setenv(EnvServerName, "chosen")
	if got := senderName("offered"); got != "mcp:chosen" {
		t.Fatalf("senderName = %q, want the configured name to win", got)
	}
}
