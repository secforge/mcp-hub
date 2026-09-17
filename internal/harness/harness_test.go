package harness

import (
	"os"
	"strings"
	"testing"
)

// The attribution is what the model sees naming who spoke, and the field
// is truncated in display — so it has to be short, and its distinguishing
// part has to come last only if it survives. "mcp:<server>" puts the kind
// first and the identity where a reader looks for it.
func TestTheSenderNameIsShortAndNamesTheServer(t *testing.T) {
	t.Setenv(EnvServerName, "mcp-hub2")
	if got := senderName("", ""); got != "mcp:mcp-hub2" {
		t.Fatalf("senderName() = %q, want %q", got, "mcp:mcp-hub2")
	}
	if len(senderName("", "")) > 24 {
		t.Errorf("attribution %q is long enough to be truncated in display", senderName("", ""))
	}
}

// The harness tells an MCP server nothing about its registered alias, so
// an unset name must still produce something meaningful rather than
// "mcp:" or the executable's path.
func TestAnUnsetServerNameStillNamesSomething(t *testing.T) {
	os.Unsetenv(EnvServerName)
	if got := senderName("", ""); got != "mcp:mcp-hub" {
		t.Fatalf("senderName() = %q, want the product default", got)
	}
}

func TestSurroundingWhitespaceDoesNotLeakIntoTheAttribution(t *testing.T) {
	t.Setenv(EnvServerName, "  mcp-hub2  ")
	if got := senderName("", ""); got != "mcp:mcp-hub2" {
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
	if got := senderName("", learnServerName(map[string]any{"serverName": "mcp-hub2"})); got != "mcp:mcp-hub2" {
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
	if got := senderName("", "offered"); got != "mcp:chosen" {
		t.Fatalf("senderName = %q, want the configured name to win", got)
	}
}

// The receiving harness rewrites this field silently — stripping quotes
// and angle brackets, removing invisibles, truncating past 64 — so a name
// set by hand can arrive as something else with nothing reporting the
// difference. Normalising here makes the transformation ours and visible.
func TestAConfiguredNameSurvivesDisplayUnchanged(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{`mcp-hub2`, "mcp:mcp-hub2"},
		{`"mcp-hub2"`, "mcp:mcp-hub2"},
		{`<hub>`, "mcp:hub"},
		{"hub​two", "mcp:hubtwo"},
		{"  spaced  ", "mcp:spaced"},
		{strings.Repeat("x", 200), "mcp:" + strings.Repeat("x", 60)},
		{`"<>`, "mcp:mcp-hub"}, // nothing usable left: the default, not an empty label
	} {
		t.Setenv(EnvServerName, tc.raw)
		if got := senderName("", ""); got != tc.want {
			t.Errorf("senderName() for %q = %q, want %q", tc.raw, got, tc.want)
		}
		if n := len(senderName("", "")); n > 64 {
			t.Errorf("attribution for %q is %d chars — the harness will truncate it", tc.raw, n)
		}
	}
}

// The guard must clear every variable deliver.Open reads, not the subset
// that happened to matter on the machine where it was written: Open
// checks the Claude socket first and falls through to the Codex backend,
// which latches its target from CODEX_THREAD_ID.
func TestClearEnvForTestingClearsEveryHarnessVariable(t *testing.T) {
	for _, name := range harnessEnv {
		t.Setenv(name, "set")
	}
	restore := ClearEnvForTesting()
	for _, name := range harnessEnv {
		if v := os.Getenv(name); v != "" {
			t.Errorf("%s survived the guard as %q — a test could still reach a live harness", name, v)
		}
	}
	if PushOnly() {
		t.Error("push mode still engaged after the guard")
	}
	restore()
	for _, name := range harnessEnv {
		if os.Getenv(name) != "set" {
			t.Errorf("%s was not restored", name)
		}
	}
}

// A connection's own name outranks the configured server name: it is the
// most specific true thing about where a message came from, and the only
// one that matches the address a reply goes back to.
func TestAConnectionNameOutranksTheServerName(t *testing.T) {
	t.Setenv(EnvServerName, "mcp-hub2")
	if got := senderName("chat-relay", "learned"); got != "mcp:chat-relay" {
		t.Fatalf("senderName() = %q, want the connection's own name", got)
	}
	// And it is sanitised like any other, since it reaches a display that
	// would otherwise transform it silently.
	if got := senderName("chat relay!", ""); got != "mcp:chatrelay" {
		t.Fatalf("senderName() = %q, want the reduced form", got)
	}
}
