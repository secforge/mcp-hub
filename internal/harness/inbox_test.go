package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/secforge/harness-transport/udsmsg"
)

// The token cannot make this decision. Measured by harness-transport on
// 2026-09-16: a model replying from its own session presents the PEER
// token, not the child token, so RequireAuth proves "a session on this
// machine that can read my key file" — not "my parent". The kernel's pid
// is the identity; the token is only the gate.
func TestOnlyTheParentPidIsAccepted(t *testing.T) {
	in := &Inbox{parent: 4242}
	var got []string
	in.onMessage = func(text string) { got = append(got, text) }

	frame := &udsmsg.Frame{Message: &udsmsg.UserMessage{Content: "relay me"}}

	for _, tc := range []struct {
		name   string
		peer   *udsmsg.Peer
		accept bool
	}{
		{"our own parent", &udsmsg.Peer{PID: 4242, Identified: true}, true},
		{"another session on this machine", &udsmsg.Peer{PID: 9999, Identified: true}, false},
		{"a peer the kernel would not identify", &udsmsg.Peer{PID: 4242}, false},
		{"no peer at all", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := len(got)
			in.handleUser(context.Background(), tc.peer, frame)
			if accepted := len(got) > before; accepted != tc.accept {
				t.Fatalf("accepted=%v, want %v", accepted, tc.accept)
			}
		})
	}
}

// An unidentified peer carrying the RIGHT pid is still refused: a pid the
// kernel did not vouch for is a number the sender could have chosen, and
// on a platform that cannot report credentials it would be every reply.
func TestAnUnverifiedPidIsNotEnough(t *testing.T) {
	in := &Inbox{parent: 4242}
	delivered := false
	in.onMessage = func(string) { delivered = true }

	in.handleUser(context.Background(), &udsmsg.Peer{PID: 4242, Identified: false},
		&udsmsg.Frame{Message: &udsmsg.UserMessage{Content: "trust me"}})

	if delivered {
		t.Fatal("a peer the kernel did not identify was accepted on the strength of its pid")
	}
}

// Binding must fail rather than produce an inbox nobody can be checked
// against — a smaller version of this feature is not this feature.
func TestOpeningWithoutAHarnessSocketFails(t *testing.T) {
	restore := ClearEnvForTesting()
	defer restore()

	if _, err := OpenInbox(); err == nil {
		t.Fatal("an inbox was bound with no parent session to authenticate against")
	} else if !strings.Contains(err.Error(), "parent session") {
		t.Fatalf("error does not say why it refused: %v", err)
	}
}

// A socket name with no pid in it cannot be checked against, so it must
// refuse rather than accept everything.
func TestOpeningWithAnUnparseableSocketNameFails(t *testing.T) {
	t.Setenv(EnvClaudeSocketName, "/tmp/not-a-session-socket")
	os.Setenv(EnvClaudeSocketName, "/tmp/not-a-session-socket")

	if _, err := OpenInbox(); err == nil {
		t.Fatal("an inbox was bound from a socket name carrying no pid")
	}
}

// Empty text is not a message: relaying it would put a blank line on the
// hub for every frame that carries none.
func TestAFrameWithNoTextIsNotRelayed(t *testing.T) {
	in := &Inbox{parent: 1}
	delivered := false
	in.onMessage = func(string) { delivered = true }

	for _, f := range []*udsmsg.Frame{
		nil,
		{},
		{Message: &udsmsg.UserMessage{Content: "   "}},
	} {
		in.handleUser(context.Background(), &udsmsg.Peer{PID: 1, Identified: true}, f)
	}
	if delivered {
		t.Fatal("an empty frame was relayed to the hub")
	}
}

// The harness wraps every SendMessage in a cross-session envelope, so the
// frame carries the wrapper around the model's words rather than the
// words. Relaying it verbatim put the whole envelope on the hub.
func TestTheHarnessEnvelopeIsStrippedBeforeRelaying(t *testing.T) {
	wrapped := `<cross-session-message from="uds:/run/user/0/cc-socks/340459.sock" ` +
		`hop-chain="1ca8f04fa71a3491891044eb" from-name="mcp-hub (build)" from-mode="prompting">` +
		"\nthe words the model actually wrote\n</cross-session-message>"

	got := frameText(&udsmsg.Frame{Message: &udsmsg.UserMessage{Content: wrapped}})

	if got != "the words the model actually wrote" {
		t.Fatalf("frameText = %q, want the body alone", got)
	}
	for _, leaked := range []string{"cross-session-message", "hop-chain", "from-name"} {
		if strings.Contains(got, leaked) {
			t.Errorf("the envelope's %q reached the hub", leaked)
		}
	}
}

// A body that cannot be extracted safely is relayed whole rather than
// truncated at whatever a failed parse thought was the end.
func TestUnwrappableContentIsRelayedAsItIs(t *testing.T) {
	plain := "no envelope here, just a message"
	if got := frameText(&udsmsg.Frame{Message: &udsmsg.UserMessage{Content: plain}}); got != plain {
		t.Fatalf("frameText = %q, want the content unchanged", got)
	}
}

// The entry must say what this process IS. Appearing as an interactive
// session would be the impersonation the design refused; appearing as an
// mcp entry named for both the parent and the server is the third option.
func TestThePublishedEntryDoesNotClaimToBeASession(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	if err := os.MkdirAll(filepath.Join(dir, ".claude", "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}

	e, err := udsmsg.NewMCPEntry("/tmp/test-inbox.sock", "mcp-hub (build)", "mcp-hub2")
	if err != nil {
		t.Fatalf("NewMCPEntry: %v", err)
	}
	if err := udsmsg.PublishSession(e); err != nil {
		t.Skipf("registry not writable here: %v", err)
	}
	defer udsmsg.UnpublishSession(e.PID)

	raw, err := os.ReadFile(filepath.Join(dir, ".claude", "sessions",
		strconv.Itoa(e.PID)+".json"))
	if err != nil {
		t.Fatalf("reading the published entry: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("the entry is not valid JSON: %v", err)
	}
	if got["kind"] == "interactive" || got["entrypoint"] == "cli" {
		t.Errorf("the entry claims to be an interactive session: %v", got)
	}
	if name, _ := got["name"].(string); !strings.Contains(name, "mcp-hub2") {
		t.Errorf("the entry does not name which MCP server it is: %q", name)
	}
	if got["pid"] == nil || got["messagingSocketPath"] == "" {
		t.Errorf("the entry is not addressable: %v", got)
	}
}
