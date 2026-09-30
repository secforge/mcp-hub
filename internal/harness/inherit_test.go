package harness

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/secforge/harness-transport/deliver"
)

// fakeProc builds a /proc tree: pid -> {comm, parent}.
func fakeProc(t *testing.T, procs map[int][2]string) {
	t.Helper()
	root := t.TempDir()
	for pid, p := range procs {
		dir := filepath.Join(root, strconv.Itoa(pid))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		os.WriteFile(filepath.Join(dir, "comm"), []byte(p[0]+"\n"), 0o600)
		// The name field is parenthesised and may contain spaces and
		// parentheses; the parent is counted from after the LAST ')'.
		os.WriteFile(filepath.Join(dir, "stat"),
			[]byte(fmt.Sprintf("%d (%s) S %s 1 1 0", pid, p[0], p[1])), 0o600)
	}
	prev := procRoot
	procRoot = root
	t.Cleanup(func() { procRoot = prev })
}

func TestLaunchedByFollowsTheAncestry(t *testing.T) {
	const owner = 100
	fakeProc(t, map[int][2]string{
		owner: {"claude", "1"},
		200:   {"sh", "100"},           // a wrapper the session launched the server through
		300:   {"bash", "100"},         // the session's tool shell
		310:   {"claude", "300"},       // `claude mcp list`, started from that shell
		320:   {"odd) name (x", "200"}, // a comm that breaks naive parsing, under the wrapper
		400:   {"bash", "1"},           // a shell outside the session entirely
		500:   {"sh", "999"},           // a chain that breaks: 999 has no entry
	})
	for _, tc := range []struct {
		name   string
		parent int
		want   bool
	}{
		{"direct child of the session", owner, true},
		{"through a wrapper", 200, true},
		{"through a wrapper with an awkward name", 320, true},
		{"started by claude mcp list", 310, false},
		{"outside the session", 400, false},
		{"chain breaks before the owner", 500, false},
	} {
		if got := launchedBy(owner, tc.parent); got != tc.want {
			t.Errorf("%s: launchedBy = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Where the ancestry cannot be read at all, nothing is released.
func TestLaunchedByKeepsWhenAncestryIsUnreadable(t *testing.T) {
	prev := procRoot
	procRoot = filepath.Join(t.TempDir(), "no-proc-here")
	t.Cleanup(func() { procRoot = prev })
	if !launchedBy(100, 12345) {
		t.Fatal("an unreadable ancestry released the socket")
	}
}

func TestReleaseInheritedSessionDropsAStraySocket(t *testing.T) {
	restore := ClearEnvForTesting()
	t.Cleanup(restore)
	parent := os.Getppid()
	// This process's real parent, presented as a Claude Code process that
	// is not the socket's owner — the `claude mcp list` shape.
	fakeProc(t, map[int][2]string{parent: {"claude", "1"}})
	t.Setenv(deliver.EnvClaudeSocket, "/run/user/0/cc-socks/99999999.sock")
	t.Setenv(deliver.EnvClaudeToken, "token")

	if !ReleaseInheritedSession() {
		t.Fatal("a socket inherited through another claude process was kept")
	}
	if os.Getenv(deliver.EnvClaudeSocket) != "" || os.Getenv(deliver.EnvClaudeToken) != "" {
		t.Fatal("released, but the socket or token is still in the environment")
	}
	if PushOnly() {
		t.Fatal("PushOnly still reports a harness after release")
	}
}

func TestReleaseInheritedSessionKeepsTheSessionsOwnSocket(t *testing.T) {
	restore := ClearEnvForTesting()
	t.Cleanup(restore)
	t.Setenv(deliver.EnvClaudeSocket, fmt.Sprintf("/run/user/0/cc-socks/%d.sock", os.Getppid()))
	t.Setenv(deliver.EnvClaudeToken, "token")
	if ReleaseInheritedSession() {
		t.Fatal("the socket of this process's own parent session was released")
	}
	if os.Getenv(deliver.EnvClaudeSocket) == "" {
		t.Fatal("the session's own socket was removed from the environment")
	}
}

func TestReleaseInheritedSessionIgnoresAnUnrecognisedSocketName(t *testing.T) {
	restore := ClearEnvForTesting()
	t.Cleanup(restore)
	t.Setenv(deliver.EnvClaudeSocket, "/tmp/some-other-harness.sock")
	if ReleaseInheritedSession() {
		t.Fatal("a socket whose name carries no owner pid was released")
	}
}
