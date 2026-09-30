package harness

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/secforge/harness-transport/deliver"
)

// procRoot is where process information is read from; a test points it at
// a fabricated tree.
var procRoot = "/proc"

// ReleaseInheritedSession drops a messaging socket this process inherited
// without being the MCP server its session launched, and reports whether
// it did.
//
// A Claude Code session exports its socket and token to everything it
// spawns: shells, and whatever a shell starts. So a client started by
// `claude mcp list` from inside a session finds that session's socket in
// its environment and would push its own startup notices into a
// conversation it has nothing to do with. The socket is named after the
// session's pid, and the session's own MCP server descends from that
// process with no other Claude Code process in between; a stray one has a
// second `claude` — the command that started it — on the way up, or never
// reaches the owner at all.
//
// Wrappers between the session and its server (`sh -c`, an env shim) are
// not Claude Code processes and do not count. Where the ancestry cannot be
// read — no /proc — nothing is dropped: an unanswerable question keeps the
// behaviour every client had before this existed.
//
// Must run before anything reads the environment for a harness.
func ReleaseInheritedSession() bool {
	sock := os.Getenv(deliver.EnvClaudeSocket)
	if sock == "" {
		return false
	}
	owner, err := strconv.Atoi(strings.TrimSuffix(filepath.Base(sock), ".sock"))
	if err != nil || owner <= 0 {
		return false
	}
	if launchedBy(owner, os.Getppid()) {
		return false
	}
	os.Unsetenv(deliver.EnvClaudeSocket)
	os.Unsetenv(deliver.EnvClaudeToken)
	return true
}

// launchedBy reports whether walking up from parent reaches owner without
// passing another Claude Code process. It answers true whenever the
// ancestry cannot be read, so an unknown never releases a socket.
func launchedBy(owner, parent int) bool {
	pid := parent
	for depth := 0; depth < 64; depth++ {
		if pid == owner {
			return true
		}
		if pid <= 1 {
			return false
		}
		comm, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "comm"))
		if err != nil {
			// The first read failing means there is no ancestry to read on
			// this system; a later one means the chain broke, and a broken
			// chain has not been shown to reach the owner.
			return depth == 0
		}
		if strings.TrimSpace(string(comm)) == "claude" {
			return false
		}
		next, ok := parentOf(pid)
		if !ok {
			return false
		}
		pid = next
	}
	return false
}

// parentOf reads pid's parent from /proc/<pid>/stat. The command name in
// that line is parenthesised and may itself contain spaces or parentheses,
// so the fields are counted from the LAST closing parenthesis.
func parentOf(pid int) (int, bool) {
	raw, err := os.ReadFile(filepath.Join(procRoot, strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, false
	}
	s := string(raw)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, false
	}
	fields := strings.Fields(s[i+1:])
	if len(fields) < 2 {
		return 0, false
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, false
	}
	return ppid, true
}
