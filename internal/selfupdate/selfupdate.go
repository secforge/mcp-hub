// Package selfupdate answers the question a failed connect raises but
// cannot answer on its own: is this process even running the code that is
// installed?
//
// An MCP server is a long-lived subprocess. Replacing the binary on disk
// does nothing to the process already running from it, so "I rebuilt it"
// and "the running client has the fix" are different facts that look
// identical from the outside. When a connect starts failing after a server
// changes its handshake, the first thing worth knowing is which of those
// two situations this is — and the difference decides the fix: a restart,
// or an update.
package selfupdate

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/secforge/mcp-hub/internal/version"
)

// versionProbeTimeout bounds asking the on-disk binary what it is. It is
// a local exec of a file this process was itself launched from, so it
// either answers immediately or something is wrong enough that waiting
// longer will not help.
var versionProbeTimeout = 5 * time.Second

// ExecutablePath is the path this process was launched from, with the
// marker Linux appends once the file has been unlinked stripped off. That
// marker is the whole reason this is not just os.Executable: a binary
// replaced by the usual write-then-rename dance leaves the running
// process pointing at a deleted inode, and the path is still exactly
// where the new file lives.
func ExecutablePath() (string, error) {
	if executablePathForTest != "" {
		return executablePathForTest, nil
	}
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(exe, " (deleted)"), nil
}

// executablePathForTest stands in for os.Executable so the replace path
// can be exercised against a scratch file rather than this test binary.
var executablePathForTest string

// SetExecutablePathForTest points this package at a stand-in for the
// running binary and returns a function restoring the previous value.
// Exported for tests in other packages that drive the tool surface.
func SetExecutablePathForTest(path string) func() {
	prev := executablePathForTest
	executablePathForTest = path
	return func() { executablePathForTest = prev }
}

// OnDiskVersion asks the binary currently at this process's own path what
// version it is, by running it. Not read from the file's bytes: the
// version is a linker-injected string and a build's own report of it is
// the only authority on what it will claim once it runs.
func OnDiskVersion() (string, error) {
	exe, err := ExecutablePath()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), versionProbeTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, exe, "--version").Output()
	if err != nil {
		return "", fmt.Errorf("could not ask %s for its version: %w", exe, err)
	}
	return parseVersionOutput(string(out))
}

// parseVersionOutput takes the first line's version token — the format
// this package's own --version prints ("mcp-hub-client <version>"). A
// mismatch here means the file at our path is not an mcp-hub-client, which
// is worth saying rather than guessing around.
func parseVersionOutput(out string) (string, error) {
	first, _, _ := strings.Cut(strings.TrimSpace(out), "\n")
	fields := strings.Fields(first)
	if len(fields) < 2 || fields[0] != "mcp-hub-client" {
		return "", fmt.Errorf("unrecognized --version output: %q", first)
	}
	return fields[1], nil
}

// Staleness is what a failed connect needs to know before anything else.
type Staleness struct {
	// Running is the version of THIS process.
	Running string
	// OnDisk is the version of the binary at this process's own path, or
	// "" when it could not be determined (see Err).
	OnDisk string
	// Stale is true when the two differ: the installed binary has moved on
	// and this process is still the old one. A restart is the fix, and
	// checking for a newer release would answer the wrong question.
	Stale bool
	// Err records why OnDisk is unknown. Not fatal — a connect failure
	// still needs reporting, just without this half of the diagnosis.
	Err error
}

// Check compares this process against the binary it was launched from.
func Check() Staleness {
	s := Staleness{Running: version.Short()}
	onDisk, err := OnDiskVersion()
	if err != nil {
		s.Err = err
		return s
	}
	s.OnDisk = onDisk
	s.Stale = onDisk != s.Running
	return s
}

// Note is what to append to a failed connect, and it says which of the two
// situations this is rather than listing possibilities. Empty when there is
// nothing useful to add, so a caller can concatenate it unconditionally.
func (s Staleness) Note() string {
	switch {
	case s.Err != nil:
		return fmt.Sprintf("\n\nThis client is %s. The binary it was launched from could not be "+
			"asked its own version (%v), so whether an update is installed but unloaded is unknown "+
			"here.", s.Running, s.Err)
	case s.Stale:
		return fmt.Sprintf("\n\nWORTH CHECKING FIRST: this process is running %s, but the binary on "+
			"disk is now %s — it has been replaced since this MCP server started, and this process "+
			"is still the old one. If the failure above looks like a protocol or handshake change, "+
			"that is very likely the cause. Ask the user to restart the MCP server; do NOT look for "+
			"an update, the update is already installed.", s.Running, s.OnDisk)
	default:
		return fmt.Sprintf("\n\nThis client is %s, and that is also what is installed on disk — so "+
			"this is not a stale process running behind a newer binary. If the failure above looks "+
			"like the server having moved on, call hub_self_update() to check GitHub for a newer "+
			"release.", s.Running)
	}
}
