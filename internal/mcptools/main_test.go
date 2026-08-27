package mcptools

import (
	"os"
	"testing"

	"github.com/secforge/mcp-hub/internal/waiter"
)

// TestMain isolates state these tests indirectly create via a real
// handleConnect: the PoC session log files (wsserver.NewHandler) and the
// wait sockets (waiter.Listen, whose Listen call now also sweeps stale
// sockets from its directory — see waiter.SocketDirForTesting). Without
// both, this package's tests would leak files into (or, worse, sweep real
// stale-but-legitimate socket files out of) the real OS temp dir, since
// this package is the only one outside internal/waiter itself that
// exercises Listen for real.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mcp-hub-mcptools-logs")
	if err != nil {
		panic(err)
	}
	os.Setenv("MCP_HUB_LOG_DIR", dir)
	restoreSocketDir := waiter.SocketDirForTesting(dir)
	code := m.Run()
	restoreSocketDir()
	os.RemoveAll(dir)
	os.Exit(code)
}
