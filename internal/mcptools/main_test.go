package mcptools

import (
	"os"
	"testing"

	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/waiter"
)

// TestMain isolates state these tests indirectly create via a real
// handleConnect: the PoC session log files (wsserver.NewHandler), the
// wait sockets (waiter.Listen, whose Listen call now also sweeps stale
// sockets from its directory — see waiter.SocketDirForTesting), and the
// connstore connections file (MCP_HUB_CONNSTORE_DIR) — without this last
// one, these tests would read and write the real user's actual stored
// connections on whatever machine runs them. It also shortens hubconn's
// closeFlushGrace — see hubconn.SetCloseFlushGraceForTesting's doc
// comment — since this package's tests call handleDisconnect (and so
// Close) heavily over loopback, where the real network flush race that
// delay exists for doesn't occur.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mcp-hub-mcptools-logs")
	if err != nil {
		panic(err)
	}
	os.Setenv("MCP_HUB_LOG_DIR", dir)
	os.Setenv("MCP_HUB_CONNSTORE_DIR", dir)
	restoreSocketDir := waiter.SocketDirForTesting(dir)
	restoreCloseFlushGrace := hubconn.SetCloseFlushGraceForTesting(0)
	code := m.Run()
	restoreCloseFlushGrace()
	restoreSocketDir()
	os.RemoveAll(dir)
	os.Exit(code)
}
