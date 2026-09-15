package mcptools

import (
	"os"
	"testing"

	"github.com/secforge/mcp-hub/internal/harness"
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
// It also unsets the harness messaging environment, which is the one
// isolation failure that reaches outside this machine's temp directory
// and into a human's conversation. A test binary is a child of whatever
// launched `go test` — routinely the developer's own editor session — so
// it inherits that session's messaging socket and child token and is, by
// every check the protocol can make, a legitimate process pushing to its
// own parent. Observed on first wiring: an entire suite run's worth of
// fake hub events was delivered into the live session watching the tests.
// The credential cannot distinguish us; only we can.
func TestMain(m *testing.M) {
	restoreHarness := harness.ClearEnvForTesting()
	defer restoreHarness()
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
