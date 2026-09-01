package hubconn

import (
	"os"
	"testing"
)

// TestMain isolates the PoC session log files these tests indirectly create
// (via wsserver.NewHandler) to a temp directory, instead of letting them
// leak into this package's source directory. It also shortens
// closeFlushGrace globally: its real-world default (200ms) exists to dodge
// a genuine network flush race, but on loopback in a test binary that race
// doesn't occur, and the delay would otherwise tax every test that calls
// Close() — tests that specifically need to observe the grace period
// (e.g. TestCloseWaitsForFlushGraceAfterSuccessfulWrite) override it back
// up to a measurable value themselves.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mcp-hub-hubconn-logs")
	if err != nil {
		panic(err)
	}
	os.Setenv("MCP_HUB_LOG_DIR", dir)
	closeFlushGrace = 0
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}
