package hubconn

import (
	"os"
	"testing"
)

// TestMain isolates the PoC session log files these tests indirectly create
// (via wsserver.NewHandler) to a temp directory, instead of letting them
// leak into this package's source directory.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mcp-hub-hubconn-logs")
	if err != nil {
		panic(err)
	}
	os.Setenv("MCP_HUB_LOG_DIR", dir)
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}
