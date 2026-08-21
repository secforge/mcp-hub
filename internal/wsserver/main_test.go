package wsserver

import (
	"os"
	"testing"
)

// TestMain provides a default MCP_HUB_LOG_DIR for tests that don't care
// where the PoC session log ends up (tests that do care override it per-test
// via t.Setenv, which layers on top of this default safely).
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mcp-hub-wsserver-logs")
	if err != nil {
		panic(err)
	}
	os.Setenv("MCP_HUB_LOG_DIR", dir)
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}
