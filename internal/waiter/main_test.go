package waiter

import (
	"os"
	"testing"
)

// TestMain points socketDir at a throwaway temp dir for the whole test
// binary run, so tests (especially the sweep ones) never touch the real
// OS temp dir — which, on a dev machine, is where other live sessions'
// real wait sockets actually live.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mcp-hub-waiter-test-")
	if err != nil {
		panic(err)
	}
	socketDir = dir
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}
