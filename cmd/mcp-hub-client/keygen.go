package main

import (
	"fmt"
	"io"
	"os"

	"github.com/secforge/mcp-hub/internal/hubenc"
)

// runKeygen writes a new key for an end-to-end encrypted hub session to
// the given path, readable by its owner only. It refuses to overwrite an
// existing file: replacing a session's key makes everything sealed with
// the old one unreadable.
func runKeygen(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: mcp-hub-client keygen <key file>")
		return 2
	}
	key, err := hubenc.NewKey()
	if err != nil {
		fmt.Fprintf(stderr, "keygen: %v\n", err)
		return 1
	}
	f, err := os.OpenFile(args[0], os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(stderr, "keygen: %v (an existing key file is never overwritten)\n", err)
		return 1
	}
	if _, err := fmt.Fprintln(f, key); err != nil {
		f.Close()
		fmt.Fprintf(stderr, "keygen: %v\n", err)
		return 1
	}
	if err := f.Close(); err != nil {
		fmt.Fprintf(stderr, "keygen: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "wrote a new session key to %s (mode 0600). Every client of the session "+
		"needs this same file; share it out of band, never through the hub.\n", args[0])
	return 0
}
