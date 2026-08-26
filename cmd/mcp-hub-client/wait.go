package main

import (
	"fmt"
	"io"
	"net"

	"github.com/secforge/mcp-hub/internal/waiter"
)

// runWait connects to the wait socket, sends the mode byte (ModeFollow if
// follow is set, ModeOnce otherwise), then copies whatever the server sends
// to stdout until the connection closes. In "once" mode that's a single
// delivery; in "follow" mode the server keeps the connection open and
// writes further chunks as they arrive, so this keeps running (and stdout
// keeps growing) until the connection is closed server-side (superseded by
// a newer wait, or the hub disconnects).
func runWait(socketPath string, follow bool, stdout, stderr io.Writer) int {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		fmt.Fprintf(stderr, "could not connect to %s: %v\n", socketPath, err)
		return 1
	}
	defer conn.Close()
	mode := byte(waiter.ModeOnce)
	if follow {
		mode = waiter.ModeFollow
	}
	if _, err := conn.Write([]byte{mode}); err != nil {
		fmt.Fprintf(stderr, "write error: %v\n", err)
		return 1
	}
	if _, err := io.Copy(stdout, conn); err != nil {
		fmt.Fprintf(stderr, "read error: %v\n", err)
		return 1
	}
	return 0
}
