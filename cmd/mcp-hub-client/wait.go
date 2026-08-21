package main

import (
	"fmt"
	"io"
	"net"
)

func runWait(socketPath string, stdout, stderr io.Writer) int {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		fmt.Fprintf(stderr, "could not connect to %s: %v\n", socketPath, err)
		return 1
	}
	defer conn.Close()
	if _, err := io.Copy(stdout, conn); err != nil {
		fmt.Fprintf(stderr, "read error: %v\n", err)
		return 1
	}
	return 0
}
