package main

import (
	"flag"
	"os"

	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/mcptools"
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "wait" {
		fs := flag.NewFlagSet("wait", flag.ExitOnError)
		socket := fs.String("socket", "", "path to the wait unix socket")
		follow := fs.Bool("follow", false, "keep the connection open and print each "+
			"delivery as it arrives, instead of exiting after one")
		fs.Parse(os.Args[2:])
		os.Exit(runWait(*socket, *follow, os.Stdout, os.Stderr))
	}

	s := server.NewMCPServer("mcp-hub-client", "0.1.0", server.WithToolCapabilities(false))
	mcptools.NewHub().Register(s)
	if err := server.ServeStdio(s); err != nil {
		os.Exit(1)
	}
}
