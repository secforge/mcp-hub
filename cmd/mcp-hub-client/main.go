package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/mcptools"
	"github.com/secforge/mcp-hub/internal/version"
)

func main() {
	// Answered before anything else starts, and printed to stdout, because
	// this is what a running MCP server is asked to compare itself against
	// when a connect fails and the binary on disk may have moved on.
	if len(os.Args) >= 2 && (os.Args[1] == "--version" || os.Args[1] == "version" || os.Args[1] == "-version") {
		fmt.Println(version.String())
		return
	}
	if len(os.Args) >= 2 && os.Args[1] == "wait" {
		fs := flag.NewFlagSet("wait", flag.ExitOnError)
		socket := fs.String("socket", "", "path to the wait unix socket")
		follow := fs.Bool("follow", false, "keep the connection open and print each "+
			"delivery as it arrives, instead of exiting after one")
		fs.Parse(os.Args[2:])
		os.Exit(runWait(*socket, *follow, os.Stdout, os.Stderr))
	}

	s := server.NewMCPServer("mcp-hub-client", version.Short(), server.WithToolCapabilities(false), server.WithRoots())
	hub := mcptools.NewHub()
	hub.Register(s)
	// ServeStdio returns on SIGTERM/SIGINT (it already installs a handler
	// that cancels its context) or stdin EOF (the client closing the
	// pipe) — either way, this is our one chance to tear down cleanly
	// before the process exits: closing the wait socket lets a
	// backgrounded `wait --follow` CLI process see its connection end and
	// exit on its own, instead of lingering as an orphaned background
	// process. Unconditional, before the os.Exit below, so it still runs
	// on a non-nil return. Does not (cannot) run on a hard SIGKILL — no
	// process can catch that in any language.
	err := server.ServeStdio(s)
	hub.Shutdown()
	if err != nil {
		os.Exit(1)
	}
}
