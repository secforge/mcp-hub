package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/httpmcp"
	"github.com/secforge/mcp-hub/internal/wsserver"
)

// buildMux wires the existing websocket relay together with the new
// HTTP-MCP endpoint and its companion watch stream, all sharing one
// hubsession.Manager (via wsHandler.Manager()) so a websocket peer and an
// HTTP-MCP peer can join the very same hub session. /mcp and /watch are
// exact, static routes; everything else (in particular "/{sessionId}")
// falls through to wsHandler's own internal mux, unmodified.
func buildMux() *http.ServeMux {
	wsHandler := wsserver.NewHandler()

	mcpSrv := httpmcp.NewServer(wsHandler.Manager())
	mcpServer := server.NewMCPServer("mcp-hub-server", "0.1.0",
		server.WithToolCapabilities(false),
		server.WithHooks(mcpSrv.Hooks()),
	)
	mcpSrv.Register(mcpServer)
	streamable := server.NewStreamableHTTPServer(mcpServer)

	mux := http.NewServeMux()
	mux.Handle("/mcp", streamable)
	mux.HandleFunc("/watch", mcpSrv.WatchHandler())
	mux.Handle("/", wsHandler)
	return mux
}

func main() {
	addr := flag.String("addr", ":8765", "listen address")
	certFile := flag.String("tls-cert", "", "TLS certificate file (optional)")
	keyFile := flag.String("tls-key", "", "TLS key file (optional)")
	flag.Parse()

	if (*certFile == "") != (*keyFile == "") {
		log.Fatal("-tls-cert and -tls-key must both be set, or both left empty")
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           buildMux(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	if useTLS(*certFile, *keyFile) {
		log.Printf("mcp-hub-server listening on %s (tls)", *addr)
		log.Fatal(srv.ListenAndServeTLS(*certFile, *keyFile))
	} else {
		log.Printf("mcp-hub-server listening on %s", *addr)
		log.Fatal(srv.ListenAndServe())
	}
}
