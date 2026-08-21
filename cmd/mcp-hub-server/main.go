package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/secforge/mcp-hub/internal/wsserver"
)

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
		Handler:           wsserver.NewHandler(),
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
