package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBuildMuxServesWebsocketMCPAndWatchRoutes(t *testing.T) {
	mux := buildMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// /watch with no token: proves the route exists and reaches httpmcp's
	// handler, not a 404 from the mux itself having no such route at all.
	resp, err := http.Get(srv.URL + "/watch")
	if err != nil {
		t.Fatalf("GET /watch: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected /watch with no token to 404, got %d", resp.StatusCode)
	}

	// /mcp: proves the route exists and reaches the StreamableHTTPServer
	// (a GET with no session is expected to be rejected by mcp-go itself,
	// not 404 from the mux having no such route).
	resp2, err := http.Get(srv.URL + "/mcp")
	if err != nil {
		t.Fatalf("GET /mcp: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode == http.StatusNotFound {
		t.Fatal("expected /mcp to be routed to the StreamableHTTPServer, got 404")
	}

	// /{sessionId}: proves the pre-existing websocket route is still
	// reachable and unaffected — a plain GET (no upgrade) to a
	// syntactically valid sessionId still reaches wsserver's own handler,
	// which will reject it for lacking a websocket upgrade, not 404 the
	// route itself.
	resp3, err := http.Get(srv.URL + "/550e8400-e29b-41d4-a716-446655440000")
	if err != nil {
		t.Fatalf("GET /{sessionId}: %v", err)
	}
	resp3.Body.Close()
	if resp3.StatusCode == http.StatusNotFound {
		t.Fatal("expected the websocket sessionId route to still be reachable, got 404")
	}
}
