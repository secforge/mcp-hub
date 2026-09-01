package httpmcp

import (
	"fmt"
	"net/http"

	"github.com/secforge/mcp-hub/internal/hubconn"
)

// WatchHandler serves GET /watch?token=<watchToken>[&follow=1] — a plain
// (non-MCP) http.Flusher-based text stream of one connected peer's own
// events, read-only: it neither joins a new peer nor consumes anything
// from the hub-protocol side. It exists because a blocking hub_wait tool
// call ties up a conversation turn; this lets a client (in practice,
// Claude backgrounding `curl -N` and watching it via the Monitor tool) get
// notified asynchronously instead, the same shape mcp-hub-client's own
// `wait --follow` already provides for its local unix-socket mechanism.
//
// follow=1 keeps streaming until the request's context is canceled (the
// client disconnects) or the peer is torn down (hub_disconnect, or its MCP
// session ending); omitted, it returns after the first batch of events and
// closes.
func (s *Server) WatchHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.URL.Query().Get("token")
		v, ok := s.tokens.Load(token)
		if !ok {
			http.Error(w, "unknown or expired watch token", http.StatusNotFound)
			return
		}
		peer := v.(*httpPeer)

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}
		follow := r.URL.Query().Get("follow") == "1"

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		ctx := r.Context()
		for {
			events, err := peer.Wait(ctx)
			if err != nil {
				return
			}
			for _, ev := range events {
				fmt.Fprintln(w, hubconn.FormatEvent(ev))
				fmt.Fprintln(w)
			}
			flusher.Flush()
			if !follow {
				return
			}
		}
	}
}
