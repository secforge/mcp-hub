package httpmcp

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/agekey"
	"github.com/secforge/mcp-hub/internal/hubsession"
	"github.com/secforge/mcp-hub/internal/sanitize"
	"github.com/secforge/mcp-hub/internal/wire"
	"github.com/secforge/mcp-hub/internal/wsserver"
)

// httpHub is the per-MCP-session state: at most one hub session membership
// at a time, mirroring how mcp-hub-client's single-process Hub wraps a
// nilable *hubconn.Conn — here scoped to one MCP client session instead of
// one OS process, since one mcp-hub-server process serves many concurrent
// MCP sessions.
type httpHub struct {
	mu   sync.Mutex
	p    *httpPeer
	sess *hubsession.Session
}

func (h *httpHub) peer() *httpPeer {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.p
}

func (h *httpHub) session() *hubsession.Session {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sess
}

// Server holds all per-MCP-session httpHubs and the watch-token index used
// by the /watch endpoint, and implements hub_connect/hub_disconnect's
// underlying logic (the MCP tool handlers themselves are added separately
// and just call these methods).
type Server struct {
	manager *hubsession.Manager

	hubs sync.Map // mcpSessionID string -> *httpHub

	tokens sync.Map // watchToken string -> *httpPeer
}

// NewServer wires a Server to manager — pass wsserver's own
// (*wsserver.Handler).Manager() so HTTP-MCP peers join the exact same
// hubsession.Session objects the websocket endpoint uses, not a second,
// disconnected set of sessions.
func NewServer(manager *hubsession.Manager) *Server {
	return &Server{manager: manager}
}

// hubFor returns mcpSessionID's httpHub, creating an empty one on first
// use. Never nil.
func (s *Server) hubFor(mcpSessionID string) *httpHub {
	v, _ := s.hubs.LoadOrStore(mcpSessionID, &httpHub{})
	return v.(*httpHub)
}

// connect implements hub_connect: joins sessionID (or a freshly generated
// one, if empty) as a new peer, on behalf of mcpSessionID. Errors if
// mcpSessionID is already connected (call disconnect first), or if
// sessionID/agePublicKey are malformed.
func (s *Server) connect(mcpSessionID, sessionID, name, agePublicKey, reconnectSecret string) (peerID, joinedSessionID, watchToken string, err error) {
	hub := s.hubFor(mcpSessionID)

	hub.mu.Lock()
	alreadyConnected := hub.p != nil
	hub.mu.Unlock()
	if alreadyConnected {
		return "", "", "", fmt.Errorf("already connected — call hub_disconnect first")
	}

	if sessionID == "" {
		sessionID = uuid.NewString()
	} else if !wire.IsValidID(sessionID) {
		return "", "", "", fmt.Errorf("sessionId is not a valid session id")
	}
	if agePublicKey != "" && !agekey.Valid(agePublicKey) {
		return "", "", "", fmt.Errorf("agePublicKey is not a validly formatted age public key")
	}
	name = sanitize.Text(name, wsserver.MaxNameRunes)
	if len(reconnectSecret) > wsserver.MaxReconnectSecretRunes {
		return "", "", "", fmt.Errorf("reconnectSecret too long")
	}

	hubSession := s.manager.GetOrCreate(sessionID)
	var peer *httpPeer
	hubSession.Join(reconnectSecret, func(id string) hubsession.Peer {
		peer = newHTTPPeer(id, name, agePublicKey)
		return peer
	}, nil)

	s.tokens.Store(peer.watchToken, peer)

	hub.mu.Lock()
	hub.p = peer
	hub.sess = hubSession
	hub.mu.Unlock()

	return peer.ID(), sessionID, peer.watchToken, nil
}

// disconnect implements hub_disconnect, and is also what the MCP session
// unregister hook calls when an MCP session ends without an explicit
// hub_disconnect. A no-op if mcpSessionID was never connected.
func (s *Server) disconnect(mcpSessionID string) {
	hub := s.hubFor(mcpSessionID)

	hub.mu.Lock()
	peer, hubSession := hub.p, hub.sess
	hub.p, hub.sess = nil, nil
	hub.mu.Unlock()

	if peer == nil {
		return
	}
	s.tokens.Delete(peer.watchToken)
	if hubSession.Leave(peer) {
		s.manager.Remove(hubSession.ID())
	}
}

// Hooks returns the server.Hooks that must be installed on the MCPServer
// wrapping this Server (via server.WithHooks) so a client disconnecting —
// without ever calling hub_disconnect itself — still leaves its hub
// session cleanly instead of leaking a phantom peer forever.
func (s *Server) Hooks() *server.Hooks {
	hooks := &server.Hooks{}
	hooks.AddOnUnregisterSession(func(ctx context.Context, session server.ClientSession) {
		s.disconnect(session.SessionID())
	})
	return hooks
}
