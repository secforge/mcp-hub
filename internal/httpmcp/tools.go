package httpmcp

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/wire"
)

// mcpSessionID resolves the calling MCP client session's id from ctx, as
// set by mcp-go for every tool call. Every handler below calls this first.
func mcpSessionID(ctx context.Context) (string, error) {
	session := server.ClientSessionFromContext(ctx)
	if session == nil {
		return "", fmt.Errorf("no active MCP session")
	}
	return session.SessionID(), nil
}

// Register adds hub_connect, hub_disconnect, hub_send, hub_receive,
// hub_wait, and hub_peers to mcpServer.
func (s *Server) Register(mcpServer *server.MCPServer) {
	mcpServer.AddTool(
		mcp.NewTool("hub_connect",
			mcp.WithDescription("Join a hub session over this HTTP-MCP connection. Omit sessionId "+
				"to create a brand new session (its id is returned, to share with whoever else "+
				"should join); pass an existing one to join it. The result also includes a "+
				"watchToken — pass it to GET /watch?token=<token>&follow=1 (e.g. via curl -N) to "+
				"observe events asynchronously instead of blocking on hub_wait."),
			mcp.WithString("sessionId", mcp.Description("An existing session id to join; omit to create a new session")),
			mcp.WithString("name", mcp.Description("Optional display name shown to other peers")),
			mcp.WithString("agePublicKey", mcp.Description("Optional age recipient public key, shared with other peers")),
			mcp.WithString("reconnectSecret", mcp.Description("Optional secret that reclaims this same peer identity on a future reconnect to the same session")),
		),
		s.handleConnect,
	)
	mcpServer.AddTool(
		mcp.NewTool("hub_disconnect",
			mcp.WithDescription("Leave the current hub session, if connected")),
		s.handleDisconnect,
	)
	mcpServer.AddTool(
		mcp.NewTool("hub_send",
			mcp.WithDescription("Send a text message to the current hub session"),
			mcp.WithString("text", mcp.Required(), mcp.Description("Message text")),
			mcp.WithString("to", mcp.Description("Optional peerId to send this privately to a single peer instead of broadcasting to everyone in the session")),
		),
		s.handleSend,
	)
	mcpServer.AddTool(
		mcp.NewTool("hub_receive",
			mcp.WithDescription("Drain any events that have arrived since the last hub_receive/hub_wait, without blocking")),
		s.handleReceive,
	)
	mcpServer.AddTool(
		mcp.NewTool("hub_wait",
			mcp.WithDescription("Block until at least one new event arrives, or timeoutSeconds elapses"),
			mcp.WithNumber("timeoutSeconds", mcp.Description("How long to wait before giving up; default 30")),
		),
		s.handleWait,
	)
	mcpServer.AddTool(
		mcp.NewTool("hub_peers",
			mcp.WithDescription("List everyone else currently in the hub session")),
		s.handlePeers,
	)
}

func (s *Server) handleConnect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := mcpSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	peerID, sessionID, watchToken, err := s.connect(
		id,
		req.GetString("sessionId", ""),
		req.GetString("name", ""),
		req.GetString("agePublicKey", ""),
		req.GetString("reconnectSecret", ""),
	)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf(
		"connected — peerId=%s sessionId=%s watchToken=%s\n"+
			"Use watchToken with GET /watch?token=%s&follow=1 (e.g. curl -N) to observe events "+
			"asynchronously instead of blocking on hub_wait.",
		peerID, sessionID, watchToken, watchToken,
	)), nil
}

func (s *Server) handleDisconnect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := mcpSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	s.disconnect(id)
	return mcp.NewToolResultText("disconnected"), nil
}

func (s *Server) handleSend(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := mcpSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	hub := s.hubFor(id)
	peer, hubSession := hub.peer(), hub.session()
	if peer == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	text, err := req.RequireString("text")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	ts := time.Now().UTC().Format(time.RFC3339)
	if to := req.GetString("to", ""); to != "" {
		if err := hubSession.DeliverTo(peer, to, wire.NewDirectedMsg(peer.ID(), text, ts)); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText("sent"), nil
	}
	hubSession.Broadcast(peer, wire.NewBroadcastMsg(peer.ID(), text, ts))
	return mcp.NewToolResultText("sent"), nil
}

func (s *Server) handleReceive(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := mcpSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	peer := s.hubFor(id).peer()
	if peer == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	events := peer.Drain()
	if len(events) == 0 {
		return mcp.NewToolResultText("no new events"), nil
	}
	return mcp.NewToolResultText(hubconn.FormatEvents(events)), nil
}

func (s *Server) handleWait(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := mcpSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	peer := s.hubFor(id).peer()
	if peer == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	timeoutSeconds := req.GetFloat("timeoutSeconds", 30)
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds*float64(time.Second)))
	defer cancel()
	events, err := peer.Wait(waitCtx)
	if err != nil {
		return mcp.NewToolResultText("no new events"), nil
	}
	return mcp.NewToolResultText(hubconn.FormatEvents(events)), nil
}

func (s *Server) handlePeers(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id, err := mcpSessionID(ctx)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	hub := s.hubFor(id)
	peer, hubSession := hub.peer(), hub.session()
	if peer == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	var b strings.Builder
	for _, p := range hubSession.Peers() {
		if p.ID() == peer.ID() {
			continue
		}
		fmt.Fprintf(&b, "peer %s", p.ID())
		if p.Name() != "" {
			fmt.Fprintf(&b, " (%q)", p.Name())
		}
		if p.AgePublicKey() != "" {
			fmt.Fprintf(&b, " agePublicKey=%s", p.AgePublicKey())
		}
		b.WriteString("\n")
	}
	if b.Len() == 0 {
		return mcp.NewToolResultText("no other peers"), nil
	}
	return mcp.NewToolResultText(b.String()), nil
}
