package mcptools

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/waiter"
	"github.com/secforge/mcp-hub/internal/wire"
)

// Hub bundles the single active hub connection + wait socket for one
// mcp-hub-client process.
type Hub struct {
	conn   *hubconn.Conn
	waiter *waiter.Waiter
}

func NewHub() *Hub {
	return &Hub{}
}

func (h *Hub) Register(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("hub_connect",
			mcp.WithDescription("Connect to an mcp-hub-server session"),
			mcp.WithString("host", mcp.Required(),
				mcp.Description("Server base address, e.g. ws://localhost:8765 (the "+
					"sessionId is appended as a path segment automatically)")),
			mcp.WithString("sessionId", mcp.Description(
				"UUID identifying the session to join. Omit to start a brand new "+
					"session — a UUID will be generated and returned; you must then "+
					"share it with whoever else should join")),
		),
		h.handleConnect,
	)
	s.AddTool(
		mcp.NewTool("hub_send",
			mcp.WithDescription("Send a text message to the current hub session"),
			mcp.WithString("text", mcp.Required(), mcp.Description("Message text")),
			mcp.WithString("to", mcp.Description(
				"Optional peerId to send this privately to a single peer instead of "+
					"broadcasting to everyone in the session")),
		),
		h.handleSend,
	)
	s.AddTool(
		mcp.NewTool("hub_disconnect",
			mcp.WithDescription("Disconnect from the current hub session")),
		h.handleDisconnect,
	)
	s.AddTool(
		mcp.NewTool("hub_receive",
			mcp.WithDescription("Drain and return currently buffered hub events without blocking")),
		h.handleReceive,
	)
	s.AddTool(
		mcp.NewTool("hub_peers",
			mcp.WithDescription("List the peerIds of everyone else currently in the hub session")),
		h.handlePeers,
	)
}

func (h *Hub) handleConnect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.conn != nil {
		return mcp.NewToolResultError("already connected; call hub_disconnect first"), nil
	}
	host, err := req.RequireString("host")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	sessionID := req.GetString("sessionId", "")
	generated := sessionID == ""
	if generated {
		sessionID = uuid.NewString()
	} else if !wire.IsValidID(sessionID) {
		return mcp.NewToolResultError("sessionId must be a UUID"), nil
	}
	conn, err := hubconn.Dial(host, sessionID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("connect failed: %v", err)), nil
	}
	w, err := waiter.Listen(sessionID, conn.PeerID(), conn)
	if err != nil {
		conn.Close()
		return mcp.NewToolResultError(fmt.Sprintf("could not start wait socket: %v", err)), nil
	}
	conn.OnActivity(w.Poke)
	h.conn = conn
	h.waiter = w

	const waitRequirement = "IMPORTANT: connecting alone does not deliver anything — " +
		"you will never see a message unless this wait command is actually running in the " +
		"background. Run it now, and again every time it completes, to keep receiving:"

	invite := fmt.Sprintf(
		"Propose this to the user so they can copy it to whoever else should join:\n"+
			"  Connect to the hub at %s with sessionId %s, then wait for messages.",
		host, sessionID,
	)

	if generated {
		return mcp.NewToolResultText(fmt.Sprintf(
			"Connected as peer %s in a new session: %s\n"+
				"Share this sessionId with whoever else should join — they need it to connect.\n"+
				"%s\n"+
				"%s\n%s",
			conn.PeerID(), sessionID, invite, waitRequirement, w.WaitCommand(),
		)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf(
		"Connected as peer %s.\n%s\n%s\n%s",
		conn.PeerID(), invite, waitRequirement, w.WaitCommand(),
	)), nil
}

func (h *Hub) handleSend(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	text, err := req.RequireString("text")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	to := req.GetString("to", "")
	if to == "" {
		if err := h.conn.Send(text); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("send failed: %v", err)), nil
		}
		return mcp.NewToolResultText("sent"), nil
	}
	if !wire.IsValidID(to) {
		return mcp.NewToolResultError("to must be a UUID"), nil
	}
	if err := h.conn.SendTo(text, to); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("send failed: %v", err)), nil
	}
	return mcp.NewToolResultText("sent (private)"), nil
}

func (h *Hub) handleDisconnect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	h.waiter.Close()
	h.conn.Close()
	h.conn = nil
	h.waiter = nil
	return mcp.NewToolResultText("disconnected"), nil
}

func (h *Hub) handleReceive(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	formatted, connected := h.conn.Drain()
	if !connected {
		return mcp.NewToolResultText("hub disconnected"), nil
	}
	if formatted == "" {
		return mcp.NewToolResultText("no messages"), nil
	}
	return mcp.NewToolResultText(formatted), nil
}

func (h *Hub) handlePeers(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if h.conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	peers := h.conn.Peers()
	if len(peers) == 0 {
		return mcp.NewToolResultText("no other peers currently in the session"), nil
	}
	return mcp.NewToolResultText("Current peers: " + strings.Join(peers, ", ")), nil
}
