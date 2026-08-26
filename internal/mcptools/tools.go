package mcptools

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/agekey"
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
			mcp.WithString("name", mcp.Description(
				"Optional untrusted display name shown alongside the server log and "+
					"reported to other peers (sanitized server-side: control characters "+
					"stripped, length capped)")),
			mcp.WithString("agePublicKey", mcp.Description(
				"Optional age (https://age-encryption.org) public key ('age1...'), "+
					"format-validated but otherwise untouched by the hub — it's distributed "+
					"to other peers (via hub_peers()) so they can encrypt to you; the hub "+
					"itself never uses it cryptographically. This is DIFFERENT from "+
					"reconnectSecret: agePublicKey is visible to every other peer in the "+
					"session, so it must never be used to grant identity/peerId reuse — "+
					"anyone who saw it could then impersonate you. Use reconnectSecret for that")),
			mcp.WithString("reconnectSecret", mcp.Description(
				"Optional, never distributed to anyone (only you and the server ever see "+
					"it) — any string you choose to remember, e.g. a UUID. Presenting the "+
					"exact same reconnectSecret on a later hub_connect to this same "+
					"still-alive session reassigns your previous peerId instead of a new "+
					"one, so you're recognized as the same participant across a dropped "+
					"connection, as long as that previous connection isn't still active")),
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
			mcp.WithDescription("List everyone else currently in the hub session, including each "+
				"peer's peerId and — if they supplied one on connect — their display name and age "+
				"public key (e.g. for encrypting a message to them before sending)")),
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
	name := req.GetString("name", "")
	agePublicKey := req.GetString("agePublicKey", "")
	if agePublicKey != "" && !agekey.Valid(agePublicKey) {
		return mcp.NewToolResultError("agePublicKey is not a validly formatted age public key"), nil
	}
	reconnectSecret := req.GetString("reconnectSecret", "")
	conn, err := hubconn.Dial(host, sessionID, hubconn.DialOptions{
		Name: name, AgePublicKey: agePublicKey, ReconnectSecret: reconnectSecret,
	})
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

	waitBlock := fmt.Sprintf(
		"IMPORTANT: connecting alone does not deliver anything — you will never see a "+
			"message unless one of these is actually running in the background. Two modes, "+
			"pick based on what your harness offers:\n"+
			"  - once (default): exits after the next event, so it must be run again every "+
			"time it completes — run it now:\n"+
			"      %s\n"+
			"  - follow: stays running and prints each event as it arrives — prefer this, "+
			"backgrounded via a tool that can stream a long-running process's output as it "+
			"happens (e.g. a \"Monitor\"-style tool), if your harness has one; otherwise use "+
			"once mode above:\n"+
			"      %s",
		w.WaitCommand(), w.WaitFollowCommand(),
	)

	invite := fmt.Sprintf(
		"Propose this to the user so they can copy it to whoever else should join:\n"+
			"  Connect to the hub at %s with sessionId %s, then wait for messages.",
		host, sessionID,
	)

	var rosterNote string
	if n := conn.ExpectedPeerCount(); n == 0 {
		rosterNote = "No other peers are in this session yet."
	} else {
		rosterNote = fmt.Sprintf(
			"%d other peer(s) already in this session — you'll get a \"roster complete\" "+
				"notification (via wait/hub_receive) once you've caught up on who they are; "+
				"call hub_peers() after that to see the list.", n)
	}

	versionNote := ""
	if sv := conn.ServerVersion(); sv > wire.ProtocolVersion {
		versionNote = fmt.Sprintf(
			"\nNOTE: this mcp-hub-client speaks protocol v%d, but the server recommends v%d — "+
				"tell the user to update mcp-hub-client (see "+
				"https://github.com/secforge/mcp-hub/releases).", wire.ProtocolVersion, sv)
	} else if sv := conn.ServerVersion(); sv < wire.ProtocolVersion {
		versionNote = fmt.Sprintf(
			"\nNOTE: this mcp-hub-client speaks protocol v%d, ahead of the server's v%d — "+
				"the server may need updating.", wire.ProtocolVersion, sv)
	}

	identityNote := ""
	if name != "" || agePublicKey != "" {
		var parts []string
		if conn.Name() != "" {
			if conn.Name() != name {
				parts = append(parts, fmt.Sprintf("display name %q (sanitized from what was given)", conn.Name()))
			} else {
				parts = append(parts, fmt.Sprintf("display name %q", conn.Name()))
			}
		}
		if conn.AgePublicKey() != "" {
			parts = append(parts, "age public key "+conn.AgePublicKey())
		}
		identityNote = "\nOther peers (via hub_peers()) can see your " + strings.Join(parts, " and ") + "."
	}
	if reconnectSecret != "" {
		identityNote += "\nThis reconnectSecret is remembered (not shared with anyone) — " +
			"present it again on a future hub_connect to this same still-alive session to " +
			"be reassigned this same peerId."
	}

	if generated {
		return mcp.NewToolResultText(fmt.Sprintf(
			"Connected as peer %s in a new session: %s\n"+
				"Share this sessionId with whoever else should join — they need it to connect.\n"+
				"%s\n%s\n"+
				"%s%s%s",
			conn.PeerID(), sessionID, invite, rosterNote, waitBlock, versionNote, identityNote,
		)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf(
		"Connected as peer %s.\n%s\n%s\n%s%s%s",
		conn.PeerID(), invite, rosterNote, waitBlock, versionNote, identityNote,
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
	catchingUp := ""
	if !h.conn.RosterComplete() {
		catchingUp = " (still catching up on the initial roster — this list may be incomplete)"
	}
	peers := h.conn.Peers()
	if len(peers) == 0 {
		return mcp.NewToolResultText("no other peers currently in the session" + catchingUp), nil
	}
	lines := make([]string, 0, len(peers))
	for _, p := range peers {
		line := p.ID
		if p.Name != "" {
			line += fmt.Sprintf(" (%q)", p.Name)
		}
		if p.AgePublicKey != "" {
			line += " agePublicKey=" + p.AgePublicKey
		}
		lines = append(lines, line)
	}
	return mcp.NewToolResultText("Current peers:\n" + strings.Join(lines, "\n") + catchingUp), nil
}
