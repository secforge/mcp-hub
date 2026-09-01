package mcptools

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

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
	// mu guards conn/waiter. Needed because, unlike every other mutation of
	// these fields (which happens synchronously inside a tool call),
	// conn.OnActivity's disconnect callback (see handleConnect) can clear
	// them from the Conn's own background read goroutine at any moment —
	// automatic detection of a dead connection, not just the reactive
	// per-call check a tool handler does.
	mu     sync.Mutex
	conn   *hubconn.Conn
	waiter *waiter.Waiter

	// waitMu, waitCancel, and waitGen let a new handleWait call supersede
	// one already in flight, mirroring waiter.Waiter's single-registered-
	// waiter design for the CLI wait socket — see handleWait.
	waitMu     sync.Mutex
	waitCancel context.CancelFunc
	waitGen    uint64
}

func NewHub() *Hub {
	return &Hub{}
}

// activeConn returns the current connection and its wait socket, or (nil,
// nil) if not connected.
func (h *Hub) activeConn() (*hubconn.Conn, *waiter.Waiter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.conn, h.waiter
}

// setActiveConn records a newly established connection as the active one.
func (h *Hub) setActiveConn(conn *hubconn.Conn, w *waiter.Waiter) {
	h.mu.Lock()
	h.conn, h.waiter = conn, w
	h.mu.Unlock()
}

// clearActiveConn unconditionally forgets whatever connection is currently
// active and returns it, for an explicit hub_disconnect — which should
// tear down whatever is active right now, regardless of which Conn
// instance a caller happens to be holding a reference to.
func (h *Hub) clearActiveConn() (*hubconn.Conn, *waiter.Waiter) {
	h.mu.Lock()
	conn, w := h.conn, h.waiter
	h.conn, h.waiter = nil, nil
	h.mu.Unlock()
	return conn, w
}

// teardownIfCurrent tears down the active connection, but only if it's
// still exactly the Conn passed in. A caller that noticed conn had died —
// including conn's own background read goroutine, via conn.OnActivity — is
// racing against a fresh hub_connect (or an explicit hub_disconnect) that
// may have already replaced or cleared it; without this check, a stale
// notification about a connection nobody cares about anymore could wrongly
// tear down whatever legitimately replaced it.
func (h *Hub) teardownIfCurrent(conn *hubconn.Conn) {
	h.mu.Lock()
	if h.conn != conn {
		h.mu.Unlock()
		return
	}
	w := h.waiter
	h.conn, h.waiter = nil, nil
	h.mu.Unlock()
	if w != nil {
		w.Close()
	}
}

// disconnectedText renders the bare "hub disconnected" outcome, appending
// conn.DisconnectNote() when the connection ended for a reason more
// specific than an ordinary drop (e.g. a bridge server's close code
// signaling a dead credential) — empty, and so a no-op here, for every
// mcp-hub-server disconnect. Also names the last message cursor this
// connection actually delivered, if any, so a reconnecting client knows
// exactly what to pass hub_history(after: ...) — the server can't know
// which events a client actually processed, only what it wrote to the
// socket, so this bookkeeping has to happen here. before is deliberately
// never suggested here: it only reaches older messages, never the ones
// that arrived after this cursor while disconnected.
func disconnectedText(conn *hubconn.Conn) string {
	text := "hub disconnected" + conn.DisconnectNote()
	if cursor := conn.LastSeenCursor(); cursor != "" {
		if conn.HistoryAfterSupported() {
			text += fmt.Sprintf("\nLast message cursor you saw on this connection: %q. On your "+
				"next hub_connect, call hub_history(after: %q) to catch up on anything that arrived "+
				"while disconnected — do this by default, don't wait to notice something is "+
				"missing.", cursor, cursor)
		} else {
			text += fmt.Sprintf("\nLast message cursor you saw on this connection: %q. This "+
				"server does not support hub_history's after (forward paging), so this gap cannot "+
				"be precisely filled — on your next hub_connect, call hub_history() with no "+
				"before/after to see the most recent messages instead; this may not cover "+
				"everything that arrived while disconnected if the gap was large.", cursor)
		}
	}
	return text
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
			mcp.WithString("reconnectSecret", mcp.Required(), mcp.Description(
				"Required — always pass one, even on a brand new session. Never distributed "+
					"to anyone (only you and the server ever see it) — any string you choose "+
					"to remember, e.g. a UUID; generate one yourself if the user hasn't given "+
					"you one to reuse. Presenting the exact same reconnectSecret on a later "+
					"hub_connect reassigns your previous peerId instead of a new one, so "+
					"you're recognized as the same participant across a dropped connection, a "+
					"server restart, or even the whole session having emptied out and later "+
					"been reconstituted — as long as that previous connection isn't still "+
					"active (which would get you a fresh peerId instead, to avoid a "+
					"collision). This is required only by this MCP tool's contract, as a "+
					"guardrail so you never end up unable to resume your identity — the hub "+
					"server itself has no such requirement and happily accepts connections "+
					"without one")),
		),
		h.handleConnect,
	)
	s.AddTool(
		mcp.NewTool("teams_relay_connect",
			mcp.WithDescription("Connect to a chat-relay bridge session via a link a user was given "+
				"(e.g. one issued for a specific Microsoft Teams conversation) — the "+
				"chat-relay-specific counterpart to hub_connect, for a server that bridges into a "+
				"real chat platform rather than being an mcp-hub-server. Once connected, hub_send/"+
				"hub_receive/hub_wait/hub_peers all work the same way as for a normal hub_connect "+
				"session"),
			mcp.WithString("link", mcp.Required(), mcp.Description(
				"The exact opaque link string the user was given, unmodified — do not parse, "+
					"reformat, or strip anything from it yourself. Keep the exact string around: "+
					"reconnecting after a drop presents this same link again, alongside the same "+
					"reconnectSecret")),
			mcp.WithString("name", mcp.Description(
				"Optional untrusted display name. Depending on the bridge, this may be for its own "+
					"logs/audit only and never made visible to anyone on the other side of the "+
					"bridge (e.g. a chat-relay conversation) — don't assume it functions as an "+
					"in-conversation display name unless told otherwise")),
			mcp.WithString("reconnectSecret", mcp.Required(), mcp.Description(
				"Required — any string you choose to remember, e.g. a UUID; generate one yourself "+
					"if the user hasn't given you one to reuse. Unlike hub_connect's version, this "+
					"does not identify a peer — a bridge link already fixes which conversation you "+
					"get. Instead it authorizes resuming after a dropped connection: many such links "+
					"are single-use, so presenting the exact same link again after it's already been "+
					"used only works if paired with the same reconnectSecret from the original "+
					"connect (within whatever window the bridge grants — commonly on the order of "+
					"hours, not indefinite). Omit this and you may not be able to resume at all after "+
					"a drop")),
		),
		h.handleTeamsRelayConnect,
	)
	s.AddTool(
		mcp.NewTool("hub_send",
			mcp.WithDescription("Send a text message to the current hub session. On a bridge "+
				"session (e.g. via teams_relay_connect), this call itself waits briefly for the "+
				"real outcome — the send actually being accepted, or refused — and reports it "+
				"directly rather than a bare confirmation that doesn't mean the send succeeded; if "+
				"nothing arrives in time it falls back to a plain confirmation, with the actual "+
				"outcome then arriving later via wait/hub_receive/hub_wait instead. On a plain "+
				"hub_connect session this always returns immediately, since mcp-hub-server has no "+
				"equivalent asynchronous confirmation to wait for"),
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
		mcp.NewTool("hub_wait",
			mcp.WithDescription("Block until the next hub event arrives (or the hub disconnects), "+
				"then return it — the direct MCP-tool alternative to running the wait CLI binary "+
				"as a background/foreground process. Best for a harness that cannot background a "+
				"process at all (e.g. Codex): this call is bounded by your own MCP client's tool-"+
				"call timeout instead of a much shorter shell-exec timeout, so it needs far fewer "+
				"round trips. If the call is cancelled or times out with nothing having arrived "+
				"yet, that's normal, not an error — just call hub_wait() again. Calling hub_wait() "+
				"again while a previous call is still outstanding immediately supersedes it (the "+
				"old call returns right away); only ever have one in flight at a time")),
		h.handleWait,
	)
	s.AddTool(
		mcp.NewTool("hub_peers",
			mcp.WithDescription("List everyone else currently in the hub session, including each "+
				"peer's peerId and — if they supplied one on connect — their display name and age "+
				"public key (e.g. for encrypting a message to them before sending)")),
		h.handlePeers,
	)
	s.AddTool(
		mcp.NewTool("hub_history",
			mcp.WithDescription("Request messages that predate this connection — not meaningful for "+
				"an ordinary hub_connect session (peers only ever see events from when they joined "+
				"forward), but for a bridge session (e.g. via teams_relay_connect) backed by a "+
				"channel with real retained history. Errors if not connected. The requested messages "+
				"arrive asynchronously via wait/hub_receive/hub_wait like any other event, each "+
				"labeled distinctly from live messages, followed by a completion notice once the "+
				"page has been fully delivered — this call itself only confirms the request was "+
				"sent, it does not return the messages directly"),
			mcp.WithString("before", mcp.Description(
				"Opaque cursor from an earlier message, exclusive — the page ends strictly before "+
					"it. Omit (with after also omitted) for the most recent messages; page further "+
					"back by passing the oldest cursor seen so far. This only reaches OLDER messages "+
					"— never use it to try to catch up on messages that arrived after a disconnect, "+
					"use after for that instead")),
			mcp.WithString("after", mcp.Description(
				"Opaque cursor from an earlier message, exclusive — the page starts strictly after "+
					"it, i.e. forward paging. This is what to use after a reconnect to fetch exactly "+
					"what arrived while disconnected (pass the last cursor you saw before "+
					"disconnecting). Only works if the server supports it (errors otherwise, telling "+
					"you so) — a plain, non-bridge hub_connect session never does. Mutually exclusive "+
					"with before")),
			mcp.WithNumber("limit", mcp.Description(
				"Maximum messages to return; the server may cap this lower than requested")),
		),
		h.handleHistory,
	)
	s.AddTool(
		mcp.NewTool("hub_react",
			mcp.WithDescription("Add or remove a reaction on an earlier message — not meaningful for "+
				"an ordinary hub_connect session, but for a bridge session (e.g. via "+
				"teams_relay_connect) backed by a platform with write access. Errors if not "+
				"connected. On a bridge session this call itself waits briefly for the real "+
				"outcome (acknowledged, or refused) and reports it directly; if nothing arrives in "+
				"time it falls back to a plain confirmation that the request was sent, with the "+
				"actual outcome then arriving later via wait/hub_receive/hub_wait instead"),
			mcp.WithString("externalId", mcp.Required(), mcp.Description(
				"The target message's externalId, from an earlier msg or sendAck event")),
			mcp.WithString("reaction", mcp.Required(), mcp.Description(
				"The reaction to add or remove — an emoji or the platform's name for it (e.g. "+
					"\"👍\" or \"Like\"). Not a fixed set: pass whatever the platform actually "+
					"supports; an unsupported value comes back as an error, not a client-side "+
					"rejection")),
			mcp.WithString("action", mcp.Required(), mcp.Description(`Either "add" or "remove"`)),
		),
		h.handleReact,
	)
	s.AddTool(
		mcp.NewTool("hub_edit",
			mcp.WithDescription("Change an earlier message's content — not meaningful for an ordinary "+
				"hub_connect session, but for a bridge session (e.g. via teams_relay_connect) backed "+
				"by a platform with write access. Typically only possible on a message this "+
				"connection itself sent — platform rules usually restrict editing to your own "+
				"messages, and that's enforced by the platform, not pre-judged here. Errors if not "+
				"connected. On a bridge session this call itself waits briefly for the real outcome "+
				"and reports it directly, falling back to an async confirmation (see hub_react) if "+
				"nothing arrives in time"),
			mcp.WithString("externalId", mcp.Required(), mcp.Description(
				"The target message's externalId, from an earlier msg or sendAck event")),
			mcp.WithString("text", mcp.Required(), mcp.Description("The new message content")),
		),
		h.handleEdit,
	)
	s.AddTool(
		mcp.NewTool("hub_delete",
			mcp.WithDescription("Remove an earlier message — not meaningful for an ordinary "+
				"hub_connect session, but for a bridge session (e.g. via teams_relay_connect) backed "+
				"by a platform with write access. Typically only possible on a message this "+
				"connection itself sent, same as hub_edit; enforced by the platform, not pre-judged "+
				"here. This is a genuine deletion, not an edit to empty text — the platform renders "+
				"a tombstone rather than a blank message, and other clients learn about it via a "+
				"distinct messageDeleted event, not an edited one. Errors if not connected. On a "+
				"bridge session this call itself waits briefly for the real outcome and reports it "+
				"directly, falling back to an async confirmation (see hub_react) if nothing arrives "+
				"in time"),
			mcp.WithString("externalId", mcp.Required(), mcp.Description(
				"The target message's externalId, from an earlier msg or sendAck event")),
		),
		h.handleDelete,
	)
}

// clientName reads the connecting MCP client's self-reported name from the
// standard MCP `initialize` handshake (clientInfo.name) — empty if the
// underlying transport/session doesn't expose it. This lets hub_connect's
// result tailor its background-delivery guidance to what the connecting
// harness actually supports, rather than assuming every harness behaves
// like Claude Code.
func clientName(ctx context.Context) string {
	cs := server.ClientSessionFromContext(ctx)
	if cs == nil {
		return ""
	}
	withInfo, ok := cs.(server.SessionWithClientInfo)
	if !ok {
		return ""
	}
	return withInfo.GetClientInfo().Name
}

// looksLikeCodex is a loose, case-insensitive substring match rather than
// an exact one, since OpenAI's own docs show clientInfo.name varying by
// integration (e.g. "codex_vscode") and we'd rather over- than
// under-detect here — the cost of a false positive (an accurate but
// unnecessary warning) is much lower than a false negative (Codex silently
// told to use --follow, which it cannot act on).
func looksLikeCodex(name string) bool {
	return strings.Contains(strings.ToLower(name), "codex")
}

// buildWaitBlock builds the guidance for how to actually receive events
// after connecting — shared between hub_connect and teams_relay_connect,
// since the delivery mechanism (the CLI wait binary, or hub_wait) is
// identical either way. Codex gets its own, self-contained block from the
// start — never the generic "background one of these two" framing
// followed by a contradicting correction. Codex cannot background a
// command at all (confirmed directly from a live Codex session), so
// presenting that framing first and only then saying "ignore that" is
// actively confusing, not just redundant. reconnectInstruction fills in
// the Codex checklist's step 2 — what to actually present on a reconnect,
// which differs between hub_connect (sessionId+reconnectSecret) and
// teams_relay_connect (the same link+reconnectSecret used the first time).
func buildWaitBlock(ctx context.Context, w *waiter.Waiter, reconnectInstruction string) string {
	if looksLikeCodex(clientName(ctx)) {
		return "Persistent monitoring is active for this session.\n\n" +
			"After connecting, immediately call the foreground hub_wait tool.\n\n" +
			"When hub_wait returns for any reason—event, timeout, cancellation, or " +
			"disconnect—do the following before ending the current turn:\n\n" +
			"1. Process and acknowledge every returned event.\n" +
			"2. If disconnected, " + reconnectInstruction + ".\n" +
			"3. Immediately call hub_wait again.\n" +
			"4. Never return a final response merely because one waiter call ended.\n" +
			"5. If the user sends a message while monitoring, answer it, then immediately " +
			"call hub_wait again.\n" +
			"6. Continue this cycle indefinitely while monitoring is requested.\n" +
			"7. Only stop after the user explicitly says to stop monitoring or the " +
			"platform forcibly ends the turn.\n" +
			"8. If the platform forcibly ends the turn, report that monitoring was " +
			"interrupted and resume with hub_wait at the start of the next turn.\n\n" +
			"A timeout with no event is normal and must result in another hub_wait call."
	}
	return fmt.Sprintf(
		"IMPORTANT: connecting alone does not deliver anything — you will never see a "+
			"message unless one of these is actually running in the background. Two "+
			"modes, pick based on what your harness offers:\n"+
			"  - once (default): exits after the next event, so it must be run again "+
			"every time it completes — run it now:\n"+
			"      %s\n"+
			"  - follow: stays running and prints each event as it arrives — prefer "+
			"this, backgrounded via a tool that can stream a long-running process's "+
			"output as it happens (e.g. a \"Monitor\"-style tool), if your harness has "+
			"one; otherwise use once mode above:\n"+
			"      %s\n"+
			"When you connect to mcp-hub, use wait --follow with your Monitor/"+
			"background-streaming tool directly — don't wrap it in a manual loop or a "+
			"tee/grep pipeline.",
		w.WaitCommand(), w.WaitFollowCommand(),
	)
}

func (h *Hub) handleConnect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if prev, _ := h.activeConn(); prev != nil {
		if !prev.Connected() {
			// The previous connection died on its own (server restart,
			// network drop, kill -9) without a clean hub_disconnect() ever
			// running to clear it — normally this has already been torn
			// down automatically (see the OnActivity wiring below) well
			// before a caller gets here, but don't make the caller issue
			// hub_disconnect itself in the rare case it hasn't yet.
			h.teardownIfCurrent(prev)
		} else {
			return mcp.NewToolResultError("already connected; call hub_disconnect first"), nil
		}
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
	conn.OnActivity(func() {
		w.Poke()
		if !conn.Connected() {
			// The read loop that just invoked us is the one that detected
			// this — a silent drop, a server-side close, anything short of
			// our own hub_disconnect(). Tear down proactively instead of
			// leaving a dead-but-not-yet-noticed connection sitting around
			// (and its wait socket still listening) until whatever tool
			// call happens to come next.
			h.teardownIfCurrent(conn)
		}
	})
	h.setActiveConn(conn, w)

	waitBlock := buildWaitBlock(ctx, w, "reconnect with the same sessionId and reconnectSecret")
	if reconnectSecret == "" && looksLikeCodex(clientName(ctx)) {
		waitBlock += "\n\nNote: no reconnectSecret was given on this connect, so step 2 " +
			"above has nothing to reconnect with yet — a reconnect after a disconnect " +
			"would get a new peerId instead of resuming this one. If continuity across a " +
			"disconnect matters, disconnect and reconnect once more now, this time " +
			"passing a reconnectSecret you'll remember and reuse for every future " +
			"reconnect in this monitoring cycle."
	}

	invite := fmt.Sprintf(
		"YOU MUST tell the user the exact sentence below before doing anything else — this "+
			"session is useless alone, and the user is the only one who can forward it to "+
			"whoever (human or AI) should join it. Do not paraphrase, summarize, or omit it; "+
			"quote it verbatim so it can be copy-pasted straight into another AI's prompt:\n"+
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
		identityNote += "\nThis reconnectSecret is remembered (not shared with anyone, and " +
			"it survives a server restart or the session emptying out) — present it again " +
			"on a future hub_connect to be reassigned this same peerId."
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

// teamsRelaySocketSessionID is the fixed sessionID half of the wait
// socket's filename hash for a teams_relay_connect connection — there's no
// real sessionId in this flow, only a link, so a constant here (combined
// with the connection's own peerId, which is what actually varies) is
// enough to keep the socket path unique per connection the same way a real
// sessionId does for hub_connect. See waiter.Listen/socketPath.
const teamsRelaySocketSessionID = "teams-relay"

func (h *Hub) handleTeamsRelayConnect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if prev, _ := h.activeConn(); prev != nil {
		if !prev.Connected() {
			h.teardownIfCurrent(prev)
		} else {
			return mcp.NewToolResultError("already connected; call hub_disconnect first"), nil
		}
	}
	link, err := req.RequireString("link")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	name := req.GetString("name", "")
	reconnectSecret := req.GetString("reconnectSecret", "")
	conn, err := hubconn.DialRelay(link, hubconn.RelayDialOptions{
		Name: name, ReconnectSecret: reconnectSecret,
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("connect failed: %v", err)), nil
	}
	w, err := waiter.Listen(teamsRelaySocketSessionID, conn.PeerID(), conn)
	if err != nil {
		conn.Close()
		return mcp.NewToolResultError(fmt.Sprintf("could not start wait socket: %v", err)), nil
	}
	conn.OnActivity(func() {
		w.Poke()
		if !conn.Connected() {
			h.teardownIfCurrent(conn)
		}
	})
	h.setActiveConn(conn, w)

	waitBlock := buildWaitBlock(ctx, w, "reconnect via teams_relay_connect with the same link and reconnectSecret")

	var rosterNote string
	if n := conn.ExpectedPeerCount(); n == 0 {
		rosterNote = "No other participants in this conversation yet."
	} else {
		rosterNote = fmt.Sprintf(
			"%d other participant(s) already in this conversation — you'll get a \"roster "+
				"complete\" notification (via wait/hub_receive) once you've caught up on who "+
				"they are; call hub_peers() after that to see the list.", n)
	}

	versionNote := ""
	if sv := conn.ServerVersion(); sv > wire.ProtocolVersion {
		versionNote = fmt.Sprintf(
			"\nNOTE: this mcp-hub-client speaks protocol v%d, but the bridge server recommends "+
				"v%d — tell the user to update mcp-hub-client (see "+
				"https://github.com/secforge/mcp-hub/releases).", wire.ProtocolVersion, sv)
	} else if sv := conn.ServerVersion(); sv < wire.ProtocolVersion {
		versionNote = fmt.Sprintf(
			"\nNOTE: this mcp-hub-client speaks protocol v%d, ahead of the bridge server's v%d — "+
				"the bridge may need updating.", wire.ProtocolVersion, sv)
	}

	notes := "\nThis is a chat-relay bridge session, not a normal mcp-hub session — some things " +
		"behave differently: a directed hub_send (`to`) has no meaning here and will be refused " +
		"with an error rather than delivered; sends may be routinely refused for policy reasons " +
		"(e.g. a conversation with participants outside the bridge's home organization) — that's " +
		"expected, not a bug, and won't succeed on retry; peerJoined/peerLeft reflect real " +
		"conversation membership changes, not other clients connecting; and this link may be " +
		"single-use — hold onto the exact link and reconnectSecret you used here, since " +
		"resuming after a drop means presenting both again via teams_relay_connect, not just " +
		"the link alone."

	conversationNote := ""
	if kind := conn.ConversationKind(); kind != "" {
		conversationNote += "\nConversation kind: " + kind
		if t := conn.Topic(); t != nil && *t != "" {
			conversationNote += fmt.Sprintf(" (%q)", *t)
		}
	}
	if conn.CanSend() {
		conversationNote += "\nSending is currently permitted in this conversation — but this is " +
			"only a snapshot from connect time, not a guarantee: the policy is re-checked on every " +
			"actual send, so a later hub_send can still come back refused even though this said yes."
	} else {
		conversationNote += "\nSending is NOT currently permitted in this conversation (e.g. a " +
			"policy restriction such as an external participant) — a hub_send will be refused. " +
			"This is expected and routine, not a bug; don't compose a message assuming it can be " +
			"sent without checking first, and don't retry a refused send."
	}
	if cursor := conn.LatestCursor(); cursor != nil {
		conversationNote += fmt.Sprintf("\nLatest message cursor: %q. This cursor is opaque; "+
			"don't parse or compare it as a timestamp.", *cursor)
		if conn.HistoryAfterSupported() {
			conversationNote += " REQUIRED: if this is a reconnect to a conversation you were " +
				"connected to before, check your own prior context for the last message cursor you " +
				"saw (a disconnect from this hub reports it explicitly for exactly this reason) and " +
				"call hub_history(after: <that cursor>) before doing anything else — do this " +
				"unconditionally, don't wait until something looks missing. hub_history's before " +
				"only reaches OLDER messages and cannot fill this gap; after is what fetches what " +
				"arrived while you were disconnected."
		} else {
			conversationNote += " This server does not support forward paging (hub_history's " +
				"after), so a reconnect gap cannot be precisely filled — before only reaches OLDER " +
				"messages, never newer ones. If this is a reconnect, call hub_history() with no " +
				"before/after to see the most recent messages; this may not include everything that " +
				"arrived while you were disconnected if the gap was large."
		}
	} else {
		conversationNote += "\nThis conversation has no messages yet."
	}
	if max := conn.HistoryLimitMax(); max > 0 {
		conversationNote += fmt.Sprintf("\nhub_history() page size is capped at %d regardless of "+
			"what's requested.", max)
	}
	conversationNote += "\nUse hub_history() to fetch messages from before this connection " +
		"started, if you need them."

	return mcp.NewToolResultText(fmt.Sprintf(
		"Connected as peer %s.\n%s\n%s%s%s%s",
		conn.PeerID(), rosterNote, waitBlock, versionNote, notes, conversationNote,
	)), nil
}

func (h *Hub) handleSend(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, _ := h.activeConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	if !conn.Connected() {
		h.teardownIfCurrent(conn)
		return mcp.NewToolResultText(disconnectedText(conn)), nil
	}
	text, err := req.RequireString("text")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	to := req.GetString("to", "")
	if to != "" && !wire.IsValidID(to) {
		return mcp.NewToolResultError("to must be a UUID"), nil
	}
	ev, ok, err := conn.SendAwaitingAck(text, to)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("send failed: %v", err)), nil
	}
	if ok {
		// A bridge connection's own outcome (sendAck on success, an error
		// event on refusal) arrived in time — report it directly rather
		// than a bare "sent" that doesn't actually confirm anything on a
		// bridge session. See hubconn.Conn.SendAwaitingAck.
		return mcp.NewToolResultText(hubconn.FormatEvent(ev)), nil
	}
	if !conn.IsBridge() {
		if to == "" {
			return mcp.NewToolResultText("sent"), nil
		}
		return mcp.NewToolResultText("sent (private)"), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf(
		"sent — no acknowledgement within %v; check wait/hub_receive/hub_wait for the actual "+
			"outcome (a sendAck or an error) rather than assuming this succeeded", hubconn.AckWaitTimeout,
	)), nil
}

func (h *Hub) handleDisconnect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, w := h.clearActiveConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	if w != nil {
		w.Close()
	}
	conn.Close()
	return mcp.NewToolResultText("disconnected"), nil
}

func (h *Hub) handleReceive(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, _ := h.activeConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	formatted, connected := conn.Drain()
	if !connected {
		h.teardownIfCurrent(conn)
		// Still surface anything that arrived right before the disconnect
		// (e.g. a final message buffered just ahead of the read loop
		// erroring out) instead of silently discarding it in favor of a
		// bare "hub disconnected" — the caller can always tell the two
		// apart since disconnected-with-content still ends with the note.
		if formatted != "" {
			return mcp.NewToolResultText(formatted + "\n\n" + disconnectedText(conn)), nil
		}
		return mcp.NewToolResultText(disconnectedText(conn)), nil
	}
	if formatted == "" {
		return mcp.NewToolResultText("no messages"), nil
	}
	return mcp.NewToolResultText(formatted), nil
}

// waitPollInterval is how often handleWait re-checks the buffer while
// blocked. Deliberately not event-driven (unlike the CLI wait/waiter path,
// which registers for a Poke callback): hubconn.Conn.OnActivity holds only
// a single callback, already claimed by the waiter socket for the CLI
// wait command, and polling this rarely is cheap enough not to warrant
// extending that to a multi-listener design just for this.
const waitPollInterval = 100 * time.Millisecond

// waitAgainReminder is prepended — not appended — to a successful hub_wait
// delivery. It has to come first, not last: hub_wait is a single blocking
// call, so there's no follow-up chunk to fall back on, and if whatever's
// reading the result gets cut off partway through (a client-side read
// timeout, a truncated/streamed display of a large result) a trailing
// reminder is exactly the part most likely to never be seen. Leading with
// it means it survives being caught even by a truncated read. Only used on
// the single-shot delivery path, not the "hub disconnected" ones (nothing
// to restart there) and never for CLI `wait --follow`, which keeps
// delivering over the same connection and was never the thing this guards
// against.
const waitAgainReminder = "REMINDER: after processing the message(s) below, call hub_wait again " +
	"immediately to keep monitoring this session — do not end your turn just because this one " +
	"call returned.\n\n"

// handleWait blocks until an event is buffered or the hub disconnects,
// then returns it — the direct MCP-tool equivalent of running the wait
// CLI binary, for a harness that can't background/persist a process at
// all. Returns promptly if the caller's context is cancelled (e.g. the
// client's own tool-call timeout elapsed and it sent notifications/
// cancelled — mark3labs/mcp-go wires that into ctx per request), rather
// than leaking a goroutine blocked forever; note this depends on the
// client actually sending that notification, which the MCP spec makes
// optional, not guaranteed.
//
// A new call always supersedes one already in flight — mirroring
// waiter.Waiter's single-registered-waiter design for the CLI wait
// socket, for the same reason: without this, two concurrent calls would
// independently poll the same buffer and just race for whichever event
// arrives first via Drain (destructive), leaving the loser blocked
// waiting for a *different* event that might never come, with no
// indication anything was "stolen." This matters most for exactly the
// case handleWait's own doc above already flags: a client that silently
// abandons a call on its own timeout (no cancellation sent) and then
// retries, leaving the old call still running server-side.
func (h *Hub) handleWait(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, _ := h.activeConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}

	innerCtx, cancel := context.WithCancel(ctx)
	h.waitMu.Lock()
	if h.waitCancel != nil {
		h.waitCancel() // supersede whatever hub_wait call was already in flight
	}
	h.waitGen++
	myGen := h.waitGen
	h.waitCancel = cancel
	h.waitMu.Unlock()
	defer func() {
		h.waitMu.Lock()
		if h.waitGen == myGen {
			h.waitCancel = nil
		}
		h.waitMu.Unlock()
		cancel()
	}()

	ticker := time.NewTicker(waitPollInterval)
	defer ticker.Stop()
	for {
		if hasEvents, connected := conn.Peek(); hasEvents || !connected {
			formatted, connected := conn.Drain()
			if !connected {
				h.teardownIfCurrent(conn)
				if formatted == "" {
					return mcp.NewToolResultText(disconnectedText(conn)), nil
				}
				return mcp.NewToolResultText(formatted + "\n\n" + disconnectedText(conn)), nil
			}
			return mcp.NewToolResultText(waitAgainReminder + formatted), nil
		}
		select {
		case <-innerCtx.Done():
			if err := ctx.Err(); err != nil {
				return nil, err // the caller's own context was cancelled/timed out
			}
			// innerCtx was cancelled independently of ctx: a newer hub_wait
			// call superseded this one.
			return mcp.NewToolResultText("superseded by a newer hub_wait call"), nil
		case <-ticker.C:
		}
	}
}

func (h *Hub) handlePeers(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, _ := h.activeConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	if !conn.Connected() {
		h.teardownIfCurrent(conn)
		return mcp.NewToolResultText(disconnectedText(conn)), nil
	}
	catchingUp := ""
	if !conn.RosterComplete() {
		catchingUp = " (still catching up on the initial roster — this list may be incomplete)"
	}
	peers := conn.Peers()
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

func (h *Hub) handleHistory(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, _ := h.activeConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	if !conn.Connected() {
		h.teardownIfCurrent(conn)
		return mcp.NewToolResultText(disconnectedText(conn)), nil
	}
	before := req.GetString("before", "")
	after := req.GetString("after", "")
	limit := req.GetInt("limit", 0)
	if before != "" && after != "" {
		return mcp.NewToolResultError("specify at most one of before/after, not both"), nil
	}
	if after != "" {
		if !conn.HistoryAfterSupported() {
			return mcp.NewToolResultError(
				"this server does not support after (forward paging) — it only supports before, " +
					"which pages backward and cannot be used to catch up on messages that arrived " +
					"after a given point; call hub_history() with no before/after for the most " +
					"recent messages instead",
			), nil
		}
		if err := conn.RequestHistoryAfter(after, limit); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("history request failed: %v", err)), nil
		}
	} else if err := conn.RequestHistory(before, limit); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("history request failed: %v", err)), nil
	}
	return mcp.NewToolResultText(
		"history request sent — the messages (each marked distinctly from live traffic) and a " +
			"completion notice will arrive via wait/hub_receive/hub_wait, not from this call",
	), nil
}

func (h *Hub) handleReact(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, _ := h.activeConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	if !conn.Connected() {
		h.teardownIfCurrent(conn)
		return mcp.NewToolResultText(disconnectedText(conn)), nil
	}
	externalID, err := req.RequireString("externalId")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	reaction, err := req.RequireString("reaction")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	action, err := req.RequireString("action")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if action != "add" && action != "remove" {
		return mcp.NewToolResultError(`action must be "add" or "remove"`), nil
	}
	ev, ok, err := conn.ReactAwaitingAck(externalID, reaction, action)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("reaction request failed: %v", err)), nil
	}
	if ok {
		return mcp.NewToolResultText(hubconn.FormatEvent(ev)), nil
	}
	if !conn.IsBridge() {
		return mcp.NewToolResultText(
			"reaction request sent — confirmation (or a refusal) will arrive via wait/hub_receive/" +
				"hub_wait, not from this call",
		), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf(
		"reaction request sent — no acknowledgement within %v; check wait/hub_receive/hub_wait "+
			"for the actual outcome rather than assuming this succeeded", hubconn.AckWaitTimeout,
	)), nil
}

func (h *Hub) handleEdit(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, _ := h.activeConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	if !conn.Connected() {
		h.teardownIfCurrent(conn)
		return mcp.NewToolResultText(disconnectedText(conn)), nil
	}
	externalID, err := req.RequireString("externalId")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	text, err := req.RequireString("text")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	ev, ok, err := conn.EditMessageAwaitingAck(externalID, text)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("edit request failed: %v", err)), nil
	}
	if ok {
		return mcp.NewToolResultText(hubconn.FormatEvent(ev)), nil
	}
	if !conn.IsBridge() {
		return mcp.NewToolResultText(
			"edit request sent — confirmation (or a refusal) will arrive via wait/hub_receive/" +
				"hub_wait, not from this call",
		), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf(
		"edit request sent — no acknowledgement within %v; check wait/hub_receive/hub_wait for "+
			"the actual outcome rather than assuming this succeeded", hubconn.AckWaitTimeout,
	)), nil
}

func (h *Hub) handleDelete(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, _ := h.activeConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	if !conn.Connected() {
		h.teardownIfCurrent(conn)
		return mcp.NewToolResultText(disconnectedText(conn)), nil
	}
	externalID, err := req.RequireString("externalId")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	ev, ok, err := conn.DeleteMessageAwaitingAck(externalID)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("delete request failed: %v", err)), nil
	}
	if ok {
		return mcp.NewToolResultText(hubconn.FormatEvent(ev)), nil
	}
	if !conn.IsBridge() {
		return mcp.NewToolResultText(
			"delete request sent — confirmation (or a refusal) will arrive via wait/hub_receive/" +
				"hub_wait, not from this call",
		), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf(
		"delete request sent — no acknowledgement within %v; check wait/hub_receive/hub_wait for "+
			"the actual outcome rather than assuming this succeeded", hubconn.AckWaitTimeout,
	)), nil
}
