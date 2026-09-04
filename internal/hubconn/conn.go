package hubconn

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/agekey"
	"github.com/secforge/mcp-hub/internal/wire"
)

// pongWait bounds how long we'll go without hearing anything at all — data
// or a ping — from wsserver (which pings every 30s, wsserver.pingPeriod)
// before treating the connection as dead. Without this, a silent drop (no
// TCP FIN/RST, e.g. a killed server process or a network partition) would
// never surface: ws.ReadMessage blocks with no deadline, so Peek/Drain
// would keep reporting connected forever and anything sent into that dead
// socket in the meantime would be silently lost.
//
// Deliberately well over 3x pingPeriod (not the ~1.3x a tighter margin
// would allow): a single delayed ping — a GC pause, scheduler contention,
// an intermediary briefly holding a frame — must not read as a dead
// connection. A real network partition or dead server is still caught
// comfortably within this window; false positives from ordinary jitter are
// not an acceptable trade for slightly faster detection of the rare true
// dead case. writeWait bounds writing our own pong reply.
var (
	pongWait  = 100 * time.Second
	writeWait = 10 * time.Second
	// ackIdleInterval is how long to wait, with nothing else about to be
	// sent anyway, before firing a standalone read receipt for a consumed
	// position that hasn't been reported yet — see Conn.ackLoop. Var so
	// tests can shorten it.
	ackIdleInterval = 60 * time.Second
)

type Event struct {
	Kind         string
	PeerID       string
	Text         string
	TS           string
	Private      bool
	Name         string
	AgePublicKey string
	// Historical marks a "msg" delivered in answer to RequestMessageAfterAwaiting
	// rather than live traffic — see wire.Msg.Historical.
	Historical bool
	// Code and Retryable carry a "error" event's machine-readable reason,
	// if the server set one — see wire.Error.
	Code      string
	Retryable bool
	// ExternalID carries a "sendAck"/"reactionAck"/"editAck" event's
	// payload (see wire.SendAck/wire.ReactionAck/wire.EditAck), or — on a
	// "msg" — the same id correlating it with the sendAck that preceded
	// it (see wire.Msg.ExternalID). ActionOK is shared across all three
	// ack kinds: "did the action this connection asked for succeed."
	ExternalID string
	ActionOK   bool
	// ReplyTo/ReplyPreview carry a "msg"/"messageEdited"'s reply
	// reference, if any — see wire.Msg.ReplyTo/ReplyPreview. Empty (not a
	// distinguishable "absent" vs. "empty string") when this message
	// isn't a reply.
	ReplyTo      string
	ReplyPreview string
	// Mentions/MentionedMe carry a "msg"/"messageEdited"'s @-mentions, if
	// any — see wire.Msg.Mentions/wire.Msg.MentionedMe.
	Mentions    []wire.Mention
	MentionedMe bool
	// IsOperator is true when this event's PeerID equals the session's
	// wire.Joined.SystemPeerID (see Conn.SystemPeerID) — set here, not
	// decoded from the frame itself, since a frame has no way to declare
	// its own authority. Only meaningful together with a non-empty
	// PeerID; false whenever a server has no SystemPeerID concept at all.
	IsOperator bool
	// Own marks a "msg" this exact connection sent — see wire.Msg.Own. Also
	// used on "reactionChanged" for the same purpose — see wire.ReactionChanged.Own.
	Own bool
	// Reaction, ReactionLabel, and ReactionAction carry a "reactionChanged"
	// event's payload — see wire.ReactionChanged. PeerID may be empty for
	// an unattributed removal (see wire.ReactionChanged.PeerID).
	// ExternalID identifies the reacted-to message.
	Reaction       string
	ReactionLabel  string
	ReactionAction string
	// Cursor carries a "msg"'s own opaque position (see wire.Msg.Cursor —
	// pass it back as a MessageAfter anchor to page further past it), or a
	// "messageDeleted"'s, for placing its tombstone (see
	// wire.MessageDeleted.Cursor).
	Cursor string
	// Attachments carries a "msg"/"messageEdited"'s attachments, if any —
	// see wire.Msg.Attachments/wire.MessageEdited.Attachments. Each entry
	// may be either the inline form (ContentBytes already base64, exactly
	// the form an MCP ImageContent block's Data field expects — no
	// re-encoding needed) or the reference form (wire.Attachment.
	// IsReference true) — fetch the latter with Conn.RequestAttachment
	// before it can be rendered or saved.
	Attachments []wire.Attachment
	// AttachmentToken/AttachmentName/AttachmentContentType/
	// AttachmentContentBytes carry an "attachmentData" event's payload —
	// the reply to RequestAttachment's fetch-by-token request. See
	// wire.AttachmentData.
	AttachmentToken        string
	AttachmentName         string
	AttachmentContentType  string
	AttachmentContentBytes string
	// Format carries a "msg"/"messageEdited"'s wire.Msg.Format/
	// wire.MessageEdited.Format — a server extension (chat-relay: "text"
	// or "html") describing how Text should be interpreted. Empty means
	// either the field was absent or unset — the receiving side's own
	// default ("text").
	Format string
	// Answers carries a "msg" or "noMoreMessages" event's
	// wire.Msg.Answers/wire.NoMoreMessages.Answers — the exact anchor a
	// MessageAfter request was sent with, echoed back. Nil for a "msg"
	// reached any other way (live traffic) — its presence is what
	// identifies this event as the answer to a specific pull. See
	// wire.MessageAfter's doc comment.
	Answers *wire.Anchor
}

// PeerInfo is what's known about one other peer in the session.
type PeerInfo struct {
	ID           string
	Name         string
	AgePublicKey string
}

type Conn struct {
	ws            *websocket.Conn
	peerID        string
	name          string
	agePublicKey  string
	serverVersion int
	expectedPeers int
	// The fields below are only ever set for a bridge-style session (see
	// wire.Joined) — zero-valued for every mcp-hub-server connection.
	canSend          bool
	conversationKind string
	topic            *string
	systemPeerID     string
	behind           int
	behindSince      string
	// pongWait is snapshotted from the package-level var once, synchronously,
	// in Dial — never read from the background readLoop goroutine directly.
	// Reading the mutable package var from that goroutine on every loop
	// iteration raced against tests reassigning it for a *different* Conn's
	// Dial call: Go's race detector doesn't require actual timing overlap,
	// only the absence of a happens-before edge, and an unsynchronized
	// background goroutine has none with a later test's assignment.
	// Capturing here, before `go c.readLoop()` is spawned, uses the
	// goroutine-creation happens-before edge instead.
	pongWait time.Duration

	// ackIdleInterval is snapshotted the same way and for the same reason
	// as pongWait above — read only by ackLoop, captured before it's
	// spawned.
	ackIdleInterval time.Duration

	// isBridge is true only for a connection made via DialRelay — set once
	// during construction, read only after Dial/DialRelay has returned, so
	// (like peerID/name/etc. above) it needs no locking: the happens-before
	// edge is the same construct-then-return-then-use pattern the rest of
	// this immutable-after-construction group already relies on.
	isBridge bool

	mu         sync.Mutex
	buffer     []Event
	closed     bool
	closeCode  int       // set from the WebSocket close frame's code, if any — see DisconnectNote
	timedOut   bool      // set when the connection was dropped by our own pongWait deadline, not a close frame — see DisconnectNote
	timedOutAt time.Time // when timedOut was set — see DisconnectNote's use of it against lastFrameAt
	// lastFrameKind/lastFrameAt record the most recent frame observed —
	// "joined" (construction), "ping" (a control frame, from
	// SetPingHandler — gorilla handles these internally and they never
	// reach readLoop directly), or a decoded Event.Kind (a data frame, from
	// readLoop). Surfaced by DisconnectNote on a timeout so "the connection
	// died" comes with "and here's the last thing we actually heard",
	// rather than requiring a second round of debugging to find out.
	lastFrameKind string
	lastFrameAt   time.Time
	// lastSeenCursor is the Cursor of the most recent event delivered into
	// buffer that carried one (a "msg" or "messageDeleted") — see
	// LastSeenCursor. This is what the model actually saw through this
	// connection, the anchor hub_catch_up needs to resume from anything
	// that arrived while disconnected.
	lastSeenCursor string
	// lastConsumed/lastAckSent/ackDisabled implement the read-receipt
	// contract — see LastConsumedCursor, Drain, and ackLoop.
	//   - lastConsumed: cursor of the most recent event actually returned
	//     by Drain (i.e. delivered to the model, not merely buffered).
	//   - lastAckSent: cursor value of the last read receipt actually sent
	//     (piggybacked or standalone) — the idle timer only fires a
	//     standalone ack when lastConsumed has moved past this.
	//   - ackDisabled: set true if the server ever rejects a receipt as
	//     bad_ack/bad_ack_cursor (the ack-subsystem-specific codes — not
	//     the generic bad_cursor/bad_request other request kinds may also
	//     use) — per the server side's own guidance, that's a client-side
	//     bug, not transient, so retrying (with any cursor) would fail
	//     identically; stop trying rather than loop.
	lastConsumed    string
	lastAckSent     string
	ackDisabled     bool
	onActivity      func()
	peers           map[string]PeerInfo
	rosterAnnounced bool
	// pendingAcks holds at most one outstanding claim per expected ack
	// kind ("sendAck"/"reactionAck"/"editAck") — see claimNextAck.
	pendingAcks map[string]*ackClaim
	// pendingMessageAfter holds at most one outstanding MessageAfter
	// claim — see RequestMessageAfterAwaiting. Kept separate from
	// pendingAcks because its answer can arrive as one of three
	// different Kinds ("msg" with Answers set, "noMoreMessages", or
	// "error"), and — critically — an ordinary "msg" with no Answers
	// must NOT be diverted here, unlike pendingAcks' blind kind match;
	// see tryDivertToClaimLocked.
	pendingMessageAfter *ackClaim
}

// ackClaim is a one-shot subscription for the next event matching a
// specific ack kind (or a generic "error") — see claimNextAck.
type ackClaim struct {
	result chan Event // buffered, size 1; written to exactly once
}

// relayCloseNotes maps close codes a bridge server (e.g. a Teams-relay
// bridge) may use to signal a dead credential rather than ordinary network
// trouble, agreed as: 4001 revoked, 4002 expired, 4003 the link's
// conversation became unavailable (e.g. the bot was removed from it).
// mcp-hub-server itself never sends any of these — a plain drop from it
// still gets no note, same as before this existed. These are a secondary,
// best-effort signal: the primary one is a server sending a final "error"
// event (with Code/Retryable) before it closes, which a model actually
// reading events will see; this note is for the case where that event was
// missed (e.g. the read that would have surfaced it was itself cut short).
//
// 4005 was proposed and reserved for "this connection couldn't keep up
// with live delivery and was closed deliberately" (see the design doc's
// backpressure section) but deliberately isn't listed here: chat-relay's
// author found live, 2026-09-04, that a graceful close frame is itself a
// write, and the condition 4005 would signal is exactly "writes to this
// peer don't complete" — so it can never actually be sent for the case it
// exists to describe. chat-relay's real implementation aborts the raw
// transport instead (an ordinary close, no code — 1006 or similar), which
// this client already handles correctly as an unremarkable disconnect:
// reconnect, then hub_catch_up from the last known position. No client
// change was needed once that was understood; a genuinely-unreachable
// code isn't worth carrying a note for.
var relayCloseNotes = map[int]string{
	4001: " (revoked — do not reconnect)",
	4002: " (expired — do not reconnect)",
	4003: " (conversation unavailable — do not reconnect)",
}

// DisconnectNote returns extra text to append to a bare disconnect message
// explaining why the connection ended, whenever more than "it's gone" is
// known: a relay-specific close code (see relayCloseNotes), our own
// pongWait deadline expiring with no word from the server (indistinguishable
// from either end which of a network partition, a killed server process, or
// an intermediary silently dropping the socket actually happened), or some
// other close code the server sent that isn't one of the relay-specific
// ones (e.g. 1006 for an abnormal closure the OS/network layer detected).
// Empty only when the disconnect carried no signal at all beyond "gone".
func (c *Conn) DisconnectNote() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if note, ok := relayCloseNotes[c.closeCode]; ok {
		return note
	}
	if c.timedOut {
		return fmt.Sprintf(" (no activity from the server for %s — connection assumed dead;"+
			" this does not by itself indicate which side or which network hop failed;"+
			" last frame seen was %q, %s before the deadline expired)",
			c.pongWait, c.lastFrameKind, c.timedOutAt.Sub(c.lastFrameAt).Round(time.Second))
	}
	// CloseNormalClosure/CloseGoingAway are a deliberate, expected close (the
	// server said goodbye on purpose) — not a mystery worth reporting.
	if c.closeCode != 0 && c.closeCode != websocket.CloseNormalClosure && c.closeCode != websocket.CloseGoingAway {
		return fmt.Sprintf(" (connection closed, code %d)", c.closeCode)
	}
	return ""
}

// DialOptions carries the optional, untrusted-to-everyone-else identity a
// client presents on connect.
type DialOptions struct {
	// Name is a free-text display name shown alongside logs and reported to
	// other peers. The server sanitizes it (control characters stripped,
	// length capped) before relaying it or writing it to the log — treat
	// whatever comes back in Conn.Name() as the authoritative value.
	Name string
	// AgePublicKey is an age (https://age-encryption.org) recipient string.
	// It is validated for correct bech32 format (agekey.Valid) — both here
	// and again server-side — but never parsed, decoded, or used
	// cryptographically by the hub in any way; it is only distributed to
	// other peers so they can encrypt to this one, entirely outside the
	// hub's involvement. It does NOT affect peerId reuse — see
	// ReconnectSecret — because it's broadcast to every other peer in the
	// session, so keying identity off it would let anyone who saw it
	// impersonate that peer on reconnect.
	AgePublicKey string
	// ReconnectSecret, if given, is never distributed to anyone — only this
	// client and the server ever see it. If a peer previously connected to
	// this same still-alive session with this exact secret, it is
	// reassigned that same peerId (see hubsession.Session.Join), as long as
	// that previous connection isn't still active; otherwise it's simply
	// remembered for a future reconnect. Any string works — a UUID, a
	// random token, whatever the caller wants to remember and present again
	// later.
	ReconnectSecret string
	// CreateToken, if given, is sent as the X-Hub-Create-Token header — a
	// chat-relay extension (not part of the base wire protocol: an unknown
	// server simply never looks at it, per the protocol's own "unknown
	// fields are ignored" rule) letting a client create and claim a
	// brand-new session in one handshake, for a server that 404s an
	// unknown sessionId by design rather than creating one on first
	// connect (mcp-hub-server's own behavior). Sent as a header rather
	// than a query parameter deliberately: a query string ends up in
	// plaintext in a reverse proxy's access log, a header does not. Only
	// meaningful when sessionID doesn't already exist on the target
	// server — an existing session is joined normally and the token is
	// ignored.
	CreateToken string
}

// Dial connects to host+"/"+sessionID (e.g. "ws://localhost:8765" joining
// session "550e8400-..." dials "ws://localhost:8765/550e8400-..."), which
// auto-joins the session as part of the websocket handshake — no separate
// join message is sent. host is validated/normalized first (see
// normalizeHost) so a common mistake like using https:// or including a
// path fails with a clear message instead of an opaque dial error. Starts a
// background read loop on success.
func Dial(host, sessionID string, opts DialOptions) (*Conn, error) {
	// Snapshot once, synchronously, before any goroutine is spawned — see
	// the pongWait field's doc comment on Conn for why.
	snapPongWait, snapWriteWait, snapAckIdleInterval := pongWait, writeWait, ackIdleInterval

	base, err := normalizeHost(host)
	if err != nil {
		return nil, err
	}
	if opts.AgePublicKey != "" && !agekey.Valid(opts.AgePublicKey) {
		return nil, fmt.Errorf("agePublicKey is not a validly formatted age public key")
	}
	target := base + "/" + sessionID + "?v=" + strconv.Itoa(wire.ProtocolVersion)
	if opts.Name != "" {
		target += "&name=" + url.QueryEscape(opts.Name)
	}
	if opts.AgePublicKey != "" {
		target += "&agePublicKey=" + url.QueryEscape(opts.AgePublicKey)
	}
	if opts.ReconnectSecret != "" {
		target += "&reconnectSecret=" + url.QueryEscape(opts.ReconnectSecret)
	}
	var header http.Header
	if opts.CreateToken != "" {
		header = http.Header{}
		header.Set("X-Hub-Create-Token", opts.CreateToken)
	}
	ws, _, err := websocket.DefaultDialer.Dial(target, header)
	if err != nil {
		return nil, err
	}
	return finishHandshake(ws, snapPongWait, snapWriteWait, snapAckIdleInterval, false)
}

// finishHandshake reads the server's initial "joined" message off an
// already-connected ws, validates it, and builds the running Conn —
// the tail shared by Dial and DialRelay, which differ only in how they
// reach an open *websocket.Conn (a normalized host+sessionId URL with no
// custom headers, vs. an arbitrary caller-supplied URL with an
// Authorization/etc. header) and in isBridge, which DialRelay passes true.
func finishHandshake(ws *websocket.Conn, snapPongWait, snapWriteWait, snapAckIdleInterval time.Duration, isBridge bool) (*Conn, error) {
	_, raw, err := ws.ReadMessage()
	if err != nil {
		ws.Close()
		return nil, err
	}
	var joined wire.Joined
	if err := json.Unmarshal(raw, &joined); err != nil {
		ws.Close()
		return nil, err
	}
	if !wire.IsValidID(joined.PeerID) {
		ws.Close()
		return nil, fmt.Errorf("server returned malformed peerId %q", joined.PeerID)
	}

	ws.SetReadDeadline(time.Now().Add(snapPongWait))

	c := &Conn{
		ws:               ws,
		peerID:           joined.PeerID,
		name:             joined.Name,
		agePublicKey:     joined.AgePublicKey,
		serverVersion:    joined.ServerVersion,
		expectedPeers:    joined.PeerCount,
		peers:            make(map[string]PeerInfo),
		pongWait:         snapPongWait,
		canSend:          joined.CanSend,
		conversationKind: joined.ConversationKind,
		topic:            joined.Topic,
		systemPeerID:     joined.SystemPeerID,
		behind:           joined.Behind,
		behindSince:      joined.BehindSince,
		isBridge:         isBridge,
		lastFrameKind:    "joined",
		lastFrameAt:      time.Now(),
		ackIdleInterval:  snapAckIdleInterval,
	}
	ws.SetPingHandler(func(appData string) error {
		ws.SetReadDeadline(time.Now().Add(snapPongWait))
		c.mu.Lock()
		c.lastFrameKind, c.lastFrameAt = "ping", time.Now()
		c.mu.Unlock()
		return ws.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(snapWriteWait))
	})
	go c.readLoop()
	go c.ackLoop()
	return c, nil
}

func (c *Conn) PeerID() string { return c.peerID }

// Name is this connection's own display name, after server-side
// sanitization — empty if none was supplied.
func (c *Conn) Name() string { return c.name }

// AgePublicKey is this connection's own age public key, echoed back by the
// server — empty if none was supplied.
func (c *Conn) AgePublicKey() string { return c.agePublicKey }

// ServerVersion is the wire.ProtocolVersion the server reported in "joined".
// Compare against wire.ProtocolVersion to tell if this client is behind.
func (c *Conn) ServerVersion() int { return c.serverVersion }

// SystemPeerID is the server's operator/system peerId for this session, if
// it has one — see wire.Joined.SystemPeerID. Empty when the server doesn't
// set this concept (including every mcp-hub-server).
func (c *Conn) SystemPeerID() string { return c.systemPeerID }

// ExpectedPeerCount is how many peers were already in the session at join
// time, as reported by the server's "joined" message — i.e. how many
// peerJoined events make up the initial roster catch-up.
func (c *Conn) ExpectedPeerCount() int { return c.expectedPeers }

// The accessors below only ever return a non-zero/non-nil value for a
// bridge-style session (see wire.Joined) — nil/zero for every
// mcp-hub-server connection, since the server never sets these.

// LastSeenCursor is the cursor of the most recent message (or
// messageDeleted) this connection actually delivered — via Peek/Drain, so
// through hub_receive/hub_wait — not merely wrote to the underlying
// socket. Empty if nothing carrying a cursor has been delivered yet. Set
// from whatever this connection has itself observed, on any server.
// Report this on disconnect so a reconnecting client knows exactly what to
// pass hub_catch_up as an anchor, instead of having to have remembered it
// unprompted.
func (c *Conn) LastSeenCursor() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastSeenCursor
}

// CanSend reports whether sending was permitted as of connect time — not
// a guarantee for any send made after, since the underlying policy can
// change mid-session.
func (c *Conn) CanSend() bool { return c.canSend }

// ConversationKind and Topic describe what was joined (e.g.
// "oneOnOne"/"group"/"meeting", and a display name where one exists).
func (c *Conn) ConversationKind() string { return c.conversationKind }

// Behind/BehindSince report this peer's position relative to the newest
// message, as of connect — see wire.Joined.Behind/BehindSince. Zero/empty
// when a server doesn't set this concept (including every
// mcp-hub-server, and a fresh position with no prior cursor to compare).
func (c *Conn) Behind() int         { return c.behind }
func (c *Conn) BehindSince() string { return c.behindSince }
func (c *Conn) Topic() *string           { return c.topic }

// IsBridge reports whether this connection was made via DialRelay rather
// than Dial — a bridge-style session where a write action (send/react/
// edit) gets an asynchronous ack/error rather than mcp-hub-server's
// synchronous-enough plain send. Callers use this to decide whether
// claimNextAck is worth using at all: waiting on it against a plain
// mcp-hub-server connection, which never sends an ack of any kind, would
// just be a pure timeout tax on every send for no benefit.
func (c *Conn) IsBridge() bool { return c.isBridge }

// RosterComplete reports whether the server's "rosterComplete" event — sent
// once it has finished delivering this peer's initial roster — has been
// seen yet. Once true, that event has also been buffered (see Drain/Peek) —
// this method is for an on-demand check; the buffered event is the actual
// notification.
func (c *Conn) RosterComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rosterAnnounced
}

// Peers returns everyone else currently known to be in the session, sorted
// by peerId for stable output. Built entirely from peerJoined/peerLeft
// events seen so far — since a newly joined peer is told the full existing
// roster on join (see the server's "roster on join" behavior), this is
// complete from shortly after Dial returns, not just for peers who joined
// after this connection did.
func (c *Conn) Peers() []PeerInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]PeerInfo, 0, len(c.peers))
	for _, info := range c.peers {
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// OnActivity registers a callback invoked (from the background read
// goroutine) after every new buffered event and on disconnect.
func (c *Conn) OnActivity(f func()) {
	c.mu.Lock()
	c.onActivity = f
	c.mu.Unlock()
}

// claimNextAck registers to intercept the next event of kind ackKind for
// this connection, or the next generic "error" event, whichever arrives
// first — giving a caller that just issued a write request (a send/
// react/edit) a way to learn its own outcome synchronously, without
// racing hub_wait or the CLI wait socket over the shared event buffer
// (see Peek/Drain). Without this, a caller that itself polled Peek/Drain
// for its own ack would risk destructively draining an event meant for a
// concurrently blocked hub_wait call — the identical class of bug fixed
// once already for hub_wait vs. the CLI wait socket's registration race
// (see waiter.Waiter.Poke). A diverted event is delivered only to the
// claim; it never reaches the general buffer, Peek/Drain, or OnActivity —
// intentional, since the caller that issued the request already gets the
// answer directly and nothing else is waiting to be told about it again.
//
// A generic "error" is diverted, not just an exact ackKind match, because
// that's how this protocol reports a refusal (e.g. the tenant lock) —
// there is no per-request id linking an error back to the specific
// action that caused it, so an error arriving while a claim is pending is
// assumed to be that claim's outcome. This is a real, acknowledged
// protocol limitation, not something this method can fully close: in
// principle a claim can absorb an error that was actually about a
// different concurrent action. cancel releases the claim (e.g. on
// timeout) so a later, unrelated event isn't wrongly attributed once the
// caller has stopped waiting — after cancel, or once the claim has fired,
// everything reverts to the normal buffer path.
func (c *Conn) claimNextAck(ackKind string) (result <-chan Event, cancel func()) {
	ch := make(chan Event, 1)
	c.mu.Lock()
	if c.pendingAcks == nil {
		c.pendingAcks = make(map[string]*ackClaim)
	}
	claim := &ackClaim{result: ch}
	c.pendingAcks[ackKind] = claim
	c.mu.Unlock()
	cancel = func() {
		c.mu.Lock()
		if existing, ok := c.pendingAcks[ackKind]; ok && existing == claim {
			delete(c.pendingAcks, ackKind)
		}
		c.mu.Unlock()
	}
	return ch, cancel
}

// tryDivertToClaimLocked checks ev against any pending ack claims and, if
// it matches one, delivers it directly and reports true — the caller
// (readLoop) must skip buffering it and firing OnActivity for it entirely
// when this returns true. Must be called with c.mu held.
func (c *Conn) tryDivertToClaimLocked(ev Event) bool {
	// pendingMessageAfter's match is not a blind Kind lookup like
	// pendingAcks below — a "msg" only belongs to it when Answers is
	// actually set (i.e. it's genuinely the answer to a pull), never for
	// an ordinary live "msg", which must keep flowing to the general
	// buffer untouched. "noMoreMessages" always belongs to it, since
	// that Kind has no other meaning on this connection. A generic
	// "error" is ambiguous with pendingAcks (see below) and handled
	// together with it, not here.
	if c.pendingMessageAfter != nil {
		switch {
		case ev.Kind == "msg" && ev.Answers != nil, ev.Kind == "noMoreMessages":
			claim := c.pendingMessageAfter
			c.pendingMessageAfter = nil
			claim.result <- ev
			return true
		}
	}
	if claim, ok := c.pendingAcks[ev.Kind]; ok {
		delete(c.pendingAcks, ev.Kind)
		claim.result <- ev
		return true
	}
	if ev.Kind == "error" {
		// Any one pending claim can absorb a generic error — there is no
		// per-request id to pick the "right" one. With more than one
		// claim active concurrently this is already an edge case (see
		// claimNextAck's doc comment), so an arbitrary choice (Go map
		// iteration order) is acceptable rather than correctness-critical.
		// pendingMessageAfter is included in that pool: an error with no
		// correlation id is equally ambiguous between an ack claim and a
		// MessageAfter claim.
		if c.pendingMessageAfter != nil {
			claim := c.pendingMessageAfter
			c.pendingMessageAfter = nil
			claim.result <- ev
			return true
		}
		for kind, claim := range c.pendingAcks {
			delete(c.pendingAcks, kind)
			claim.result <- ev
			return true
		}
	}
	return false
}

// handleAckPlumbingLocked intercepts the read-receipt protocol's own
// traffic — the server's reply to an ack we sent (success or a stale-cursor
// rejection) and the two error codes that mean an ack itself was malformed
// — before it ever reaches the model-visible buffer or the claim-diversion
// mechanism. None of this is something the model asked for or should see:
// it's internal bookkeeping about how far this connection has told the
// server it has read. Reports whether ev was consumed here (true) or
// should fall through to normal handling (false). Must be called with c.mu
// held.
func (c *Conn) handleAckPlumbingLocked(ev Event) bool {
	if ev.Kind == "ack" {
		if !ev.ActionOK {
			// Our receipt was behind what the server holds — per the
			// server side's own guidance, adopt its reported position as
			// our own lastAckSent rather than retry: monotonicity means
			// nothing is lost, since it only rejects a position older
			// than one it already has.
			c.lastAckSent = ev.Cursor
		}
		return true
	}
	if ev.Kind == "error" && (ev.Code == "bad_ack" || ev.Code == "bad_ack_cursor") {
		// bad_ack/bad_ack_cursor are specific to the ack subsystem — unlike
		// the generic bad_cursor/bad_request a bridge server may also use
		// for unrelated requests (a malformed reaction, a history request
		// naming both before and after), which must never trip this: error
		// events carry no correlation id, so a generic code can't be
		// attributed to "this was about an ack" at all. A malformed or
		// missing ack cursor is a bug in this client's own bookkeeping, not
		// a transient condition (retryable is false, and resending would
		// fail identically) — stop sending receipts entirely rather than
		// repeat the same mistake on every future event. Not surfaced to
		// the model: it never asked for this ack,
		// so an error about it would be pure noise.
		c.ackDisabled = true
		return true
	}
	return false
}

// ackLoop periodically sends a standalone read receipt (wire.Ack) if
// lastConsumed has moved past lastAckSent since the last one actually sent
// — piggybacked or standalone — since the last tick. Sends nothing when
// nothing new has been read: an idle receipt repeating a position the
// server already holds would turn a read receipt into a heartbeat. Runs
// for the connection's lifetime; a write to a closed connection simply
// errors and ends the loop on its own, the same shape pingLoop uses
// server-side (see wsserver.peer.pingLoop).
func (c *Conn) ackLoop() {
	ticker := time.NewTicker(c.ackIdleInterval)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		if c.ackDisabled || c.closed {
			c.mu.Unlock()
			return
		}
		consumed, sent := c.lastConsumed, c.lastAckSent
		c.mu.Unlock()
		if consumed == "" || consumed == sent {
			continue
		}
		if err := c.ws.WriteJSON(wire.NewAck(consumed)); err != nil {
			return
		}
		c.mu.Lock()
		c.lastAckSent = consumed
		c.mu.Unlock()
	}
}

func (c *Conn) readLoop() {
	for {
		_, raw, err := c.ws.ReadMessage()
		if err == nil {
			c.ws.SetReadDeadline(time.Now().Add(c.pongWait))
		}
		if err != nil {
			c.mu.Lock()
			c.closed = true
			if ce, ok := err.(*websocket.CloseError); ok {
				c.closeCode = ce.Code
			} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
				c.timedOut = true
				c.timedOutAt = time.Now()
			}
			f := c.onActivity
			c.mu.Unlock()
			if f != nil {
				f()
			}
			return
		}
		ev, ok := decodeEvent(raw)
		if !ok {
			continue
		}
		c.mu.Lock()
		c.lastFrameKind, c.lastFrameAt = ev.Kind, time.Now()
		if c.handleAckPlumbingLocked(ev) {
			c.mu.Unlock()
			continue
		}
		if c.tryDivertToClaimLocked(ev) {
			c.mu.Unlock()
			continue
		}
		if ev.PeerID != "" && c.systemPeerID != "" && ev.PeerID == c.systemPeerID {
			ev.IsOperator = true
		}
		c.buffer = append(c.buffer, ev)
		if ev.Cursor != "" {
			c.lastSeenCursor = ev.Cursor
		}
		switch ev.Kind {
		case "peerJoined":
			c.peers[ev.PeerID] = PeerInfo{ID: ev.PeerID, Name: ev.Name, AgePublicKey: ev.AgePublicKey}
		case "peerLeft":
			delete(c.peers, ev.PeerID)
		case "rosterComplete":
			c.rosterAnnounced = true
		}
		f := c.onActivity
		c.mu.Unlock()
		if f != nil {
			f()
		}
	}
}

// DecodeEvent decodes a single raw wire-protocol JSON event into an Event —
// exported so other in-process code that constructs the same wire.* values
// directly (see internal/httpmcp) can reuse this decoding/normalization
// logic instead of duplicating it, by marshaling the wire value and
// decoding it right back rather than hand-rolling a second conversion path.
func DecodeEvent(raw []byte) (Event, bool) {
	return decodeEvent(raw)
}

func decodeEvent(raw []byte) (Event, bool) {
	typ, err := wire.DecodeType(raw)
	if err != nil {
		return Event{}, false
	}
	switch typ {
	case wire.TypeMsg:
		var m wire.Msg
		if err := json.Unmarshal(raw, &m); err != nil || !wire.IsValidID(m.PeerID) {
			return Event{}, false
		}
		return Event{Kind: "msg", PeerID: m.PeerID, Text: m.Text, TS: m.TS, Private: m.Private,
			Historical: m.Historical, ExternalID: m.ExternalID, Own: m.Own, Cursor: m.Cursor,
			Attachments: m.Attachments, Format: m.Format, ReplyTo: m.ReplyTo, ReplyPreview: m.ReplyPreview,
			Mentions: m.Mentions, MentionedMe: m.MentionedMe, Answers: m.Answers}, true
	case wire.TypeNoMoreMessages:
		var n wire.NoMoreMessages
		if err := json.Unmarshal(raw, &n); err != nil {
			return Event{}, false
		}
		return Event{Kind: "noMoreMessages", Answers: n.Answers}, true
	case wire.TypeError:
		var e wire.Error
		if err := json.Unmarshal(raw, &e); err != nil {
			return Event{}, false
		}
		return Event{Kind: "error", Text: e.Message, Code: e.Code, Retryable: e.Retryable}, true
	case wire.TypePeerJoined:
		var p wire.PeerEvent
		if err := json.Unmarshal(raw, &p); err != nil || !wire.IsValidID(p.PeerID) {
			return Event{}, false
		}
		return Event{Kind: "peerJoined", PeerID: p.PeerID, Name: p.Name, AgePublicKey: p.AgePublicKey}, true
	case wire.TypePeerLeft:
		var p wire.PeerEvent
		if err := json.Unmarshal(raw, &p); err != nil || !wire.IsValidID(p.PeerID) {
			return Event{}, false
		}
		return Event{Kind: "peerLeft", PeerID: p.PeerID}, true
	case wire.TypeRosterComplete:
		return Event{Kind: "rosterComplete"}, true
	case wire.TypeSendAck:
		var a wire.SendAck
		if err := json.Unmarshal(raw, &a); err != nil {
			return Event{}, false
		}
		return Event{Kind: "sendAck", ExternalID: a.ExternalID, ActionOK: a.OK}, true
	case wire.TypeReactionChanged:
		var r wire.ReactionChanged
		if err := json.Unmarshal(raw, &r); err != nil {
			return Event{}, false
		}
		if r.PeerID != "" && !wire.IsValidID(r.PeerID) {
			return Event{}, false
		}
		return Event{Kind: "reactionChanged", PeerID: r.PeerID, ExternalID: r.ExternalID,
			Reaction: r.Reaction, ReactionLabel: r.Label, ReactionAction: r.Action, TS: r.TS, Own: r.Own}, true
	case wire.TypeMessageEdited:
		var m wire.MessageEdited
		if err := json.Unmarshal(raw, &m); err != nil {
			return Event{}, false
		}
		return Event{Kind: "messageEdited", ExternalID: m.ExternalID, Text: m.Text, TS: m.TS, Own: m.Own,
			Attachments: m.Attachments, Format: m.Format, ReplyTo: m.ReplyTo, ReplyPreview: m.ReplyPreview,
			Mentions: m.Mentions, MentionedMe: m.MentionedMe}, true
	case wire.TypeReactionAck:
		var a wire.ReactionAck
		if err := json.Unmarshal(raw, &a); err != nil {
			return Event{}, false
		}
		return Event{Kind: "reactionAck", ExternalID: a.ExternalID, Reaction: a.Reaction,
			ReactionAction: a.Action, ActionOK: a.OK}, true
	case wire.TypeEditAck:
		var a wire.EditAck
		if err := json.Unmarshal(raw, &a); err != nil {
			return Event{}, false
		}
		return Event{Kind: "editAck", ExternalID: a.ExternalID, ActionOK: a.OK}, true
	case wire.TypeDeleteAck:
		var a wire.DeleteAck
		if err := json.Unmarshal(raw, &a); err != nil {
			return Event{}, false
		}
		return Event{Kind: "deleteAck", ExternalID: a.ExternalID, ActionOK: a.OK}, true
	case wire.TypeMessageDeleted:
		var d wire.MessageDeleted
		if err := json.Unmarshal(raw, &d); err != nil {
			return Event{}, false
		}
		return Event{Kind: "messageDeleted", ExternalID: d.ExternalID, Cursor: d.Cursor, TS: d.TS, Own: d.Own}, true
	case wire.TypeAck:
		var a wire.Ack
		if err := json.Unmarshal(raw, &a); err != nil {
			return Event{}, false
		}
		// Cursor here is the server's reply, not an echo — see wire.Ack's
		// doc comment: on OK false it's the position the server actually
		// holds, to be adopted rather than treated as confirmation of what
		// was sent.
		return Event{Kind: "ack", Cursor: a.AckCursor, ActionOK: a.OK}, true
	case wire.TypeAttachmentData:
		var a wire.AttachmentData
		if err := json.Unmarshal(raw, &a); err != nil {
			return Event{}, false
		}
		return Event{Kind: "attachmentData", AttachmentToken: a.Token, AttachmentName: a.Name,
			AttachmentContentType: a.ContentType, AttachmentContentBytes: a.ContentBytes}, true
	default:
		return Event{}, false
	}
}

// ackCursorForOutbound returns the read-receipt cursor to piggyback on the
// next outbound message, and records that it's about to be sent (updating
// lastAckSent) — see the Conn field doc comments for the full contract.
// Every outbound method sends this unconditionally (rule: "piggyback it on
// every outbound message"), even when unchanged from the last one sent;
// only the idle timer (ackLoop) additionally checks whether it moved.
// Empty once ackDisabled (a prior bad_ack/bad_ack_cursor) or before
// anything has been consumed yet.
func (c *Conn) ackCursorForOutbound() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ackDisabled || c.lastConsumed == "" {
		return ""
	}
	c.lastAckSent = c.lastConsumed
	return c.lastConsumed
}

func (c *Conn) Send(text string, attachments []wire.Attachment, format, replyTo string) error {
	m := wire.NewOutgoingMsg(text)
	m.AckCursor = c.ackCursorForOutbound()
	m.Attachments = attachments
	m.Format = format
	m.ReplyTo = replyTo
	return c.ws.WriteJSON(m)
}

// SendTo sends text privately to a single peer, identified by peerId. The
// server processes this asynchronously: a delivery failure (e.g. an unknown
// or departed peer) does not surface as a returned error here, but as a
// buffered "error" event picked up by a later Peek/Drain.
func (c *Conn) SendTo(text, peerID string, attachments []wire.Attachment, format, replyTo string) error {
	m := wire.NewOutgoingDirectedMsg(text, peerID)
	m.AckCursor = c.ackCursorForOutbound()
	m.Attachments = attachments
	m.Format = format
	m.ReplyTo = replyTo
	return c.ws.WriteJSON(m)
}

// RequestMessageAfterAwaiting sends a wire.MessageAfter request for
// anchor and waits up to AckWaitTimeout for its answer — a "msg" event
// (Historical true, Answers echoing anchor), a "noMoreMessages" event, or
// an "error" (typically Code "bad_anchor"). See wire.MessageAfter's doc
// comment for the full contract, and wire.Anchor's for what belongs in
// anchor (exactly one of Cursor/At). The returned Event is delivered
// directly to the caller — it never reaches the general buffer/Peek/
// Drain path, since the caller that issued this request is already the
// one waiting on the answer (same reasoning as SendAwaitingAck).
//
// At most one MessageAfter call may be outstanding on a Conn at a time;
// a second call issued before the first resolves replaces the first
// claim, and the first call's resultCh is abandoned (it will time out
// rather than receive an answer that instead goes to the second caller).
// mcp-hub-client's own usage never does this — see the design doc for
// why catch-up is deliberately sequential, not concurrent, walks — but
// it's worth stating since the wire itself doesn't forbid concurrent
// walks (see wire.MessageAfter's doc comment on the server side of that).
func (c *Conn) RequestMessageAfterAwaiting(anchor wire.Anchor) (Event, bool, error) {
	ch := make(chan Event, 1)
	c.mu.Lock()
	claim := &ackClaim{result: ch}
	c.pendingMessageAfter = claim
	c.mu.Unlock()
	cancel := func() {
		c.mu.Lock()
		if c.pendingMessageAfter == claim {
			c.pendingMessageAfter = nil
		}
		c.mu.Unlock()
	}
	defer cancel()

	m := wire.MessageAfter{Type: wire.TypeMessageAfter, Anchor: anchor}
	if err := c.ws.WriteJSON(m); err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-ch:
		return ev, true, nil
	case <-time.After(AckWaitTimeout):
		return Event{}, false, nil
	}
}

// React asks the server to add or remove a reaction on an earlier
// message, identified by externalID — see wire.Reaction. action is "add"
// or "remove". Not meaningful for mcp-hub-server, which silently ignores
// any message type it doesn't recognize; for a bridge server with write
// access to the underlying platform. Success/failure arrives
// asynchronously as a "reactionAck" (or an "error" event on refusal),
// picked up by a later Peek/Drain like anything else — this call only
// confirms the request was sent.
func (c *Conn) React(externalID, reaction, action string) error {
	r := wire.NewReactionRequest(externalID, reaction, action)
	r.AckCursor = c.ackCursorForOutbound()
	return c.ws.WriteJSON(r)
}

// EditMessage asks the server to change an earlier message's content,
// identified by externalID — see wire.Edit. Not meaningful for
// mcp-hub-server. Success/failure arrives asynchronously as an "editAck"
// (or an "error" event on refusal), like React. attachments, if non-nil,
// replaces the message's attachments (always the inline form — see
// wire.Edit.Attachments); pass nil to leave existing attachments alone.
func (c *Conn) EditMessage(externalID, text string, attachments []wire.Attachment, format, replyTo string) error {
	e := wire.NewEditRequest(externalID, text, attachments, format, replyTo)
	e.AckCursor = c.ackCursorForOutbound()
	return c.ws.WriteJSON(e)
}

// DeleteMessage asks the server to remove an earlier message, identified
// by externalID — see wire.Delete. Not meaningful for mcp-hub-server.
// Success/failure arrives asynchronously as a "deleteAck" (or an "error"
// event on refusal), like React/EditMessage.
func (c *Conn) DeleteMessage(externalID string) error {
	d := wire.NewDeleteRequest(externalID)
	d.AckCursor = c.ackCursorForOutbound()
	return c.ws.WriteJSON(d)
}

// AckWaitTimeout is how long SendAwaitingAck/ReactAwaitingAck/
// EditMessageAwaitingAck wait for their own ack (or a generic error)
// before giving up — generous relative to how fast an ack has actually
// been observed to arrive against a real bridge server (well under a
// second), while staying small enough that a caller mistakenly calling
// one of these against a connection where IsBridge() is false wouldn't
// be worth blocking on for long even without the IsBridge check these
// methods already do first.
var AckWaitTimeout = 5 * time.Second

// SendAwaitingAck sends text (broadcast, or to a single peer if to is
// non-empty) and, only for a bridge connection (see IsBridge — a plain
// mcp-hub-server connection never emits any ack at all, so waiting would
// be a pure timeout tax for no benefit), waits up to AckWaitTimeout for
// its own outcome via claimNextAck: a "sendAck" event on success, an
// "error" event on refusal.
//
// Returns (event, true, nil) if an outcome arrived in time — format it
// with FormatEvent to report it, whether success or failure, directly as
// the caller's own answer. Returns (Event{}, false, nil) if this isn't a
// bridge connection (nothing to wait for; report success the way this
// always has, since the write itself is fire-and-forget either way) or
// if the wait timed out with no outcome yet — the write may still
// succeed or fail later, reported the normal way via wait/hub_receive/
// hub_wait, exactly as before this existed. Returns (Event{}, false, err)
// only if the write itself failed locally.
func (c *Conn) SendAwaitingAck(text, to string, attachments []wire.Attachment, format, replyTo string) (Event, bool, error) {
	if !c.isBridge {
		if to == "" {
			return Event{}, false, c.Send(text, attachments, format, replyTo)
		}
		return Event{}, false, c.SendTo(text, to, attachments, format, replyTo)
	}
	resultCh, cancel := c.claimNextAck("sendAck")
	defer cancel()
	var err error
	if to == "" {
		err = c.Send(text, attachments, format, replyTo)
	} else {
		err = c.SendTo(text, to, attachments, format, replyTo)
	}
	if err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-resultCh:
		return ev, true, nil
	case <-time.After(AckWaitTimeout):
		return Event{}, false, nil
	}
}

// ReactAwaitingAck is React, but — only for a bridge connection — waits
// up to AckWaitTimeout for its own "reactionAck"/"error" outcome. See
// SendAwaitingAck for the full contract; identical shape.
func (c *Conn) ReactAwaitingAck(externalID, reaction, action string) (Event, bool, error) {
	if !c.isBridge {
		return Event{}, false, c.React(externalID, reaction, action)
	}
	resultCh, cancel := c.claimNextAck("reactionAck")
	defer cancel()
	if err := c.React(externalID, reaction, action); err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-resultCh:
		return ev, true, nil
	case <-time.After(AckWaitTimeout):
		return Event{}, false, nil
	}
}

// EditMessageAwaitingAck is EditMessage, but — only for a bridge
// connection — waits up to AckWaitTimeout for its own "editAck"/"error"
// outcome. See SendAwaitingAck for the full contract; identical shape.
func (c *Conn) EditMessageAwaitingAck(externalID, text string, attachments []wire.Attachment, format, replyTo string) (Event, bool, error) {
	if !c.isBridge {
		return Event{}, false, c.EditMessage(externalID, text, attachments, format, replyTo)
	}
	resultCh, cancel := c.claimNextAck("editAck")
	defer cancel()
	if err := c.EditMessage(externalID, text, attachments, format, replyTo); err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-resultCh:
		return ev, true, nil
	case <-time.After(AckWaitTimeout):
		return Event{}, false, nil
	}
}

// RequestAttachment fetches the actual bytes behind a reference-form
// attachment's Token (see wire.Attachment.IsReference) by sending an
// AttachmentRequest and waiting up to AckWaitTimeout for the server's
// reply — an "attachmentData" event on success, an "error" event
// (bad_attachment/not_found/unavailable) on refusal. Returns (Event{},
// false, nil) on a bare timeout, same convention as SendAwaitingAck and
// friends — most notably including mcp-hub-server's own relay, which
// never emits a Token in the first place and so never replies to this at
// all; a caller should only ever call this for a Token actually seen on
// an Attachment.IsReference()==true entry.
func (c *Conn) RequestAttachment(token string) (Event, bool, error) {
	resultCh, cancel := c.claimNextAck("attachmentData")
	defer cancel()
	if err := c.ws.WriteJSON(wire.NewAttachmentRequest(token)); err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-resultCh:
		return ev, true, nil
	case <-time.After(AckWaitTimeout):
		return Event{}, false, nil
	}
}

// DeleteMessageAwaitingAck is DeleteMessage, but — only for a bridge
// connection — waits up to AckWaitTimeout for its own "deleteAck"/"error"
// outcome. See SendAwaitingAck for the full contract; identical shape.
func (c *Conn) DeleteMessageAwaitingAck(externalID string) (Event, bool, error) {
	if !c.isBridge {
		return Event{}, false, c.DeleteMessage(externalID)
	}
	resultCh, cancel := c.claimNextAck("deleteAck")
	defer cancel()
	if err := c.DeleteMessage(externalID); err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-resultCh:
		return ev, true, nil
	case <-time.After(AckWaitTimeout):
		return Event{}, false, nil
	}
}

// closeFlushGrace is how long Close waits, after successfully writing the
// close frame, before tearing down the underlying connection. A successful
// WriteControl only means the frame was handed to the OS, not that it
// reached the peer — closing immediately after races the write, and can
// tear the connection down before the frame is actually transmitted,
// producing exactly the abnormal closure (1006) this was meant to prevent.
// Found live: a session where the frame apparently never reached the
// server despite Close() otherwise behaving normally, traced (by
// elimination — the connection was confirmed alive and the server's own
// process confirmed not to have restarted) to this race, not to a missing
// or bridge-specific code path. Var so tests can shorten it.
var closeFlushGrace = 200 * time.Millisecond

// SetCloseFlushGraceForTesting overrides closeFlushGrace (see its own doc
// comment) for tests in another package that call Close() on loopback,
// where the real network flush race this exists for doesn't occur — the
// default would otherwise tax every such test. Mirrors
// waiter.SocketDirForTesting's shape.
func SetCloseFlushGraceForTesting(d time.Duration) (restore func()) {
	orig := closeFlushGrace
	closeFlushGrace = d
	return func() { closeFlushGrace = orig }
}

// Close sends a normal-closure WebSocket close frame before closing the
// underlying connection, so a deliberate disconnect (e.g. hub_disconnect)
// is distinguishable server-side from a crash or network failure — without
// this, every Close looked identical to an abnormal closure (1006) in a
// server's own end-reason logging, and to a peer's own next-connect
// context, making a clean disconnect indistinguishable from one that
// wasn't. If the write itself fails (e.g. the connection is already dead),
// there's nothing to flush, so Close proceeds straight to closing the
// underlying connection and returns the write's error — not silently
// discarded, since "did the frame actually go out" is exactly the
// question a caller debugging an unattributed drop needs answered.
func (c *Conn) Close() error {
	writeErr := c.ws.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client disconnect"),
		time.Now().Add(writeWait))
	if writeErr == nil {
		time.Sleep(closeFlushGrace)
	}
	closeErr := c.ws.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

// Connected reports whether the connection is still open, without touching
// the event buffer — for a caller that needs to know the connection's own
// state (e.g. before trusting cached data like the peer roster) rather than
// draining or peeking at buffered events.
func (c *Conn) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed
}

// maxPendingOwnMessages caps how many consecutive own-action echoes (see
// isSuppressibleOwnEvent) Peek will hold back before reporting hasEvents
// anyway — the overflow valve for "usually shouldn't happen" scenarios
// (e.g. a burst of sends with no reply in between), so a connection can
// never go silently unbounded on suppressed events.
const maxPendingOwnMessages = 10

// isSuppressibleOwnEvent reports whether e is this connection's own echo
// of an action it took — a "msg" it sent, or a "reactionChanged"/
// "messageEdited" it caused — the class of event Peek holds back from
// waking a caller on, since a client already knows about its own action
// and a wake carrying nothing else has no new information.
func isSuppressibleOwnEvent(e Event) bool {
	if !e.Own {
		return false
	}
	switch e.Kind {
	case "msg", "reactionChanged", "messageEdited", "messageDeleted":
		return true
	default:
		return false
	}
}

// Peek reports whether wake-worthy events are buffered, and whether the
// connection is still open. Non-destructive.
//
// "Wake-worthy" deliberately excludes this connection's own action echoes
// (see isSuppressibleOwnEvent — a "msg" it sent, or a "reactionChanged"/
// "messageEdited" it caused): the caller already knows about its own
// action, so waking on nothing but an echo of it is a wake with no new
// information, and risks an agent answering itself. Such an event stays
// buffered — not dropped — until either a genuinely new (non-own) event
// arrives, at which point Drain returns everything buffered together in
// order (the own event alongside whatever triggered the wake), or
// maxPendingOwnMessages own-action events have accumulated with nothing
// else, at which point Peek reports hasEvents anyway rather than holding
// them indefinitely. Every other event kind — including sendAck, which
// exists specifically to be an immediate signal — is always wake-worthy.
func (c *Conn) Peek() (hasEvents, connected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hasWakeWorthyEventsLocked(), !c.closed
}

func (c *Conn) hasWakeWorthyEventsLocked() bool {
	ownCount := 0
	for _, e := range c.buffer {
		if isSuppressibleOwnEvent(e) {
			ownCount++
			continue
		}
		return true
	}
	return ownCount >= maxPendingOwnMessages
}

// Drain clears and returns the buffered events formatted for delivery, and
// whether the connection is still open. This is the read-receipt system's
// "consumed" boundary (see lastConsumed's doc comment): once events leave
// here, they're considered delivered to the model, not merely received.
func (c *Conn) Drain() (formatted string, connected bool) {
	events, connected := c.DrainEvents()
	return FormatEvents(events), connected
}

// DrainBatch is Drain, but keeps each buffered event as its own formatted
// string rather than joining them into one — for a caller (Waiter's
// follow-mode delivery) that can issue one network write per event
// instead of bundling a whole burst into a single write. See
// FormatEventsBatch's doc comment for why this matters. Same
// consumed-boundary semantics as Drain/DrainEvents; only one of the three
// should be called on a given batch.
func (c *Conn) DrainBatch() (chunks []string, connected bool) {
	events, connected := c.DrainEvents()
	return FormatEventsBatch(events), connected
}

// DrainEvents is Drain without the text formatting — for a caller (like
// mcptools' image-rendering path) that needs the raw Event.Attachments
// rather than FormatEvents' text-only rendering. Same consumed-boundary
// semantics as Drain; the two must not both be called on the same batch.
func (c *Conn) DrainEvents() (events []Event, connected bool) {
	c.mu.Lock()
	events = c.buffer
	c.buffer = nil
	connected = !c.closed
	for _, e := range events {
		if e.Cursor != "" {
			c.lastConsumed = e.Cursor
		}
	}
	c.mu.Unlock()
	return events, connected
}

// LastConsumedCursor is the cursor of the most recent event actually
// delivered via Drain — exposed for tests and for a caller that wants to
// inspect read-receipt state directly, though the ack piggybacking/idle
// timer already act on it automatically.
func (c *Conn) LastConsumedCursor() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastConsumed
}

// LastAckSentCursor is the cursor value of the last read receipt actually
// sent (piggybacked or standalone) — exposed for tests and diagnostics,
// same reasoning as LastConsumedCursor.
func (c *Conn) LastAckSentCursor() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastAckSent
}

// AckDisabled reports whether the server has ever rejected a read receipt
// as malformed (bad_ack/bad_ack_cursor) — once true, this connection stops
// sending them for the rest of its lifetime. Exposed for tests and
// diagnostics.
func (c *Conn) AckDisabled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ackDisabled
}
