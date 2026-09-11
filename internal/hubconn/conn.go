package hubconn

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/agekey"
	"github.com/secforge/mcp-hub/internal/sanitize"
	"github.com/secforge/mcp-hub/internal/wire"
)

// SystemPeerIDOperator and SystemPeerIDSystem are the two fixed,
// well-known peerIds a server uses for its own operator/system-
// originated messages — restored 2026-09-08 alongside Event.IsOperator
// (see its own doc comment): the project owner's instruction was to
// stop advertising which one to expect (wire.Joined.SystemPeerID,
// removed), not to stop recognizing them ("Operator 000000 and system
// fffff must be supported") — they're constants a client checks
// directly. SystemPeerIDOperator (the Nil UUID) is the human running
// the server; SystemPeerIDSystem (all-Fs) is an automated, server-
// generated message — a teams session's own HMAC-derived peer IDs
// force a v4 nibble, so all-Fs can never collide with a real one.
const (
	SystemPeerIDOperator = "00000000-0000-0000-0000-000000000000"
	SystemPeerIDSystem   = "ffffffff-ffff-ffff-ffff-ffffffffffff"
)

// debugEnabled gates debugf below — off by default, so normal operation
// never pays for or leaks this. Added 2026-09-07 to chase a live bug
// report (hub_catch_up(gap: true) timing out repeatedly, anchor never
// advancing) that static code reading couldn't pin down: whether the
// server's answer to a MessageAfter request is actually reaching
// RequestMessageAfterAwaiting's waiting channel. Read once at package
// init rather than on every call, since it's an operator toggle, not
// something that changes mid-process.
var debugEnabled = os.Getenv("MCP_HUB_DEBUG") != ""

// debugf writes a timestamped diagnostic line to stderr when
// MCP_HUB_DEBUG is set — never to stdout, which mcp-hub-client's own
// stdio MCP transport uses for protocol framing. A no-op otherwise, so
// this is safe to leave in permanently rather than ripping out once the
// current investigation concludes.
func debugf(format string, args ...any) {
	if !debugEnabled {
		return
	}
	fmt.Fprintf(os.Stderr, "[hubconn debug %s] "+format+"\n",
		append([]any{time.Now().Format(time.RFC3339Nano)}, args...)...)
}

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
	// confirmReminderInterval is how often confirmReminderLoop checks for
	// something delivered live but never confirmed via hub_confirm (or an
	// intervening hub_receive/hub_wait/hub_catch_up) — see that method's
	// doc comment. Var so tests can shorten it.
	confirmReminderInterval = 5 * time.Minute
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
	// Behind carries a standalone ack's reply's own wire.Ack.Behind, if
	// the server sent one — see that field's doc comment. Nil on every
	// other event kind, and on an "ack" from a server that doesn't send
	// it; never zero-as-absent, since a genuine "0 behind" is meaningful
	// and must stay distinguishable from "not sent."
	// UnconfirmedCount and UnconfirmedSince are set only on a
	// "confirmReminder": how many cursor-bearing messages have been
	// delivered live without a confirm, and when that run started. They
	// are what makes the reminder state a growing cost rather than repeat
	// an instruction.
	UnconfirmedCount int
	UnconfirmedSince time.Time
	Behind *int
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
	// IsOperator is true when this event's PeerID is one of the two
	// well-known system/operator constants (SystemPeerIDOperator,
	// SystemPeerIDSystem). Which constant to expect is deliberately not
	// advertised on the wire — they are well-known and fixed, so a client
	// checks directly rather than needing to be told.
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
	// The fields below are only ever set for a teams session (see
	// wire.Joined) — zero-valued for every mcp-hub-server connection.
	canSend          bool
	conversationKind string
	topic            *string
	behind           int
	behindSince      string
	// features/featuresDeclared hold wire.Joined.Features, if the server
	// sent one at all — see that field's doc comment. featuresDeclared
	// distinguishes "no Features field sent" (pre-v3, nothing known)
	// from "Features sent but this key absent" (definitively
	// unsupported) — both look identical as a missing map entry
	// otherwise. Immutable after construction, same as the other Joined-
	// derived fields above; not under mu.
	features         map[string]json.RawMessage
	featuresDeclared bool
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

	// confirmReminderInterval is snapshotted the same way and for the
	// same reason as ackIdleInterval above — read only by
	// confirmReminderLoop, captured before it's spawned.
	confirmReminderInterval time.Duration

	// How a connection was dialled is deliberately not recorded here: it
	// is a fact about the credential, never about what the server can do,
	// and both forms are opaque links carrying the same fields anyway.
	// Everything behavioural comes from the server's own declaration —
	// see features/HasFeature.

	mu         sync.Mutex
	buffer     []Event
	// ackReplyMisses counts CONSECUTIVE ConfirmReceived calls that waited
	// for a standalone ack's reply and got nothing back within
	// AckWaitTimeout. Whether a server answers a standalone ack with a
	// Behind count (see wire.Ack.Behind) is a per-SERVER capability, so
	// it can only be probed when the server declares nothing at all.
	//
	// A counter, not a one-shot bool: ratcheting to "never wait again"
	// after a single miss turns one slow or lost reply into a permanent,
	// silent conclusion for the connection's whole remaining life. Reset
	// to 0 by any reply that DOES arrive; once it reaches
	// ackReplyMissThreshold, later confirms skip waiting entirely until a
	// genuine ack reply resets it — reconnect gets a fresh Conn and
	// starts at 0 again either way.
	ackReplyMisses int
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
	// liveUnconfirmed records that something carrying a cursor has been
	// live-delivered into buffer with no genuine hand-over since — the
	// only condition confirmReminderLoop fires on. It has to be a flag
	// rather than a comparison of lastSeenCursor against lastConsumed:
	// cursors are opaque, so equality is the only test they permit, and
	// the two positions legitimately diverge whenever a synchronous path
	// (hub_catch_up especially) hands over a cursor the live read loop
	// never saw — which would leave them unequal forever and the reminder
	// firing on every tick.
	liveUnconfirmed bool
	// unconfirmedCount and unconfirmedSince describe HOW MUCH has been
	// read live without being confirmed. The reminder needs them because
	// the cost of ignoring it is not constant: every unconfirmed message
	// is one a reconnect has to re-walk to rediscover, and a reminder that
	// reads identically at one message and at four hundred is one a reader
	// stops looking at.
	unconfirmedCount int
	unconfirmedSince time.Time
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

// relayCloseNotes maps close codes a teams relay may use to signal a
// dead credential rather than ordinary network
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

// maxNameRunes and maxTopicRunes bound the free-text header values this
// client sends. A server applies its own limits regardless; these exist so
// an over-long value is trimmed here rather than becoming a request some
// proxy rejects for header size, with no indication of which field did it.
const (
	maxNameRunes  = 64
	maxTopicRunes = 256
)

// DialOptions carries what a client presents on connect. Every field is
// sent on every connection, as a request header — a client does not
// decide per-server which are relevant, and never infers anything from
// the shape of the link it was handed. A server ignores what it does not
// use.
type DialOptions struct {
	// ReconnectSecret is REQUIRED. It is what reclaims this client's
	// identity on a later connect, and a server refuses a connection
	// without one: an identity with no secret behind it can never be
	// reclaimed, so the next connect is a different participant to the
	// server, to its peers and to its own read position — degradation
	// that shows up as lost continuity rather than as an error.
	//
	// It belongs to the client, not to whoever is driving it: minted on a
	// first connect, stored per link, and presented unchanged every time.
	ReconnectSecret string
	// AgentID is the peerId this client was last assigned here, sent only
	// when there is one. It REQUESTS that identity back, authorized by
	// ReconnectSecret — never an assertion a server should trust alone,
	// since an identity header honoured without the secret is
	// impersonation of any peer whose id someone has seen. A server that
	// cannot verify the pair refuses the connection rather than quietly
	// assigning a different identity, so the answer to "who am I" is
	// always the peerId in the server's own joined message.
	AgentID string
	// Name is a free-text display name. The server sanitizes it (control
	// characters stripped, length capped) before relaying it or logging
	// it — treat whatever comes back in Conn.Name() as authoritative.
	// What it is VISIBLE to varies by server: on a hub session other peers
	// see it; a teams relay may keep it for its own audit log and never
	// show it in the conversation.
	Name string
	// AgePublicKey is an age (https://age-encryption.org) recipient
	// string, validated for bech32 format (agekey.Valid) here and again
	// server-side, but never parsed or used cryptographically by a hub —
	// only redistributed to other peers so they can encrypt to this one.
	//
	// Deliberately NOT an identity credential: every peer in the session
	// can see it, so granting peerId reuse on it would let anyone who saw
	// it impersonate its owner. ReconnectSecret, which no peer ever sees,
	// is what identity rests on.
	AgePublicKey string
	// CreateToken is a capability for creating and claiming a session in
	// this same handshake, for a server that refuses an unknown session
	// rather than creating one on first connect. Only meaningful when the
	// link names a session that doesn't exist yet; an existing one is
	// joined normally and this is ignored.
	CreateToken string
	// Topic names a session being created. Meaningless on a join.
	Topic string
}

// Dial connects to a link — the whole address, exactly as it was issued,
// which a client never parses, reformats or reasons about. Everything
// before the "#" is dialled verbatim; the fragment is the credential and
// travels as "Authorization: Bearer", never in the request line. A URL
// fragment is by definition not transmitted, which is what keeps that
// credential out of the server's access log, any proxy in front of it,
// and this client's own logs — by construction rather than by remembering
// to redact it.
//
// Joining happens as part of the websocket handshake; no separate join
// message is sent. Starts a background read loop on success.
func Dial(link string, opts DialOptions) (*Conn, error) {
	// Snapshot once, synchronously, before any goroutine is spawned — see
	// the pongWait field's doc comment on Conn for why.
	snapPongWait, snapWriteWait, snapAckIdleInterval, snapConfirmReminderInterval := pongWait, writeWait, ackIdleInterval, confirmReminderInterval

	target, credential, _ := strings.Cut(link, "#")
	if target == "" || credential == "" {
		// A fragmentless link names nothing: the credential IS what
		// identifies the session, so there is no address left to dial
		// without it. Refused here rather than dialled, since a server
		// applying the same rule would refuse it anyway, less clearly.
		return nil, fmt.Errorf("link must carry its #-delimited credential — pass the link exactly as it was issued")
	}
	if opts.ReconnectSecret == "" {
		return nil, fmt.Errorf("reconnectSecret is required")
	}
	if opts.AgePublicKey != "" && !agekey.Valid(opts.AgePublicKey) {
		return nil, fmt.Errorf("agePublicKey is not a validly formatted age public key")
	}

	// A header value carrying a control character does not travel as
	// written: it produces a request the server cannot parse, which comes
	// back as an unexplained refusal rather than as anything naming the
	// cause. Free text gets the control characters stripped, the way a
	// server would strip them anyway. A credential is refused instead —
	// silently mangling one turns a typo into an authentication failure,
	// which is a far worse thing to debug than being told the value is
	// malformed.
	for field, value := range map[string]string{
		"link credential": credential,
		"reconnectSecret": opts.ReconnectSecret,
		"agentId":         opts.AgentID,
		"createToken":     opts.CreateToken,
	} {
		if strings.ContainsFunc(value, unicode.IsControl) {
			return nil, fmt.Errorf("%s contains a control character, which cannot be sent in a request header", field)
		}
	}

	header := http.Header{}
	header.Set("Authorization", "Bearer "+credential)
	header.Set("Agent-Secret", opts.ReconnectSecret)
	header.Set("Hub-Protocol-Version", strconv.Itoa(wire.ProtocolVersion))
	if opts.AgentID != "" {
		header.Set("Agent-Id", opts.AgentID)
	}
	if name := sanitize.Text(opts.Name, maxNameRunes); name != "" {
		header.Set("Agent-Name", name)
	}
	if opts.AgePublicKey != "" {
		header.Set("Agent-Age-Public-Key", opts.AgePublicKey)
	}
	if opts.CreateToken != "" {
		header.Set("Hub-Create-Token", opts.CreateToken)
	}
	if topic := sanitize.Text(opts.Topic, maxTopicRunes); topic != "" {
		header.Set("Hub-Topic", topic)
	}

	ws, err := dialWS(target, header)
	if err != nil {
		return nil, err
	}
	return finishHandshake(ws, snapPongWait, snapWriteWait, snapAckIdleInterval, snapConfirmReminderInterval)
}

// dialWS opens the WebSocket and, when the server answers with anything
// other than an upgrade, names the status it actually sent. The library
// reports every non-101 response as the same opaque "bad handshake", which
// makes a deliberate refusal — 403 for a credential a server won't honour,
// 404 for a session it doesn't have — indistinguishable from an
// unreachable host. Those need different fixes, so the caller has to be
// able to tell them apart.
func dialWS(target string, header http.Header) (*websocket.Conn, error) {
	ws, resp, err := websocket.DefaultDialer.Dial(target, header)
	if err == nil {
		return ws, nil
	}
	if resp == nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if said := strings.TrimSpace(string(body)); said != "" {
		return nil, fmt.Errorf("%w — server answered %s: %s", err, resp.Status, said)
	}
	return nil, fmt.Errorf("%w — server answered %s", err, resp.Status)
}

// finishHandshake reads the server's initial "joined" message off an
// already-connected ws, validates it, and builds the running Conn —
// the tail shared by Dial and DialRelay, which differ only in how they
// reach an open *websocket.Conn (a normalized host+sessionId URL with no
// custom headers, vs. an arbitrary caller-supplied URL with an
// Authorization/etc. header). Nothing downstream of here differs between
// the two: everything behavioural comes from the server's declared
// features, never from which of them dialled.
func finishHandshake(ws *websocket.Conn, snapPongWait, snapWriteWait, snapAckIdleInterval, snapConfirmReminderInterval time.Duration) (*Conn, error) {
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
		ws:                      ws,
		peerID:                  joined.PeerID,
		name:                    joined.Name,
		agePublicKey:            joined.AgePublicKey,
		serverVersion:           joined.ServerVersion,
		expectedPeers:           joined.PeerCount,
		peers:                   make(map[string]PeerInfo),
		pongWait:                snapPongWait,
		canSend:                 joined.CanSend,
		conversationKind:        joined.ConversationKind,
		topic:                   joined.Topic,
		behind:                  joined.Behind,
		behindSince:             joined.BehindSince,
		features:                joined.Features,
		featuresDeclared:        joined.Features != nil,
		lastFrameKind:           "joined",
		lastFrameAt:             time.Now(),
		ackIdleInterval:         snapAckIdleInterval,
		confirmReminderInterval: snapConfirmReminderInterval,
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
	go c.confirmReminderLoop()
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

// FeaturesDeclared reports whether the server sent a Features object at
// all (see wire.Joined.Features) — false for a pre-v3 server, where
// nothing is known about individual capabilities either way and a
// runtime fallback (e.g. ConfirmReceived's ackReplyMisses probe) is the
// only way to find out.
func (c *Conn) FeaturesDeclared() bool { return c.featuresDeclared }

// HasFeature reports whether the server explicitly declared support for
// the named feature — meaningless (always false) when FeaturesDeclared
// is false, since a pre-v3 server has said nothing either way.
func (c *Conn) HasFeature(name string) bool {
	_, ok := c.features[name]
	return ok
}

// ExpectedPeerCount is how many peers were already in the session at join
// time, as reported by the server's "joined" message — i.e. how many
// peerJoined events make up the initial roster catch-up.
func (c *Conn) ExpectedPeerCount() int { return c.expectedPeers }

// The accessors below only ever return a non-zero/non-nil value for a
// teams session (see wire.Joined) — nil/zero for every
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
func (c *Conn) Topic() *string      { return c.topic }

// WantsActionAcks reports whether this server answers a write action
// (send/react/edit/delete) with its own asynchronous sendAck/reactionAck/
// editAck/deleteAck, making claimNextAck worth using: waiting for one from
// a server that never sends any is a pure timeout tax on every send.
//
// Answered only from the server's own declaration — the "actionAcks"
// feature. No fallback to how the connection was dialled, and no runtime
// probe: a server that declares nothing gets the same answer as one that
// declares it doesn't do this, because a client cannot honestly
// distinguish "won't" from "can't" without being told.
//
// A DIFFERENT feature from "ackReplies", which covers a standalone
// hub_confirm ack's Behind count. A server can answer those while sending
// no per-action acks at all, so the two must not be conflated.
func (c *Conn) WantsActionAcks() bool { return c.HasFeature("actionAcks") }

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
			debugf("tryDivertToClaimLocked: matched claim=%p kind=%q cursor=%q answers=%+v",
				claim, ev.Kind, ev.Cursor, ev.Answers)
			claim.result <- ev
			return true
		default:
			// A pending MessageAfter claim exists but this event doesn't
			// match it — e.g. a live "msg" with no Answers arriving while
			// a gap/catch-up walk is waiting. Logged because the claim
			// falling through here, unresolved, is exactly the failure
			// mode under investigation 2026-09-07 (a MessageAfter answer
			// silently not reaching its waiter) if it ever fires for an
			// event that was actually meant to be that answer.
			debugf("tryDivertToClaimLocked: pending claim=%p NOT matched by kind=%q cursor=%q answers=%+v — falling through to buffer",
				c.pendingMessageAfter, ev.Kind, ev.Cursor, ev.Answers)
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
		// A caller that wants this specific reply (currently only
		// ConfirmReceived, to surface a server's optional Behind count —
		// added 2026-09-08) registers a claim first via claimNextAck.
		// The bookkeeping above still applies unconditionally either
		// way; only the delivery differs — a claimed reply falls through
		// to tryDivertToClaimLocked below instead of being discarded
		// here, same as every other ack kind's claim path. ackLoop's own
		// background standalone acks never register a claim, so they
		// keep being silently discarded exactly as before.
		if _, claimed := c.pendingAcks["ack"]; claimed {
			return false
		}
		return true
	}
	if ev.Kind == "error" && (ev.Code == "bad_ack" || ev.Code == "bad_ack_cursor") {
		// bad_ack/bad_ack_cursor are specific to the ack subsystem — unlike
		// the generic bad_cursor/bad_request a teams relay may also use
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

// ConfirmReceived sends an immediate standalone read receipt (wire.Ack)
// for cursor and marks it as this connection's consumed boundary — the
// same lastConsumed/lastAckSent state ackLoop's idle timer and Send's
// piggybacking already read (see MarkConsumed's doc comment). Unlike
// those, which a caller drives from its own drain, this is a caller
// asserting the boundary directly: for mcp-hub-client, the model itself,
// via hub_confirm, since nothing else in this client's async delivery
// paths (wait --follow, one-shot wait) can honestly claim the model read
// anything. Sent immediately rather than waiting for ackLoop's idle tick
// — a caller invoking this wants the receipt to land now, not on the
// next timer.
//
// ackReplyMissThreshold bounds ackReplyMisses — see its own doc comment
// for why this is a streak, not a single miss: a transient slow/lost
// reply must not read as "this server never answers" for the rest of
// the connection's life. Var so tests can shorten the number of stalls
// they need to pay to exercise the ratchet, the same pattern
// ackIdleInterval/confirmReminderInterval already use for their own
// tunables.
var ackReplyMissThreshold = 2

// Returns the server's reported Behind count from its reply, if one
// arrives (see wire.Ack.Behind's doc comment for why this count is worth
// having: it's measured from the position the model itself just
// asserted, not one inherited from connect time). nil, nil is a normal,
// expected result — a server that doesn't answer standalone acks is not
// an error.
func (c *Conn) ConfirmReceived(cursor string) (*int, error) {
	c.MarkConsumed([]Event{{Cursor: cursor}})
	// The server's own declaration (wire.Joined.Features) settles this
	// whenever it's available: never infer a capability from silence at
	// runtime. A declared "no" is certain for this connection's life, so
	// it skips waiting without probing at all; a declared "yes" always
	// waits, since the server has promised a reply. Only an UNDECLARED
	// Features map — nothing known either way — falls back to
	// ackReplyMisses's probe-and-ratchet.
	var skipWait bool
	if c.featuresDeclared {
		skipWait = !c.HasFeature("ackReplies")
	} else {
		c.mu.Lock()
		skipWait = c.ackReplyMisses >= ackReplyMissThreshold
		c.mu.Unlock()
	}
	if skipWait {
		if err := c.ws.WriteJSON(wire.NewAck(cursor)); err != nil {
			return nil, err
		}
		c.mu.Lock()
		c.lastAckSent = cursor
		c.mu.Unlock()
		return nil, nil
	}
	resultCh, cancel := c.claimNextAck("ack")
	defer cancel()
	if err := c.ws.WriteJSON(wire.NewAck(cursor)); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.lastAckSent = cursor
	c.mu.Unlock()
	select {
	case ev := <-resultCh:
		c.mu.Lock()
		c.ackReplyMisses = 0
		c.mu.Unlock()
		return ev.Behind, nil
	case <-time.After(AckWaitTimeout):
		c.mu.Lock()
		c.ackReplyMisses++
		c.mu.Unlock()
		return nil, nil
	}
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

// confirmReminderLoop periodically checks whether anything live-delivered
// is still awaiting a genuine synchronous hand-over (liveUnconfirmed —
// see its field comment for why this can't be a comparison of
// lastSeenCursor against lastConsumed, and MarkConsumed's for why
// "hand-over" is a deliberately narrower signal than "written to a
// socket"), and if so, injects a client-generated reminder event asking
// the model to call hub_confirm. This is the only primitive that can
// close the delivery-vs-consumption gap: everything else (a timer alone,
// N+1-arrival, delivery hooks) observes this client's own writes, not
// whether a model actually read anything.
//
// Built as an ordinary buffered Event rather than a side channel
// specifically so it flows through the exact same delivery paths
// (wait --follow, hub_receive, hub_wait) as any other event — including
// FormatEventsBatch's own i/N markers when it happens to land bundled
// with others. Deliberately generated here, client-locally, and never
// from a wire frame: per the hub discussion's own security refinement, a
// line instructing the model to advance its own confirmed position must
// never be something a peer could produce, or it becomes silent loss
// reintroduced through the front door. Never carries its own Cursor —
// only Text, holding the cursor to suggest confirming — so this event is
// never itself mistaken for a confirmed hand-over by
// recordHandedOver/MarkConsumed, which key entirely off Event.Cursor.
func (c *Conn) confirmReminderLoop() {
	ticker := time.NewTicker(c.confirmReminderInterval)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		seen := c.lastSeenCursor
		if seen == "" || !c.liveUnconfirmed {
			c.mu.Unlock()
			continue
		}
		c.buffer = append(c.buffer, Event{
			Kind:             "confirmReminder",
			Text:             seen,
			UnconfirmedCount: c.unconfirmedCount,
			UnconfirmedSince: c.unconfirmedSince,
		})
		f := c.onActivity
		c.mu.Unlock()
		if f != nil {
			f()
		}
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
			debugf("readLoop: decodeEvent failed, dropping frame raw=%s", raw)
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
		if ev.PeerID == SystemPeerIDOperator || ev.PeerID == SystemPeerIDSystem {
			ev.IsOperator = true
		}
		c.buffer = append(c.buffer, ev)
		if ev.Cursor != "" {
			c.lastSeenCursor = ev.Cursor
			if !c.liveUnconfirmed {
				c.unconfirmedSince = time.Now()
			}
			c.liveUnconfirmed = true
			c.unconfirmedCount++
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
		// PeerID is normally a real peer's UUID, but a system/relay-level
		// notification (e.g. chat-relay's "chat renamed" event) can be a
		// "msg" with no PeerID at all — that is a legitimate, peerless
		// message, not a malformed one, and must not be confused with a
		// garbage non-UUID id, which IS rejected. Silently dropping the
		// peerless case here once caused a real bug: a MessageAfter answer
		// landing on exactly such a message never reached the pending
		// claim, so RequestMessageAfterAwaiting always timed out on it.
		if err := json.Unmarshal(raw, &m); err != nil || (m.PeerID != "" && !wire.IsValidID(m.PeerID)) {
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
		return Event{Kind: "ack", Cursor: a.AckCursor, ActionOK: a.OK, Behind: a.Behind}, true
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

func (c *Conn) Send(text string, attachments []wire.Attachment, format, replyTo string, mentions []wire.Mention) error {
	m := wire.NewOutgoingMsg(text)
	m.AckCursor = c.ackCursorForOutbound()
	m.Attachments = attachments
	m.Format = format
	m.ReplyTo = replyTo
	m.Mentions = mentions
	return c.ws.WriteJSON(m)
}

// SendTo sends text privately to a single peer, identified by peerId. The
// server processes this asynchronously: a delivery failure (e.g. an unknown
// or departed peer) does not surface as a returned error here, but as a
// buffered "error" event picked up by a later Peek/Drain.
func (c *Conn) SendTo(text, peerID string, attachments []wire.Attachment, format, replyTo string, mentions []wire.Mention) error {
	m := wire.NewOutgoingDirectedMsg(text, peerID)
	m.AckCursor = c.ackCursorForOutbound()
	m.Attachments = attachments
	m.Format = format
	m.ReplyTo = replyTo
	m.Mentions = mentions
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
	prev := c.pendingMessageAfter
	c.pendingMessageAfter = claim
	c.mu.Unlock()
	debugf("RequestMessageAfterAwaiting: anchor=%+v claim=%p replacing-prev=%v", anchor, claim, prev != nil)
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
		debugf("RequestMessageAfterAwaiting: claim=%p write error: %v", claim, err)
		return Event{}, false, err
	}
	select {
	case ev := <-ch:
		debugf("RequestMessageAfterAwaiting: claim=%p resolved kind=%q cursor=%q answers=%+v",
			claim, ev.Kind, ev.Cursor, ev.Answers)
		return ev, true, nil
	case <-time.After(AckWaitTimeout):
		debugf("RequestMessageAfterAwaiting: claim=%p timed out after %s waiting for anchor=%+v",
			claim, AckWaitTimeout, anchor)
		return Event{}, false, nil
	}
}

// React asks the server to add or remove a reaction on an earlier
// message, identified by externalID — see wire.Reaction. action is "add"
// or "remove". Not meaningful for mcp-hub-server, which silently ignores
// any message type it doesn't recognize; for a teams relay with write
// access to the underlying platform. Success/failure arrives
// asynchronously as a "reactionAck" (or an "error" event on refusal),
// picked up by a later Peek/Drain like anything else — this call only
// confirms the request was sent.
// requireAction refuses a write action the server has said nothing about
// supporting, before anything is written to the socket. Two fields answer
// two different questions: "reactions"/"edit"/"delete" say whether the
// request is worth SENDING, and "actionAcks" says whether an answer is
// worth WAITING for. Conflating them sends a request the server drops in
// silence and then waits out the full ack timeout for a reply that cannot
// come.
//
// A server that declares no features at all has said nothing either way,
// so the request goes out and the caller finds out from the answer — the
// same rule the rest of the feature vocabulary follows.
func (c *Conn) requireAction(feature string) error {
	if !c.featuresDeclared || c.HasFeature(feature) {
		return nil
	}
	return fmt.Errorf("this server does not support %s on this session (it declares no %q feature), so the request would be dropped without an answer", feature, feature)
}

func (c *Conn) React(externalID, reaction, action string) error {
	if err := c.requireAction("reactions"); err != nil {
		return err
	}
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
func (c *Conn) EditMessage(externalID, text string, attachments []wire.Attachment, format, replyTo string, mentions []wire.Mention) error {
	if err := c.requireAction("edit"); err != nil {
		return err
	}
	e := wire.NewEditRequest(externalID, text, attachments, format, replyTo, mentions)
	e.AckCursor = c.ackCursorForOutbound()
	return c.ws.WriteJSON(e)
}

// DeleteMessage asks the server to remove an earlier message, identified
// by externalID — see wire.Delete. Not meaningful for mcp-hub-server.
// Success/failure arrives asynchronously as a "deleteAck" (or an "error"
// event on refusal), like React/EditMessage.
func (c *Conn) DeleteMessage(externalID string) error {
	if err := c.requireAction("delete"); err != nil {
		return err
	}
	d := wire.NewDeleteRequest(externalID)
	d.AckCursor = c.ackCursorForOutbound()
	return c.ws.WriteJSON(d)
}

// AckWaitTimeout is how long SendAwaitingAck/ReactAwaitingAck/
// EditMessageAwaitingAck wait for their own ack (or a generic error)
// before giving up — generous relative to how fast an ack has actually
// been observed to arrive against a real teams relay (well under a
// second), while staying small enough that a caller mistakenly calling
// one of these against a server that declares no actionAcks wouldn't be
// worth blocking on for long even without the WantsActionAcks check
// these methods already do first.
var AckWaitTimeout = 5 * time.Second

// SendAwaitingAck sends text (broadcast, or to a single peer if to is
// non-empty) and, only against a server declaring actionAcks (see
// WantsActionAcks — a server that emits no ack at all would make waiting
// a pure timeout tax for no benefit), waits up to AckWaitTimeout for
// its own outcome via claimNextAck: a "sendAck" event on success, an
// "error" event on refusal.
//
// Returns (event, true, nil) if an outcome arrived in time — format it
// with FormatEvent to report it, whether success or failure, directly as
// the caller's own answer. Returns (Event{}, false, nil) if no ack is
// declared for this server (nothing to wait for; report success the way this
// always has, since the write itself is fire-and-forget either way) or
// if the wait timed out with no outcome yet — the write may still
// succeed or fail later, reported the normal way via wait/hub_receive/
// hub_wait, exactly as before this existed. Returns (Event{}, false, err)
// only if the write itself failed locally.
func (c *Conn) SendAwaitingAck(text, to string, attachments []wire.Attachment, format, replyTo string, mentions []wire.Mention) (Event, bool, error) {
	if !c.WantsActionAcks() {
		if to == "" {
			return Event{}, false, c.Send(text, attachments, format, replyTo, mentions)
		}
		return Event{}, false, c.SendTo(text, to, attachments, format, replyTo, mentions)
	}
	resultCh, cancel := c.claimNextAck("sendAck")
	defer cancel()
	var err error
	if to == "" {
		err = c.Send(text, attachments, format, replyTo, mentions)
	} else {
		err = c.SendTo(text, to, attachments, format, replyTo, mentions)
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

// ReactAwaitingAck is React, but — only when actionAcks is declared — waits
// up to AckWaitTimeout for its own "reactionAck"/"error" outcome. See
// SendAwaitingAck for the full contract; identical shape.
func (c *Conn) ReactAwaitingAck(externalID, reaction, action string) (Event, bool, error) {
	if !c.WantsActionAcks() {
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

// EditMessageAwaitingAck is EditMessage, but — only when actionAcks is
// connection — waits up to AckWaitTimeout for its own "editAck"/"error"
// outcome. See SendAwaitingAck for the full contract; identical shape.
func (c *Conn) EditMessageAwaitingAck(externalID, text string, attachments []wire.Attachment, format, replyTo string, mentions []wire.Mention) (Event, bool, error) {
	if !c.WantsActionAcks() {
		return Event{}, false, c.EditMessage(externalID, text, attachments, format, replyTo, mentions)
	}
	resultCh, cancel := c.claimNextAck("editAck")
	defer cancel()
	if err := c.EditMessage(externalID, text, attachments, format, replyTo, mentions); err != nil {
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

// DeleteMessageAwaitingAck is DeleteMessage, but — only when actionAcks is
// connection — waits up to AckWaitTimeout for its own "deleteAck"/"error"
// outcome. See SendAwaitingAck for the full contract; identical shape.
func (c *Conn) DeleteMessageAwaitingAck(externalID string) (Event, bool, error) {
	if !c.WantsActionAcks() {
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
// or teams-specific code path. Var so tests can shorten it.
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
// rather than FormatEvents' text-only rendering. Unlike Drain/DrainBatch's
// older behavior, this does NOT by itself mark anything as consumed for
// the read-receipt system — see MarkConsumed's doc comment for why that
// boundary moved out of every drain and into an explicit call a caller
// makes only once it's certain the model actually received the result
// (not merely that bytes left this process for a socket).
func (c *Conn) DrainEvents() (events []Event, connected bool) {
	c.mu.Lock()
	events = c.buffer
	c.buffer = nil
	connected = !c.closed
	c.mu.Unlock()
	return events, connected
}

// MarkConsumed records the given events' cursors as delivered to the
// model, for the read-receipt system (ack piggybacking, and ackLoop's
// idle-triggered standalone ack) — moved out of Drain/DrainBatch/
// DrainEvents themselves and into this explicit call, found live,
// 2026-09-07: those three are also what waiter's follow-mode delivery
// and one-shot `wait` use to write events to the wait socket, neither of
// which confirms a model ever read anything — only that this process
// wrote bytes onward. Marking "consumed" at drain time meant a standalone
// ack (and hence a teams relay's own delivery-tracking field, e.g.
// chat-relay's Behind) reported truthfully on "reached this process,"
// not on "reached the model," while claiming the latter. Call this only
// from a genuinely synchronous hand-over — an MCP tool result the model
// is about to receive directly, mirroring mcptools.Hub.recordHandedOver's
// identical reasoning and sharing its call site.
func (c *Conn) MarkConsumed(events []Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range events {
		if e.Cursor != "" {
			c.lastConsumed = e.Cursor
			c.liveUnconfirmed = false
			c.unconfirmedCount = 0
			c.unconfirmedSince = time.Time{}
		}
	}
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
