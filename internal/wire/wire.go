package wire

import (
	"encoding/json"
	"regexp"
)

var idPattern = regexp.MustCompile(
	`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`,
)

// IsValidID reports whether s is a well-formed sessionId/peerId (a standard
// UUID string). Both the server and the client must validate any id read off
// the wire before trusting it — a peerId in particular is fully
// server-controlled from the client's point of view, and is used both in
// filesystem paths (the wait socket) and rendered directly to the model, so
// an unvalidated value is a path-traversal and prompt-injection vector.
func IsValidID(s string) bool {
	return idPattern.MatchString(s)
}

type Type string

const (
	TypeJoined          Type = "joined"
	TypeError           Type = "error"
	TypeMsg             Type = "msg"
	TypePeerJoined      Type = "peerJoined"
	TypePeerLeft        Type = "peerLeft"
	TypeRosterComplete  Type = "rosterComplete"
	TypeHistory         Type = "history"
	TypeHistoryComplete Type = "historyComplete"
	TypeSendAck         Type = "sendAck"
	TypeReactionChanged Type = "reactionChanged"
	TypeMessageEdited   Type = "messageEdited"
	TypeReaction        Type = "reaction"
	TypeEdit            Type = "edit"
	TypeReactionAck     Type = "reactionAck"
	TypeEditAck         Type = "editAck"
	TypeDelete          Type = "delete"
	TypeDeleteAck       Type = "deleteAck"
	TypeMessageDeleted  Type = "messageDeleted"
	TypeAck             Type = "ack"
)

// ProtocolVersion identifies the wire protocol's schema. Bump it only for a
// genuinely breaking change — additive changes (new optional fields, new
// event kinds) don't need it, since both the server and hubconn already
// tolerate those: unknown JSON fields are silently ignored by
// encoding/json, and decodeEvent's default case silently drops any message
// with an unrecognized "type". A client that doesn't send a version at all
// (e.g. the "?v=" query param is absent) is treated as version 1 — that is
// the deliberate, permanent backward-compatible baseline, not a fallback
// that will later change meaning.
const ProtocolVersion = 1

type envelope struct {
	Type Type `json:"type"`
}

// DecodeType reads just the "type" field from a wire message.
func DecodeType(raw []byte) (Type, error) {
	var e envelope
	if err := json.Unmarshal(raw, &e); err != nil {
		return "", err
	}
	return e.Type, nil
}

type Joined struct {
	Type   Type   `json:"type"`
	PeerID string `json:"peerId"`
	// PeerCount is how many other peers were already in the session at the
	// moment this peer joined — i.e. how many peerJoined events (the
	// "roster") will follow. Computed atomically server-side so it can never
	// drift from what's actually delivered.
	PeerCount int `json:"peerCount"`
	// ServerVersion is this server's ProtocolVersion, so the client can tell
	// if it's behind and surface that to the model.
	ServerVersion int `json:"serverVersion"`
	// Name echoes back this peer's own display name after sanitization, so
	// the client can tell if anything was stripped/truncated from what it
	// requested. Empty if none was supplied.
	Name string `json:"name,omitempty"`
	// AgePublicKey echoes back this peer's own age public key. Always
	// exactly what was supplied (already format-validated pre-upgrade) or
	// empty if none was supplied.
	AgePublicKey string `json:"agePublicKey,omitempty"`

	// The fields below are for a bridge-style session (e.g. one reached via
	// teams_relay_connect) backed by a channel with real history and
	// send-permission policy — mcp-hub-server never sets any of them, since
	// none apply to an ordinary hub session.

	// LatestCursor is the cursor of the newest message the server
	// currently holds, or nil if there are none yet (a channel can exist
	// with nothing in it). Purely informational and opaque: it lets a
	// model that remembers an earlier cursor from a prior connection tell
	// whether it's behind without a speculative History request — nothing
	// on this side compares cursors automatically.
	LatestCursor *string `json:"latestCursor,omitempty"`
	// HistoryAfter advertises whether this server supports History.After —
	// forward paging, for a reconnecting client to fetch exactly what
	// arrived after the last cursor it saw, rather than paging backward
	// with Before (which can only reach older messages, never newer ones,
	// and so cannot fill a reconnect gap). False (the default, including
	// every mcp-hub-server) means only Before is supported; a client must
	// fall back to hub_history() with no cursor (the most recent page)
	// instead, which may not cover the whole gap for a long disconnect.
	HistoryAfter bool `json:"historyAfter,omitempty"`
	// HistoryLimitMax is the server's cap on a single History request's
	// limit — asking for more just returns this many, not an error. Zero
	// means the server didn't set one (including every mcp-hub-server).
	HistoryLimitMax int `json:"historyLimitMax,omitempty"`
	// CanSend reports whether sending is currently permitted in this
	// conversation. A snapshot at connect time, not a guarantee — it can
	// go stale mid-session (e.g. an external participant joins), so a
	// later send can still be refused even after this was true.
	CanSend bool `json:"canSend,omitempty"`
	// ConversationKind and Topic describe what was joined (e.g.
	// "oneOnOne"/"group"/"meeting", and a display name where one exists)
	// without needing a History request first. Empty/nil when not
	// applicable or not set by the server.
	ConversationKind string  `json:"conversationKind,omitempty"`
	Topic            *string `json:"topic,omitempty"`
}

func NewJoined(peerID string, peerCount int, name, agePublicKey string) Joined {
	return Joined{
		Type: TypeJoined, PeerID: peerID, PeerCount: peerCount, ServerVersion: ProtocolVersion,
		Name: name, AgePublicKey: agePublicKey,
	}
}

type Error struct {
	Type    Type   `json:"type"`
	Message string `json:"message"`
	// Code, if present, is a stable machine-readable reason a client can
	// branch on without parsing Message — e.g. a chat-relay-style bridge
	// distinguishing "invalid_credential" (never retry) from "unavailable"
	// (transient, retry is fine). Empty when a server doesn't set one;
	// mcp-hub-server itself doesn't today. Additive: an older client that
	// doesn't know this field still gets Message.
	Code string `json:"code,omitempty"`
	// Retryable, when Code is set, says whether retrying the operation that
	// produced this error could succeed. Meaningless without Code.
	Retryable bool `json:"retryable,omitempty"`
}

func NewError(message string) Error {
	return Error{Type: TypeError, Message: message}
}

type Msg struct {
	Type    Type   `json:"type"`
	PeerID  string `json:"peerId,omitempty"`
	Text    string `json:"text"`
	TS      string `json:"ts,omitempty"`
	To      string `json:"to,omitempty"`      // set by the client to request directed (private) delivery
	Private bool   `json:"private,omitempty"` // set by the server on a delivered directed message
	// Historical marks a msg delivered in answer to a History request
	// rather than live traffic — additive, so a client that doesn't know
	// the field just renders it as an ordinary message. mcp-hub-server
	// itself never sets this; it's for a bridge server (e.g. one backed by
	// a channel with real message history) answering History.
	Historical bool `json:"historical,omitempty"`
	// ExternalID, for a bridge session, is the sending server's own id for
	// this message (the same value given in a SendAck for the send that
	// produced it) — correlates a canonical msg with the sendAck that
	// preceded it. Empty when not applicable.
	ExternalID string `json:"externalId,omitempty"`
	// Own, for a bridge session, is true when this exact connection is
	// the one that sent the message. Deliberately a decision for the
	// receiving client to act on, not the server: whether to skip waking
	// on your own echoed send is policy, and different consumers of the
	// same stream (an agent, a UI, a hub client) may want different
	// answers. mcp-hub-server never sets this.
	Own bool `json:"own,omitempty"`
	// Cursor, for a bridge session, is this message's own opaque
	// position — the value a client passes back as History.Before to
	// page further back past it. mcp-hub-server never sets this, since it
	// has no history concept at all.
	Cursor string `json:"cursor,omitempty"`
	// AckCursor, set by the client, piggybacks a read receipt on this
	// message: "this is the cursor of the last event I've actually
	// consumed" — not merely received. See Ack for the standalone form and
	// the full read-receipt contract. Ignored by mcp-hub-server, which has
	// no history/read-receipt concept at all.
	AckCursor string `json:"ackCursor,omitempty"`
}

// NewOutgoingMsg is what a client sends to the server to broadcast to the
// whole session.
func NewOutgoingMsg(text string) Msg {
	return Msg{Type: TypeMsg, Text: text}
}

// NewOutgoingDirectedMsg is what a client sends to the server to deliver
// text privately to a single peer (identified by peerId).
func NewOutgoingDirectedMsg(text, to string) Msg {
	return Msg{Type: TypeMsg, Text: text, To: to}
}

// NewBroadcastMsg is what the server sends to other session members.
func NewBroadcastMsg(peerID, text, ts string) Msg {
	return Msg{Type: TypeMsg, PeerID: peerID, Text: text, TS: ts}
}

// NewDirectedMsg is what the server sends to the single targeted peer for a
// private message.
func NewDirectedMsg(peerID, text, ts string) Msg {
	return Msg{Type: TypeMsg, PeerID: peerID, Text: text, TS: ts, Private: true}
}

type PeerEvent struct {
	Type   Type   `json:"type"`
	PeerID string `json:"peerId"`
	// Name and AgePublicKey are that peer's own sanitized/validated values
	// from when it connected (see Joined). Both are empty if that peer
	// didn't supply them.
	Name         string `json:"name,omitempty"`
	AgePublicKey string `json:"agePublicKey,omitempty"`
}

func NewPeerJoined(peerID, name, agePublicKey string) PeerEvent {
	return PeerEvent{Type: TypePeerJoined, PeerID: peerID, Name: name, AgePublicKey: agePublicKey}
}

func NewPeerLeft(peerID string) PeerEvent {
	return PeerEvent{Type: TypePeerLeft, PeerID: peerID}
}

// RosterComplete is sent by the server to a newly joined peer once it has
// finished delivering that peer's initial roster (one peerJoined per
// existing peer) — it is always the last message from that delivery,
// emitted under the same lock as the roster itself, so it can never race
// ahead of or behind the events it's promising are complete.
type RosterComplete struct {
	Type Type `json:"type"`
}

func NewRosterComplete() RosterComplete {
	return RosterComplete{Type: TypeRosterComplete}
}

// History is a client request for messages relative to its own
// connection — not part of a normal mcp-hub session (peers only ever see
// events from when they joined forward), but meaningful for a bridge
// server backed by a channel with real retained history (e.g. a
// Teams-relay bridge). Exactly one of Before/After should be set (Before
// takes precedence if a server receives both, but a well-behaved client
// never sends both at once); both are server-defined opaque cursors,
// exclusive of the boundary message itself.
//
// Before pages backward: omitted (with After also empty) means "the most
// recent Limit messages"; otherwise the page ends strictly before it, so a
// client pages further back by repeatedly passing the oldest cursor it has
// seen. This can only reach older messages — never newer ones — so it
// cannot be used to fill a reconnect gap.
//
// After pages forward, strictly after the given cursor — the page starts
// immediately past it, so a client resuming after a disconnect gets
// exactly what arrived while it was away. Only meaningful when the server
// advertises support via Joined.HistoryAfter; a server that doesn't
// support it should be sent Before (or nothing) instead.
//
// Limit may be capped server-side to less than requested — a server that
// does so should advertise the cap elsewhere (e.g. on Joined) so a client
// isn't left guessing why a page came back smaller than asked.
type History struct {
	Type   Type   `json:"type"`
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`
	Limit  int    `json:"limit"`
	// AckCursor piggybacks a read receipt — see Msg.AckCursor.
	AckCursor string `json:"ackCursor,omitempty"`
}

func NewHistoryRequest(before string, limit int) History {
	return History{Type: TypeHistory, Before: before, Limit: limit}
}

// NewHistoryAfterRequest builds a forward-paging History request — see
// History.After. Only meaningful against a server that set
// Joined.HistoryAfter true.
func NewHistoryAfterRequest(after string, limit int) History {
	return History{Type: TypeHistory, After: after, Limit: limit}
}

// HistoryComplete is sent once a History request's answering burst of msg
// events (each carrying Historical: true) has been fully delivered —
// including an empty burst, so a client at the start of a conversation
// gets a positive "there is no more" rather than inferring completion from
// a gap in traffic. Mirrors RosterComplete's role for the initial roster.
type HistoryComplete struct {
	Type Type `json:"type"`
}

func NewHistoryComplete() HistoryComplete {
	return HistoryComplete{Type: TypeHistoryComplete}
}

// Ack is a standalone read receipt — the same information Msg/Reaction/
// Edit/Delete/History.AckCursor piggyback, sent on its own when nothing
// else is about to go out anyway (see hubconn's idle-ack timer). Also
// doubles as the server's reply shape to either form: OK true confirms the
// cursor was accepted; OK false means it was behind what the server
// already holds, and AckCursor in that reply is the server's actual
// position, not an echo of what was sent — the client should adopt it
// rather than retry. A malformed or missing cursor is refused as a plain
// Error (code "bad_ack_cursor"/"bad_ack" — ack-subsystem-specific, not the
// generic "bad_cursor"/"bad_request" other request kinds may also use,
// since error events carry no correlation id and a generic code couldn't
// be attributed to the ack that caused it) instead of an Ack reply, since
// that's a protocol violation rather than a stale-but-valid receipt.
type Ack struct {
	Type      Type   `json:"type"`
	AckCursor string `json:"ackCursor,omitempty"`
	OK        bool   `json:"ok,omitempty"`
}

func NewAck(ackCursor string) Ack {
	return Ack{Type: TypeAck, AckCursor: ackCursor}
}

// SendAck confirms a send reached its destination, sent immediately —
// before the canonical message comes back through whatever async delivery
// path a bridge server uses to fan a sent message back out to connections
// (which can be arbitrarily delayed, e.g. a slow polling/reconciliation
// mode). Without this, a client has no way to distinguish "the send is
// still in flight" from "it silently failed" during that gap. Deliberately
// not a msg itself — the canonical message still arrives exactly once,
// later, with a real cursor; this only answers "did it leave the
// building?". ExternalID is the sending server's own id for the sent
// message (opaque to this client), correlating this ack with the later
// canonical message when it arrives. mcp-hub-server never sends this,
// since a plain hub_send already completes synchronously.
type SendAck struct {
	Type       Type   `json:"type"`
	ExternalID string `json:"externalId,omitempty"`
	OK         bool   `json:"ok"`
}

// ReactionChanged reports a reaction added to or removed from an earlier
// message — by anyone, on any bridge session; not something a receiving
// client requested. mcp-hub-server never sends this; a bridge server does,
// as part of a real chat platform's normal activity. Reaction and Label
// are both open strings, not a closed enum: Microsoft Teams' own reaction
// set has changed over time (confirmed from live chat-relay data
// containing "Eyes" and "Question mark" alongside the classic "Like"), so
// a client must not reject or normalize an unrecognized value — whatever
// the source platform reports is authoritative. PeerID may be empty: a
// removal is detected by diffing the reaction set on the message, and if
// the person who removed it is no longer identifiable from that diff, the
// event still fires (the removal itself is real information) with PeerID
// simply absent, rather than being suppressed for lack of full attribution.
type ReactionChanged struct {
	Type       Type   `json:"type"`
	ExternalID string `json:"externalId"`
	PeerID     string `json:"peerId,omitempty"`
	Reaction   string `json:"reaction"`
	Label      string `json:"label,omitempty"`
	// Action is "add" or "remove".
	Action string `json:"action"`
	TS     string `json:"ts,omitempty"`
	// Own is true when this exact connection made the reaction change —
	// see wire.Msg.Own for the identical reasoning (a client's own action
	// isn't news to itself).
	Own bool `json:"own,omitempty"`
}

// MessageEdited reports that an earlier message's content changed — by
// anyone, not something a receiving client requested. mcp-hub-server never
// sends this. Text is the same simplified/rendered form a Msg carries, so
// the two are directly comparable. The message's own cursor (see History)
// does not change on an edit — a client locates the message it already
// has by ExternalID, not by a new cursor. Own is true only when this
// connection made the edit; per platform rules (a sender may only edit
// their own messages) this can currently only ever be true for a
// connection's own prior send, and may be permanently false if the
// sending server has no write access to make edits at all.
type MessageEdited struct {
	Type       Type   `json:"type"`
	ExternalID string `json:"externalId"`
	Text       string `json:"text"`
	TS         string `json:"ts,omitempty"`
	Own        bool   `json:"own,omitempty"`
}

// Reaction is a client request to add or remove a reaction on an earlier
// message, identified by ExternalID (the same id a Msg or SendAck
// carries). Action is "add" or "remove". Reaction is an open string — see
// wire.ReactionChanged.Reaction for why — a receiving server is the
// authority on whether a value is valid, not this package; an invalid
// value comes back as an ordinary error event, not a client-side
// rejection. Not meaningful for mcp-hub-server, which has nothing to
// react to; for a bridge server with write access to the underlying
// platform.
type Reaction struct {
	Type       Type   `json:"type"`
	ExternalID string `json:"externalId"`
	Reaction   string `json:"reaction"`
	Action     string `json:"action"`
	// AckCursor piggybacks a read receipt — see Msg.AckCursor.
	AckCursor string `json:"ackCursor,omitempty"`
}

func NewReactionRequest(externalID, reaction, action string) Reaction {
	return Reaction{Type: TypeReaction, ExternalID: externalID, Reaction: reaction, Action: action}
}

// Edit is a client request to change an earlier message's content,
// identified by ExternalID. Not meaningful for mcp-hub-server; for a
// bridge server with write access, and then only ever for a message that
// server itself is able to edit (e.g. a platform's "you may only edit
// your own messages" rule) — a server is the authority on whether an edit
// is permitted, not this package.
type Edit struct {
	Type       Type   `json:"type"`
	ExternalID string `json:"externalId"`
	Text       string `json:"text"`
	// AckCursor piggybacks a read receipt — see Msg.AckCursor.
	AckCursor string `json:"ackCursor,omitempty"`
}

func NewEditRequest(externalID, text string) Edit {
	return Edit{Type: TypeEdit, ExternalID: externalID, Text: text}
}

// ReactionAck and EditAck confirm a Reaction/Edit request was actually
// carried out — the same reasoning as SendAck: without an explicit
// confirmation, a client can't tell "still in flight" from "silently
// failed." A refusal (e.g. policy, or an edit Graph won't permit) arrives
// as an ordinary error event instead of one of these with OK false — OK
// is expected to always be true when either of these is sent at all;
// kept as a field rather than assumed for symmetry with SendAck and in
// case a server ever has a reason to send a negative ack explicitly.
type ReactionAck struct {
	Type       Type   `json:"type"`
	ExternalID string `json:"externalId,omitempty"`
	Reaction   string `json:"reaction,omitempty"`
	Action     string `json:"action,omitempty"`
	OK         bool   `json:"ok"`
}

type EditAck struct {
	Type       Type   `json:"type"`
	ExternalID string `json:"externalId,omitempty"`
	OK         bool   `json:"ok"`
}

// Delete is a client request to remove an earlier message, identified by
// ExternalID. Not meaningful for mcp-hub-server. Deliberately a distinct
// request rather than Edit with empty text: a deletion and an edit to
// nothing look identical in the underlying platform payload, but a client
// that conflated them would render an empty message where the platform
// renders a tombstone — this keeps that distinction on the wire instead
// of asking every client to reconstruct it.
type Delete struct {
	Type       Type   `json:"type"`
	ExternalID string `json:"externalId"`
	// AckCursor piggybacks a read receipt — see Msg.AckCursor.
	AckCursor string `json:"ackCursor,omitempty"`
}

func NewDeleteRequest(externalID string) Delete {
	return Delete{Type: TypeDelete, ExternalID: externalID}
}

type DeleteAck struct {
	Type       Type   `json:"type"`
	ExternalID string `json:"externalId,omitempty"`
	OK         bool   `json:"ok"`
}

// MessageDeleted reports that an earlier message was removed — by
// anyone, not something a receiving client requested. mcp-hub-server
// never sends this. Cursor is the deleted message's own position, the
// same value it would have carried on Msg — included so a client
// rendering a transcript can place the tombstone correctly without
// needing to have seen the original message first. Own is true only when
// this connection performed the deletion.
type MessageDeleted struct {
	Type       Type   `json:"type"`
	ExternalID string `json:"externalId"`
	Cursor     string `json:"cursor,omitempty"`
	TS         string `json:"ts,omitempty"`
	Own        bool   `json:"own,omitempty"`
}
