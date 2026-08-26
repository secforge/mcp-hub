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
	TypeJoined     Type = "joined"
	TypeError      Type = "error"
	TypeMsg        Type = "msg"
	TypePeerJoined Type = "peerJoined"
	TypePeerLeft   Type = "peerLeft"
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
}

func NewJoined(peerID string, peerCount int) Joined {
	return Joined{Type: TypeJoined, PeerID: peerID, PeerCount: peerCount, ServerVersion: ProtocolVersion}
}

type Error struct {
	Type    Type   `json:"type"`
	Message string `json:"message"`
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
}

func NewPeerJoined(peerID string) PeerEvent {
	return PeerEvent{Type: TypePeerJoined, PeerID: peerID}
}

func NewPeerLeft(peerID string) PeerEvent {
	return PeerEvent{Type: TypePeerLeft, PeerID: peerID}
}
