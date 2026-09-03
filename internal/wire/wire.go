package wire

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
	TypeHistoryBegin    Type = "historyBegin"
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
	TypeAttachment      Type = "attachment"
	TypeAttachmentData  Type = "attachmentData"
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
	// SystemPeerID, if set, is the peerId a server uses for its own
	// operator/system-originated messages on this session (chat-relay:
	// always the all-zeros UUID, but a client must not hardcode that —
	// this field is the authoritative source, and the value is otherwise
	// only a server-side convention). Every peerId is server-assigned —
	// no inbound client frame ever carries one — so a msg/messageEdited
	// whose PeerID equals this one is reliably the server's own operator
	// channel, not something a peer could spoof by claiming the same id.
	// Empty when a server has no such concept (including every
	// mcp-hub-server, where every peerId is an ordinary participant).
	SystemPeerID string `json:"systemPeerId,omitempty"`
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

// Mention is one @-mention on a Msg/MessageEdited — see Msg.Mentions.
type Mention struct {
	Name string `json:"name,omitempty"`
	ID   string `json:"id"`
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
	// ReplyTo is the ExternalID of the message this one is a reply to — a
	// server extension (chat-relay), absent (not null/empty) when this
	// message isn't a reply. Same id space as ExternalID/SendAck, so it's
	// directly usable as the target of a Reaction/Edit/Delete on the
	// quoted message, not just a display reference. Set by the server on
	// a delivered msg/messageEdited (see below); a client may also set it
	// on an outgoing send to request a native threaded-reply citation —
	// see Edit.ReplyTo's doc comment for the validation a server should
	// apply in that direction (refuse an unrecognized/foreign id outright
	// rather than send anything, since resolving the citation can surface
	// that other message's own preview text).
	ReplyTo string `json:"replyTo,omitempty"`
	// ReplyPreview is the server's own (lossy — formatting flattened,
	// possibly truncated) abbreviation of the quoted message's text, for
	// when ReplyTo names a message outside a client's own history. Not
	// the authoritative quoted text — use History around ReplyTo's cursor
	// for that. Text on the same server may already open with a
	// human-readable "(in reply to X: preview)" line for plain-text-only
	// readers; ReplyTo/ReplyPreview are the structural form of that same
	// information, so a client surfacing both may want to avoid saying it
	// twice.
	ReplyPreview string `json:"replyPreview,omitempty"`
	// Mentions lists who this message @-mentions, if any — a server
	// extension (chat-relay), absent (nil, not an empty slice) when the
	// message mentions no one. Each entry's ID is the sending platform's
	// own directory id (opaque here — this package doesn't validate its
	// form, same as PeerID/ExternalID), not a hub peerId; there is no
	// wire-level way to resolve one into the other.
	Mentions []Mention `json:"mentions,omitempty"`
	// MentionedMe is true when the receiving connection's own identity is
	// among Mentions — computed per conversation (all connections on the
	// same conversation share one identity there), never per hub peerId,
	// so a client never needs to know its own directory id to use this.
	// mcp-hub-server never sets it.
	MentionedMe bool `json:"mentionedMe,omitempty"`
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
	// Attachments carries inline binary content — see Attachment. A
	// server extension (not part of the base protocol, hence "unknown
	// fields ignored" keeps mcp-hub-server's own relay unaffected either
	// way, same as AckCursor). On an edit (see Edit/MessageEdited), an
	// absent Attachments must be read as "unchanged", never "remove them"
	// — there is deliberately no way to express attachment removal here.
	Attachments []Attachment `json:"attachments,omitempty"`
	// Format is a server extension (chat-relay) declaring how Text should
	// be interpreted: "text" (the default if omitted — plain, escaped
	// verbatim) or "html" (bold/lists/code/quotes/tables/links, sanitized
	// server-side through the same allowlist used to render it — scripts,
	// event handlers, styles, iframes, and off-host images are stripped).
	// An unrecognized value is refused outright by a server that validates
	// it, not silently downgraded to "text" — so don't guess a value the
	// target server wasn't confirmed to accept. mcp-hub-server's own relay
	// has no opinion on this field at all, same as Attachments.
	Format string `json:"format,omitempty"`
}

// Attachment is binary content attached to a Msg/Edit — a server
// extension, currently images only by convention (image/png, image/jpeg,
// image/gif, image/webp; a server may refuse anything else). Two shapes
// share this one struct, distinguished by which fields are set:
//
//   - Sent by a client, and as mcp-hub-server's own relay delivers it:
//     ContentType + ContentBytes (base64-encoded raw bytes) inline, no
//     Token. Servers that enforce a size cap generally do so on the raw
//     byte size (8MB is the number chat-relay settled on) — the wire/JSON
//     size after base64 inflation (~33%) and envelope overhead is
//     implementation detail a client shouldn't need to reason about, so
//     check raw size client-side before encoding, not the encoded
//     string's length.
//   - Delivered by a reference-style server (chat-relay): Token +
//     ContentType (+ optional Name/Kind), ContentBytes empty — see
//     IsReference. The actual bytes are fetched on demand by sending an
//     AttachmentRequest for Token and reading back an AttachmentData
//     reply. A server does this so no peer pays for bytes it doesn't
//     want, one send doesn't fan out N copies, and a live message stays
//     identical to the same message replayed from history — and because
//     some content (e.g. a Teams-relayed image, fetched from Graph on
//     demand) never exists as inline bytes in a msg at all.
type Attachment struct {
	ContentType  string `json:"contentType"`
	ContentBytes string `json:"contentBytes,omitempty"`
	// Token identifies this attachment for a later AttachmentRequest —
	// set only on the reference form (see IsReference), never sent by a
	// client attaching its own content.
	Token string `json:"token,omitempty"`
	// Name is an optional original filename. Always present on the
	// reference form when the server has one; also settable on the
	// inline/outgoing form by a client sending a generic file (see
	// ReadFileAttachment/NewFileAttachmentFromData) — a receiving server
	// or client with no use for it just ignores it, same as any other
	// additive field.
	Name string `json:"name,omitempty"`
	// Kind is an optional server-defined category (e.g. "image"),
	// reference form only — informational, not required to interpret
	// ContentType.
	Kind string `json:"kind,omitempty"`
}

// IsReference reports whether this Attachment is the reference form (a
// Token to fetch, no inline bytes yet) rather than inline content.
func (a Attachment) IsReference() bool {
	return a.Token != "" && a.ContentBytes == ""
}

// AttachmentRequest asks a reference-style server (see Attachment) for the
// actual bytes behind a Token seen on an earlier Msg/MessageEdited. Not
// meaningful against mcp-hub-server's own relay, which always inlines
// bytes and never emits a Token to fetch in the first place — sending
// this against it just gets silently ignored, same as any other
// unrecognized type.
type AttachmentRequest struct {
	Type  Type   `json:"type"`
	Token string `json:"token"`
}

func NewAttachmentRequest(token string) AttachmentRequest {
	return AttachmentRequest{Type: TypeAttachment, Token: token}
}

// AttachmentData is the server's reply to an AttachmentRequest — the
// actual bytes for Token. A request that can't be satisfied comes back as
// an ordinary Error instead, with Code one of "bad_attachment" (malformed
// token), "not_found" (unknown, or belongs to a different session), or
// "unavailable" (recorded but no servable bytes, e.g. a recode failure).
type AttachmentData struct {
	Type        Type   `json:"type"`
	Token       string `json:"token"`
	Name        string `json:"name,omitempty"`
	ContentType string `json:"contentType"`
	// ContentBytes is base64-encoded raw bytes — the recoded copy a
	// reference-style server actually stores and serves, which may differ
	// in ContentType from whatever was originally sent (e.g. an upstream
	// platform's own re-encode).
	ContentBytes string `json:"contentBytes"`
}

// MaxAttachmentRawBytes is the raw (pre-base64) size cap a sending client
// should enforce before ever encoding a file — see Attachment's doc
// comment on why this is checked against raw bytes, not the inflated wire
// size. Shared by every send path (mcptools, httpmcp) so the limit can't
// drift between them. 32MB, coordinated live with chat-relay when it
// raised its own limit from 8MB for the same reason (arbitrary binary
// attachments, not just images) — its own two caps (per-attachment raw,
// whole-frame after base64+JSON overhead) live in one place on its side
// specifically so they can't disagree the way an earlier 1MB-vs-12MB split
// once did there; matching its number here is the same discipline applied
// across servers, not just within one.
const MaxAttachmentRawBytes = 32 * 1024 * 1024

// AllowedAttachmentContentTypes are the only content types the attachment
// extension accepts — images only, matching what chat-relay refuses
// server-side with error/unsupported_media. Checking client-side too gives
// a faster, clearer error than a round trip just to be told no.
var AllowedAttachmentContentTypes = map[string]bool{
	"image/png":  true,
	"image/jpeg": true,
	"image/gif":  true,
	"image/webp": true,
}

// AttachmentContentTypeForExt maps a file extension (case-insensitive,
// with or without a leading dot) to its content type, for a client that
// reads a local image file and needs to derive contentType rather than
// being told it directly.
func AttachmentContentTypeForExt(ext string) (string, bool) {
	switch strings.ToLower(strings.TrimPrefix(ext, ".")) {
	case "png":
		return "image/png", true
	case "jpg", "jpeg":
		return "image/jpeg", true
	case "gif":
		return "image/gif", true
	case "webp":
		return "image/webp", true
	default:
		return "", false
	}
}

// ReadAttachmentFile reads path from the local filesystem, validates its
// extension and raw size against AttachmentContentTypeForExt/
// MaxAttachmentRawBytes, and returns a single-element Attachment slice
// ready to send — the shared implementation behind every "imagePath"-style
// send-tool parameter (mcptools, httpmcp), so the rules can't drift between
// them. Returns a nil slice, nil error for an empty path (no attachment
// requested — not an error).
func ReadAttachmentFile(path string) ([]Attachment, error) {
	if path == "" {
		return nil, nil
	}
	contentType, ok := AttachmentContentTypeForExt(filepath.Ext(path))
	if !ok {
		return nil, fmt.Errorf("unsupported image type — only .png, .jpg/.jpeg, .gif, .webp are accepted")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not read imagePath: %w", err)
	}
	if len(data) > MaxAttachmentRawBytes {
		return nil, fmt.Errorf("image too large: %d bytes exceeds the %d byte limit", len(data), MaxAttachmentRawBytes)
	}
	return []Attachment{{
		ContentType:  contentType,
		ContentBytes: base64.StdEncoding.EncodeToString(data),
	}}, nil
}

// NewAttachmentFromData validates and wraps client-supplied base64 image
// bytes directly — the shape a remote sender (no local filesystem to read
// a path from, unlike ReadAttachmentFile) must use instead. Returns a
// nil slice, nil error if data is empty (no attachment requested).
func NewAttachmentFromData(data, contentType string) ([]Attachment, error) {
	if data == "" {
		return nil, nil
	}
	if !AllowedAttachmentContentTypes[contentType] {
		return nil, fmt.Errorf(
			"unsupported image type — only image/png, image/jpeg, image/gif, image/webp are accepted")
	}
	raw, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf("imageData is not valid base64: %w", err)
	}
	if len(raw) > MaxAttachmentRawBytes {
		return nil, fmt.Errorf("image too large: %d bytes exceeds the %d byte limit", len(raw), MaxAttachmentRawBytes)
	}
	return []Attachment{{ContentType: contentType, ContentBytes: data}}, nil
}

// ReadFileAttachment is ReadAttachmentFile without the images-only
// restriction — for a hub-to-hub session (mcp-hub-server's own relay,
// which never validates attachment content types at all) where a client
// wants to send an arbitrary binary file, not just an image. Content type
// is derived from the file's extension via the standard library's mime
// package (registered per-OS/per-install, so results can vary — this is
// a best-effort label for the receiving side to act on, not a guarantee),
// falling back to "application/octet-stream" for an unrecognized or
// missing extension rather than refusing the send outright: unlike the
// images-only path (where an unrecognized extension usually means "this
// isn't an image, don't pretend it is"), a generic byte blob is exactly
// as sendable without a specific label as with a wrong one. The original
// filename is preserved in Attachment.Name so the receiving side can
// offer a better name than a generic one. Same MaxAttachmentRawBytes cap
// as the images-only path — this is not a bigger-file allowance, just a
// broader content-type one. Returns a nil slice, nil error for an empty
// path (no attachment requested — not an error).
func ReadFileAttachment(path string) ([]Attachment, error) {
	if path == "" {
		return nil, nil
	}
	contentType := mime.TypeByExtension(filepath.Ext(path))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not read filePath: %w", err)
	}
	if len(data) > MaxAttachmentRawBytes {
		return nil, fmt.Errorf("file too large: %d bytes exceeds the %d byte limit", len(data), MaxAttachmentRawBytes)
	}
	return []Attachment{{
		ContentType:  contentType,
		ContentBytes: base64.StdEncoding.EncodeToString(data),
		Name:         filepath.Base(path),
	}}, nil
}

// NewFileAttachmentFromData is NewAttachmentFromData without the
// images-only restriction — see ReadFileAttachment for why, and for the
// same reasoning behind not refusing an unrecognized/empty contentType
// outright (a generic byte blob is exactly as sendable unlabeled as
// mislabeled). name is optional and, if given, preserved in
// Attachment.Name for the receiving side. Returns a nil slice, nil error
// if data is empty (no attachment requested).
func NewFileAttachmentFromData(data, contentType, name string) ([]Attachment, error) {
	if data == "" {
		return nil, nil
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	raw, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, fmt.Errorf("fileData is not valid base64: %w", err)
	}
	if len(raw) > MaxAttachmentRawBytes {
		return nil, fmt.Errorf("file too large: %d bytes exceeds the %d byte limit", len(raw), MaxAttachmentRawBytes)
	}
	return []Attachment{{ContentType: contentType, ContentBytes: data, Name: name}}, nil
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
func NewBroadcastMsg(peerID, text, ts string, attachments []Attachment, format, replyTo string) Msg {
	return Msg{Type: TypeMsg, PeerID: peerID, Text: text, TS: ts, Attachments: attachments, Format: format, ReplyTo: replyTo}
}

// NewDirectedMsg is what the server sends to the single targeted peer for a
// private message.
func NewDirectedMsg(peerID, text, ts string, attachments []Attachment, format, replyTo string) Msg {
	return Msg{Type: TypeMsg, PeerID: peerID, Text: text, TS: ts, Private: true, Attachments: attachments, Format: format, ReplyTo: replyTo}
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
//
// Count/Oldest/Newest exist so a client can tell a truncated READ apart
// from a truncated SEND — the server-side burst was complete and correct
// (verified, live, 2026-09-03: a message a client believed it never
// received was intact in the server's own store and delivered whole; the
// loss was entirely in a display/notification layer downstream of this
// client, one that cut a many-event burst to its first few lines with no
// indication anything was cut). Without a number to check against, "I
// only see 4 of the 20 events you say you sent" is not something a client
// can detect on its own — it just looks like a short but complete answer.
// All three additive/omitempty: an older client/server that doesn't know
// them loses nothing it had.
type HistoryComplete struct {
	Type Type `json:"type"`
	// Count is how many msg events this burst delivered (0 for an empty
	// burst — still sent, not omitted, so "no history" is also
	// positively confirmed). Compare against however many Historical
	// events were actually seen since the matching History request; a
	// mismatch means something between the server and this exact reading
	// of it was lost, and hub_history(before/after: <the gap>) is how to
	// recover it, not just re-reading the same notification.
	Count int `json:"count,omitempty"`
	// Oldest/Newest are the delivered burst's own cursor range (its
	// first and last Msg.Cursor, in delivery order) — omitted along with
	// Count when the burst was empty. A client that already holds a
	// cursor from further back than Oldest knows this page didn't reach
	// far enough and should page again with before/after.
	Oldest string `json:"oldest,omitempty"`
	Newest string `json:"newest,omitempty"`
}

func NewHistoryComplete() HistoryComplete {
	return HistoryComplete{Type: TypeHistoryComplete}
}

// HistoryBegin is an optional leading counterpart to HistoryComplete, sent
// (if a server implements it) BEFORE a History request's answering burst
// of msg events rather than after — same Count/Oldest/Newest fields,
// known ahead of the loop since the page is materialized before it's
// streamed out. The reason for a second frame carrying the same
// information twice: a downstream display/notification layer that
// truncates a long burst overwhelmingly cuts the TAIL, not the head
// (confirmed live, 2026-09-03), so a trailing-only marker is truncated
// away in exactly the scenario it exists to catch. A leading marker
// survives a tail cut; the trailing HistoryComplete survives a (rarer)
// head cut; a client holding both can also catch a middle cut by
// comparing them. Not sent by any server today — additive/optional, so a
// server that doesn't implement it is unaffected, and a client that
// doesn't recognize this type ignores it like any other unknown frame.
type HistoryBegin struct {
	Type   Type   `json:"type"`
	Count  int    `json:"count,omitempty"`
	Oldest string `json:"oldest,omitempty"`
	Newest string `json:"newest,omitempty"`
}

func NewHistoryBegin(count int, oldest, newest string) HistoryBegin {
	return HistoryBegin{Type: TypeHistoryBegin, Count: count, Oldest: oldest, Newest: newest}
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
	// Attachments mirrors Msg.Attachments — the edited message's current
	// attachments (inline or reference form, same as a live Msg), not a
	// diff against what it had before. Absent means the edit itself
	// carried no Attachments field at all (see Edit.Attachments) and so
	// left them unchanged; a receiving client should keep whatever
	// attachments it already associated with this externalId in that
	// case, not treat an absent field here as "now has none."
	Attachments []Attachment `json:"attachments,omitempty"`
	// Format mirrors Msg.Format — how Text should be interpreted.
	Format string `json:"format,omitempty"`
	// ReplyTo/ReplyPreview mirror Msg.ReplyTo/Msg.ReplyPreview — an edit
	// never changes what a message replies to, so these are set from the
	// same underlying reply reference as the original Msg, present here
	// too so a client that only ever saw the edited version still has it.
	ReplyTo      string `json:"replyTo,omitempty"`
	ReplyPreview string `json:"replyPreview,omitempty"`
	// Mentions/MentionedMe mirror Msg.Mentions/Msg.MentionedMe — an edit
	// can change who's mentioned, so these reflect the edited text, not
	// the original.
	Mentions    []Mention `json:"mentions,omitempty"`
	MentionedMe bool      `json:"mentionedMe,omitempty"`
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
	// Attachments, like Msg.Attachments, carries inline content (see
	// Attachment) to attach — always the inline form (ContentType +
	// ContentBytes), even against a reference-style server, which is
	// responsible for recoding/storing it and delivering it back by
	// reference on MessageEdited, the same as it does for a Msg. Absent
	// means "leave existing attachments as they are" — there is
	// deliberately no way to express attachment removal via Edit, same as
	// Msg.
	Attachments []Attachment `json:"attachments,omitempty"`
	// Format is a server extension — see Msg.Format for the full contract
	// ("text"/"html", unrecognized values refused). Applies to the new
	// Text this edit sets.
	Format string `json:"format,omitempty"`
	// ReplyTo requests a native threaded-reply citation to another
	// message, identified by its externalId — a server extension
	// (chat-relay), symmetric with the ReplyTo a client receives (see
	// Msg.ReplyTo). Must name a message the server actually holds in this
	// same conversation; a server that validates it should refuse
	// outright (not send anything) for an unknown/foreign/malformed
	// value, since resolving the citation can surface that message's own
	// preview text — an unvalidated cross-conversation reference is a
	// disclosure risk, not just a bad request. Empty means "not a reply."
	ReplyTo string `json:"replyTo,omitempty"`
}

func NewEditRequest(externalID, text string, attachments []Attachment, format, replyTo string) Edit {
	return Edit{Type: TypeEdit, ExternalID: externalID, Text: text, Attachments: attachments, Format: format, ReplyTo: replyTo}
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
