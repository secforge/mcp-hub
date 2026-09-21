package hubconn

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/secforge/mcp-hub/internal/agekey"
	"github.com/secforge/mcp-hub/internal/sanitize"
	"github.com/secforge/mcp-hub/internal/version"
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
	// CorrelationID is the id this client put on the request, echoed back
	// by a correlating server — see wire.Msg.ID. Empty on live traffic
	// nobody asked for, on every frame from a server without the feature,
	// and on an error that is not about a request (a close reason, or
	// wire.CodeBadCorrelation, which cannot echo the value it refuses).
	// So an empty one is never evidence that an echo failed; only the
	// absence of the declared feature says the server does not echo.
	CorrelationID string
	// ExternalID carries a "sendAck"/"reactionAck"/"editAck" event's
	// payload (see wire.SendAck/wire.ReactionAck/wire.EditAck), or — on a
	// "msg" — the same id correlating it with the sendAck that preceded
	// it (see wire.Msg.ExternalID). ActionOK is shared across all three
	// ack kinds: "did the action this connection asked for succeed."
	ExternalID string
	ActionOK   bool
	// ActionOKStated reports whether the server actually SAID whether the
	// action succeeded, as opposed to sending an ack with no "ok" field
	// at all. Go decodes an absent bool as false, so without this an
	// omitted field is indistinguishable from an explicit refusal — and
	// the client would report "the server refused" about a server that
	// said nothing of the kind. Seen live: an ack family where one member
	// was built unlike its siblings and omitted the field, turning every
	// success into a reported refusal.
	//
	// So the three states stay apart: succeeded, refused, and didn't say.
	// Only the first two are claims about the world.
	ActionOKStated bool

	// RosterPeers carries the session's membership on a "roster" event —
	// the server stating the whole list in one message, re-sent in full
	// whenever it changes. It INCLUDES this connection itself as the
	// server sent it; the read loop removes that entry before the event
	// is buffered, so a reader is never handed a list it has to mentally
	// correct. Empty on every other kind.
	RosterPeers []PeerInfo

	// RosterReadAt is when the conversation behind a teams link was last
	// read, where the server says. Empty means no answer about it, which
	// is not the same as never read.
	RosterReadAt string
	// RosterNameTaken says another peer in this roster already answers to
	// this connection's own display name. Set only on a "roster" event,
	// and only where this connection has a name of its own to collide.
	RosterNameTaken bool
	// PushSeq numbers this event within the sequence of pushes this
	// connection has made, from 1, and is set only on the push path.
	//
	// It exists because a PUSH CANNOT BE CONFIRMED. The transport says so
	// outright: a successful Deliver returns ObservedNothing, meaning the
	// bytes were written and arrival is unverified. A harness that accepts
	// a message and then discards it because its inbox is full reports
	// nothing back, so this client cannot tell a delivery from a loss.
	//
	// Nothing else in a pushed message can reveal a hole. Cursors are
	// opaque and unordered by contract, so two consecutive arrivals say
	// nothing about whether a third belonged between them. A number does:
	// a reader that sees 45, 46, 48 knows 47 existed and did not arrive,
	// and can ask for it. Seen live on 2026-09-21, when 28 pushes were
	// dropped by a busy session's inbox and neither the client nor the
	// reader ever knew.
	PushSeq int

	// Mirrored says the connection this event arrived on is a mirror of
	// some other conversation (a teams link), where a message we send is
	// echoed back as its own "msg" event once it lands over there. On a
	// plain hub connection nothing echoes back, so an ack is the whole
	// story — and telling a reader to expect a second copy that will
	// never come is the kind of instruction that gets waited on.
	Mirrored bool
	// Behind carries a standalone ack's reply's own wire.Ack.Behind, if
	// the server sent one — see that field's doc comment. Nil on every
	// other event kind, and on an "ack" from a server that doesn't send
	// it; never zero-as-absent, since a genuine "0 behind" is meaningful
	// and must stay distinguishable from "not sent."
	// ByName and ByID identify who made a pin change, on the platform
	// behind a mirrored conversation — never a peerId. See wire.Identity.
	ByName string
	ByID   string
	// PinnedList carries the answer to a pins request, and is nil rather
	// than empty when this event is not one: an empty set of pins and "no
	// answer here" are different things.
	PinnedList []string
	// UnconfirmedCount and UnconfirmedSince are set only on a
	// "confirmReminder": how many cursor-bearing messages have been
	// delivered live without a confirm, and when that run started. They
	// are what makes the reminder state a growing cost rather than repeat
	// an instruction.
	UnconfirmedCount int
	UnconfirmedSince time.Time
	Behind           *int
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
	//
	// Matching carries a "msg" or "noMoreMessages" event's
	// wire.Msg.Matching/wire.NoMoreMessages.Matching — the filter the
	// server actually APPLIED, which is not necessarily the one that was
	// sent. Nil when the answer was unfiltered, which is what makes a
	// filtered terminator ("nothing further matches") distinguishable
	// from an unfiltered one ("no more messages") instead of two
	// readings of the same frame.
	Answers  *wire.Anchor
	Matching *wire.Filter
	// ReconnectAfter carries a "serverStopping" frame's own estimate, in
	// seconds — see wire.ServerStopping.
	ReconnectAfter int
	// HeldCount, on a "deliveryHeld" event, is how many live messages the
	// push window has withheld. SpillPath and SpillBytes, on a message
	// whose body was too large to deliver inline, name the file it was
	// written to and its size. All three exist so a shaped delivery states
	// what was done to it rather than quietly arriving smaller.
	HeldCount  int
	SpillPath  string
	SpillBytes int
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
	// The fields below are only ever set for a teams session (see
	// wire.Joined) — zero-valued for every mcp-hub-server connection.
	canSend          bool
	conversationKind string
	topic            *string
	// behind is nil when the server did not state it — a server with no
	// such concept, or a connection with no prior position. Distinct from
	// a stated 0, which asserts "you are caught up" as a fact.
	behind      *int
	behindSince string
	// features/featuresDeclared hold wire.Joined.Features, if the server
	// sent one at all — see that field's doc comment. featuresDeclared
	// distinguishes "no Features field sent" (pre-v3, nothing known)
	// from "Features sent but this key absent" (definitively
	// unsupported) — both look identical as a missing map entry
	// otherwise. Immutable after construction, same as the other Joined-
	// derived fields above; not under mu.
	// clientRelease is what the server verified the current client
	// release to be, or nil where it verified nothing. Kept as the
	// pointer it arrived as, so "said nothing" stays distinguishable
	// from "said something empty".
	clientRelease *wire.ClientRelease

	// pinnedAtConnect is what joined.Pinned carried. A snapshot, not a
	// live set: it is what was pinned when this connection opened, and
	// pinned/unpinned events move it on from there. Kept so the connect
	// result can state it without a round trip.
	pinnedAtConnect  []string
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

	// activity carries "something happened" to activityLoop, which is the
	// ONLY place onActivity is called from. Capacity 1 and a non-blocking
	// send: every callback drains whatever is there, so a second signal
	// arriving while one is pending would tell it nothing new.
	// lateAnswers counts, per ack kind, the answers still owed to
	// requests that timed out — see expectLateAnswer.
	lateAnswers map[string]int

	activity     chan struct{}
	activityOnce sync.Once
	// gone is closed when the read loop ends, so a caller blocked on a
	// claim learns the connection dropped instead of waiting out its
	// whole deadline. An attachment fetch waits up to three minutes, and
	// it runs inside the activity callback, so the teardown and the
	// reconnect that follow it were delayed by that much. Found by an
	// external reviewer, 2026-09-20.
	gone     chan struct{}
	goneOnce sync.Once

	// activityStop ends activityLoop when the read loop ends. Without it
	// the loop ranged forever on a channel nothing ever closed, so every
	// dial/close cycle left a goroutine alive holding its Conn, its
	// buffer and its callback — three cycles, three leaks.
	activityStop     chan struct{}
	activityStopOnce sync.Once

	// writeWait bounds an application write the same way it already
	// bounds the keepalive's — snapshotted here for the same reason as
	// pongWait above.
	writeWait time.Duration

	// confirmReminderInterval is snapshotted the same way and for the
	// same reason as pongWait above — read only by
	// confirmReminderLoop, captured before it's spawned.
	confirmReminderInterval time.Duration

	// How a connection was dialled is deliberately not recorded here: it
	// is a fact about the credential, never about what the server can do,
	// and both forms are opaque links carrying the same fields anyway.
	// Everything behavioural comes from the server's own declaration —
	// see features/HasFeature.

	mu     sync.Mutex
	buffer []Event
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
	closed         bool
	closeCode      int // set from the WebSocket close frame's code, if any — see DisconnectNote
	// reconnectAfter is the server's own restart estimate in seconds,
	// from a "serverStopping" frame. Zero when none arrived — which
	// includes a graceful stop whose frame was missed, so it is never
	// read as "this was not graceful": the close code answers that.
	reconnectAfter int
	timedOut       bool      // set when the connection was dropped by our own pongWait deadline, not a close frame — see DisconnectNote
	timedOutAt     time.Time // when timedOut was set — see DisconnectNote's use of it against lastFrameAt
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
	// contract — see LastConsumedCursor and Drain.
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
	lastConsumed string
	// lastConfirmed is the position the MODEL has confirmed it has
	// complete, and is the only thing this client ever offers as a read
	// receipt. lastConsumed above means HANDED OVER; a confirm means
	// COMPLETE. They are different facts and the wire has one column for
	// the second, so piggybacking the first was this client asserting,
	// on the reader's behalf, something the reader had not decided.
	//
	// The reader then saw it: the confirm reminder asks for the last
	// message it has complete, "possibly earlier than" the last
	// delivered — and an honest lower answer was refused by a monotonic
	// server, because a send had already offered the higher one. The
	// honest confirm looked like the error.
	//
	// Advanced only by an accepted ConfirmReceived, never by
	// MarkConsumed.
	lastConfirmed string
	lastAckSent   string
	ackDisabled   bool
	onActivity    func()
	// budget governs what may be PUSHED to a reader unbidden — see
	// budget's own doc comment for why the receiver's context, rather than
	// the transport, is the scarce resource here.
	// writeMu serializes every frame written to ws. gorilla/websocket
	// permits exactly ONE concurrent writer and panics when it detects a
	// second ("concurrent write to websocket connection"), so this is not
	// a tidiness lock: the reminder loop runs on a timer for the whole
	// life of the connection while the MCP request goroutine writes
	// whatever tool the model just called, and the two coinciding is
	// ordinary rather than unlucky. A panic inside an MCP server takes the
	// session's connection with it and reads as a crash rather than a race.
	//
	// WriteControl is the one method exempt from that contract, which is
	// why pingLoop does not take this — see its own comment. The rule was
	// looked up once, applied correctly to the exempt case, and not to the
	// thirteen call sites that needed it.
	writeMu sync.Mutex
	// abandoned records that this connection was ended by the client
	// itself because nothing was draining its buffer — see abandon.
	abandoned bool
	budget    *budget
	// budgetOwner identifies this connection inside a SHARED window, so a
	// confirm releases what this connection delivered rather than a
	// prefix of everyone's. Empty where the window is this connection's
	// alone — see ShareDeliveryBudget.
	budgetOwner string
	// skippedHeld records that a confirm released a prefix containing a
	// message the window had held back, so the next tool result can tell
	// the reader their position moved over something they never saw.
	skippedHeld     bool
	peers           map[string]PeerInfo
	rosterAnnounced bool
	// fatalText is set by an "error" event the server marked NOT
	// retryable, and is what stops this client reconnecting for ever to
	// something that has been deleted — see PermanentFailure.
	fatalText string
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
	// pushSeq counts pushes made on this connection, so each carries a
	// position a reader can check for gaps — see Event.PushSeq.
	pushSeq int
	// resumablePeers is joined.ResumablePeers as the server sent it: how
	// many peers in this conversation still hold a reconnect secret,
	// present only when the server MINTED a fresh identity for this
	// connection and nil when it reclaimed one.
	//
	// Nil and zero are different answers and are kept apart. Nil means
	// either "this identity was resumed, so the question does not
	// arise" or "this server does not say"; mintNoticeDeclared tells
	// those apart. Zero means the server minted and there was genuinely
	// nothing to resume, which is the reassuring case and is worth being
	// able to state.
	resumablePeers     *int
	mintNoticeDeclared bool
	// correlates is set when the server declared wire.FeatureCorrelation
	// at join: every request that awaits an answer then carries a
	// client-chosen id which the server echoes on the answer and on any
	// error refusing it.
	//
	// It selects between two attribution schemes and never runs both.
	// With it, a claim is matched by id and a late answer simply finds
	// no claim holding its id, so the late-answer debt (expectLateAnswer)
	// is not recorded at all — its trigger would be unreachable and a
	// live ratchet nobody can trigger is a bug with no reproduction.
	// Without it, the debt machinery runs exactly as before.
	//
	// DELETION TRIGGER for expectLateAnswer/spendLateAnswerLocked and
	// everything they carry: when no server this client supports is
	// still without the feature. Until then both paths are load-bearing.
	correlates bool
	// attachments is what the server declared under "attachments" — its
	// own per-attachment raw cap and whether it accepts images only.
	// Read at connect so a send can be refused BEFORE a file is encoded
	// and pushed, rather than learning the answer from a refusal that
	// arrives after the whole thing is on the wire. The helper that
	// decodes this shape existed and had no client caller at all.
	attachments     wire.AttachmentsFeature
	attachmentsSaid bool
}

// ackClaim is a one-shot subscription for the next event matching a
// specific ack kind (or a generic "error") — see claimNextAck.
type ackClaim struct {
	result chan Event // buffered, size 1; written to exactly once
	// corrID is this claim's correlation id (see wire.Msg.ID), set only
	// when the server declared the "correlation" feature. When it is
	// set it is the WHOLE match: an answer of the right kind carrying a
	// different id is somebody else's and falls through to the buffer,
	// and an answer carrying this id belongs here however long it took.
	// Empty against a server that does not correlate, where the kind is
	// the only key there is and expectLateAnswer covers the rest.
	corrID string
	// token, where set, narrows this claim to one attachmentData: the
	// claim is keyed by event kind and is consumed by the FIRST event of
	// that kind, so without this a late answer to an abandoned request
	// satisfies the next one and the wrong file is handed back as
	// confidently as the right one. Matching here rather than in the
	// caller is what keeps the claim alive for the answer it is actually
	// waiting for — a caller that filtered after the fact would find the
	// claim already deleted and wait out its whole deadline for an
	// answer that had gone to the buffer.
	token string
	// anchor is the history request this claim is waiting on. An answer
	// names the anchor it answers, and one naming a different anchor
	// belongs to an earlier request — handing it back here would answer
	// "what follows X" with what follows Y, and a catch-up caller then
	// persists a position from a walk it never made.
	anchor wire.Anchor
}

// fatalErrorCodes are the error codes that describe the CONNECTION
// rather than the request that provoked them. Only these disable
// automatic reconnect, because only these are still true on the next
// dial. Anything else refuses one operation and says nothing about the
// connection.
//
// ONE ENTRY, from chat-relay's own source rather than from observation.
// Asked 2026-09-19 for the authoritative list, its answer was that an
// error frame on that server NEVER means the connection is over: every
// code it emits (unavailable, send_refused, not_found, bad_request,
// bad_attachment, bad_ack, bad_ack_cursor, bad_anchor, bad_filter,
// too_large, unsupported) refuses one request and leaves the connection
// usable. What ends a connection there is the CLOSE.
//
// conversation_unavailable stays because it is the one case where the
// frame carries the reason and the close (4003) carries the verdict; a
// reader that saw the frame but missed the close still learns why. The
// five codes that used to sit beside it — conversation_deleted,
// unauthorized, forbidden, revoked, expired — were guesses at names that
// server does not send, and a guess in this map is a live connection
// killed over a word.
var fatalErrorCodes = map[string]bool{
	"conversation_unavailable": true,
}

// relayCloseNotes maps close codes that mean this connection should not
// be dialled again on its own, rather than ordinary network trouble:
// 4001 revoked, 4002 expired, 4003 the link's conversation became
// unavailable (e.g. the bot was removed from it), 4004 superseded.
//
// EVERY OTHER CLOSE IS RETRYABLE, 1006 included. chat-relay states that
// its overflow and write-timeout paths abort the transport rather than
// sending a close code, deliberately: a close frame is itself a write,
// and the condition being signalled is that writes do not complete, so a
// signal that can only be sent when the failure is not happening is not
// a signal. Recovery therefore cannot key off a code being present.
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
	// 4002 is a constant on chat-relay that nothing currently issues —
	// its expiry path closes with 4001. Kept because not retrying an
	// expired credential is right whoever sends it, but a 4002 seen in
	// the wild is evidence that something new shipped, not evidence
	// about the server as it stands.
	4002: " (expired — do not reconnect)",
	4003: " (conversation unavailable — do not reconnect)",
	// 4004 SUPERSEDED, added 2026-09-19 when chat-relay gave the
	// authoritative close-code list. It does not mean the connection was
	// broken: another connection presented a verified secret and took
	// this identity, and the socket that lost is simply no longer the one
	// holding it.
	//
	// chat-relay's own recommendation was to reconnect on it. This client
	// deliberately does not, and the reason is what an automatic
	// reconnect would BE: presenting the same secret again, which takes
	// the identity straight back from whoever just claimed it. If that
	// other holder also reconnects automatically — and if it is this same
	// client, it does — the two take turns forever, each one's recovery
	// causing the other's failure.
	//
	// The case where this client displaced its OWN socket is unaffected:
	// the replacement is already connected, so nothing schedules anything.
	// What is refused here is only the foreign takeover, and refusing it
	// leaves the decision with whoever can tell the two apart.
	4004: " (superseded — another connection holds this identity now; " +
		"reconnecting would take it back from whoever has it, so it is not done automatically)",
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

// PermanentFailure reports that reconnecting this link cannot succeed,
// and why. Two sources say so, and both are the server's own statement
// rather than an inference from a failure: a close code in the
// do-not-reconnect range (see relayCloseNotes), or an "error" event the
// server marked NOT retryable — a deleted conversation, a revoked or
// expired credential.
//
// The distinction matters because everything else is worth retrying. A
// drop with no explanation is ambiguous by nature — a killed process, a
// partition, a laptop that was asleep — and treating ambiguity as
// permanent is how a client stops reconnecting to a hub that is merely
// slow to come back.
func (c *Conn) PermanentFailure() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if note, ok := relayCloseNotes[c.closeCode]; ok {
		return strings.TrimSpace(strings.Trim(note, " ()")), true
	}
	if c.fatalText != "" {
		return c.fatalText, true
	}
	return "", false
}

// GracefulShutdown reports whether the server closed this connection on
// purpose, from the close code alone.
//
// Deliberately NOT keyed on having received a "serverStopping" frame. The
// close code comes from the websocket layer and cannot be half-received;
// the frame is an ordinary message that a busy reader can miss or receive
// truncated. Requiring the frame would report a clean shutdown as a crash
// exactly when this client was busiest, which is the failure mode the
// two-signal design exists to avoid.
//
// False for an ordinary drop, and that stays as ambiguous as it has
// always been: a killed process, an OOM, or a dead host sends no close
// frame at all, so false means "nothing said this was deliberate" — never
// "the server did not restart".
func (c *Conn) GracefulShutdown() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeCode == websocket.CloseGoingAway
}

// reconnectJitter is how much of the server's estimate to spread retries
// across. The estimate is a floor, not an appointment: every peer told
// the same number and obeying it exactly arrives in one burst against a
// server that has only just finished starting.
const reconnectJitter = 0.4

// SuggestedReconnectDelay is how long to wait before reconnecting after a
// graceful shutdown — the server's own estimate plus a random share of
// it, drawn per call so two peers given the same number do not return
// together. Zero when the server offered no estimate, which means "no
// advice", not "reconnect immediately".
func (c *Conn) SuggestedReconnectDelay() time.Duration {
	c.mu.Lock()
	secs := c.reconnectAfter
	c.mu.Unlock()
	if secs <= 0 {
		return 0
	}
	base := time.Duration(secs) * time.Second
	return base + time.Duration(rand.Float64()*reconnectJitter*float64(base))
}

// ServerReconnectEstimate is what the server actually said, unjittered —
// for reporting what was claimed as distinct from what this client
// decided to do about it. Zero when no estimate arrived.
func (c *Conn) ServerReconnectEstimate() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Duration(c.reconnectAfter) * time.Second
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
	snapPongWait, snapWriteWait, snapConfirmReminderInterval := pongWait, writeWait, confirmReminderInterval

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
	// AN OBSERVATION, NOT A CLAIM. This is the binary's own word for
	// itself: unsigned, and forgeable by anything that dials. It is
	// useful for a server showing who is running what, and it is not
	// evidence of anything — the only thing that makes a version
	// evidence is the signed release manifest (see internal/selfupdate).
	//
	// Sent for development builds too, which is the case a server most
	// wants to see, and never omitted as a way of looking current:
	// version.Short() is never empty, and "unknown" is a more useful
	// answer than a missing header.
	header.Set("Agent-Version", version.Short())
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
	return finishHandshake(ws, snapPongWait, snapWriteWait, snapConfirmReminderInterval)
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

// maxReadFrameBytes bounds a single incoming websocket frame, matching
// the bundled server's own cap (wsserver.maxReadMessageBytes) and, for
// the same reason it does, chat-relay's coordinated 44MB whole-frame cap
// with a little headroom: an attachment's base64 payload inflates
// wire.MaxAttachmentRawBytes by about a third, and a JSON envelope sits
// on top of that.
//
// The server has bounded what a client may send it since it was written.
// This is the other direction, which had no bound at all:
// gorilla/websocket defaults to none, so a server — hostile, wedged, or
// simply wrong — could announce a frame of any size and this client would
// read all of it into memory before deciding anything about it. Trusting
// the far end because it is "the server" is the assumption the whole
// untrusted-content design here refuses to make everywhere else.
const maxReadFrameBytes = 48 * 1024 * 1024

// finishHandshake reads the server's initial "joined" message off an
// already-connected ws, validates it, and builds the running Conn —
// the tail shared by Dial and DialRelay, which differ only in how they
// reach an open *websocket.Conn (a normalized host+sessionId URL with no
// custom headers, vs. an arbitrary caller-supplied URL with an
// Authorization/etc. header). Nothing downstream of here differs between
// the two: everything behavioural comes from the server's declared
// features, never from which of them dialled.
func finishHandshake(ws *websocket.Conn, snapPongWait, snapWriteWait, snapConfirmReminderInterval time.Duration) (*Conn, error) {
	// BOUNDED, because an upgrade is not an answer. A server that
	// completes the HTTP upgrade and then says nothing left this read
	// blocked with no deadline set yet — the deadline the read loop
	// relies on is only installed further down — so hub_connect hung
	// indefinitely against a server that had already accepted the socket.
	// A hang is the one failure a caller cannot report or retry.
	// A HANDSHAKE DEADLINE, not the keepalive's. pongWait is 100s, which
	// is the right bound for an idle established connection and far past
	// most MCP tool timeouts for a server that upgrades and then says
	// nothing — hub_connect simply hung for it. A joined frame is the
	// first thing a server sends and it sends it immediately. Found by
	// an external reviewer, 2026-09-20.
	handshakeWait := 10 * time.Second
	if snapPongWait < handshakeWait {
		handshakeWait = snapPongWait
	}
	_ = ws.SetReadDeadline(time.Now().Add(handshakeWait))
	// Set BEFORE the first read, not with the rest of the connection's
	// settings further down: the joined frame is itself a frame from a
	// server this client has not yet learned anything about.
	ws.SetReadLimit(maxReadFrameBytes)
	_, raw, err := ws.ReadMessage()
	if err != nil {
		ws.Close()
		return nil, fmt.Errorf("no joined message from the server after the connection was "+
			"accepted (waited %s): %w", handshakeWait, err)
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
		peers:                   make(map[string]PeerInfo),
		pongWait:                snapPongWait,
		writeWait:               snapWriteWait,
		activity:                make(chan struct{}, 1),
		activityStop:            make(chan struct{}),
		gone:                    make(chan struct{}),
		canSend:                 joined.CanSend,
		conversationKind:        joined.ConversationKind,
		topic:                   joined.Topic,
		behind:                  joined.Behind,
		behindSince:             joined.BehindSince,
		pinnedAtConnect:         pinnedOrNil(joined.Pinned),
		clientRelease:           joined.ClientRelease,
		features:                joined.Features,
		featuresDeclared:        joined.Features != nil,
		correlates:              correlationUsable(joined),
		attachments:             attachmentsDeclared(joined),
		resumablePeers:          joined.ResumablePeers,
		mintNoticeDeclared:      joined.HasFeature(wire.FeatureMintNotice),
		attachmentsSaid:         attachmentsStated(joined),
		lastFrameKind:           "joined",
		lastFrameAt:             time.Now(),
		confirmReminderInterval: snapConfirmReminderInterval,
		budget:                  newBudget(),
	}
	ws.SetPingHandler(func(appData string) error {
		ws.SetReadDeadline(time.Now().Add(snapPongWait))
		c.mu.Lock()
		c.lastFrameKind, c.lastFrameAt = "ping", time.Now()
		c.mu.Unlock()
		return ws.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(snapWriteWait))
	})
	go c.readLoop()
	go c.activityLoop()
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

// attachmentsDeclared and attachmentsStated split what the server said
// about attachments from whether it said anything, because the two are
// different facts and collapsing them is how a silent server ends up
// treated as one declaring zero.
func attachmentsDeclared(joined wire.Joined) wire.AttachmentsFeature {
	af, _ := joined.AttachmentsFeature()
	return af
}

func attachmentsStated(joined wire.Joined) bool {
	_, ok := joined.AttachmentsFeature()
	return ok
}

// MaxAttachmentBytes is the per-attachment raw cap this server declared,
// or 0 when it declared none. A caller checks this BEFORE encoding: the
// alternative is pushing a file the server will refuse, which on this
// path costs the whole transfer and then returns the refusal
// asynchronously, as an error the caller still has to attribute.
func (c *Conn) MaxAttachmentBytes() int {
	if !c.attachmentsSaid {
		return 0
	}
	return c.attachments.MaxRawBytes
}

// AcceptsImagesOnly reports whether this server said it takes images and
// nothing else. False when it said nothing, which is not the same as a
// server that stated it accepts anything — but the two call for the same
// behaviour here, since neither is grounds to refuse locally.
func (c *Conn) AcceptsImagesOnly() bool {
	return c.attachmentsSaid && c.attachments.ImagesOnly
}

// MintedWithResumablePeers reports that the server gave this connection a
// NEW identity while the conversation still held peers that could have
// been resumed, and how many.
//
// A client cannot work this out for itself. Its own stored identities
// are scoped to one project, so a scope holding nothing for this link is
// indistinguishable from a first-ever connect — and the only local way
// to tell the two apart would be to read other projects' entries, which
// is not this client's data to read. The server has the fact already,
// about its own conversation, with no other project in it.
//
// Reports false when the server did not mint (the identity was
// reclaimed, so nothing was lost), when it minted and nothing was
// resumable, and when it does not declare the feature at all — the last
// of which is silence rather than a zero, and must not be rendered as
// "there was nothing to resume".
func (c *Conn) MintedWithResumablePeers() (int, bool) {
	if !c.mintNoticeDeclared || c.resumablePeers == nil || *c.resumablePeers == 0 {
		return 0, false
	}
	return *c.resumablePeers, true
}

// FeaturesDeclared reports whether the server sent a Features object at
// all (see wire.Joined.Features) — false for a pre-v3 server, where
// nothing is known about individual capabilities either way and a
// runtime fallback (e.g. ConfirmReceived's ackReplyMisses probe) is the
// only way to find out.
func (c *Conn) FeaturesDeclared() bool { return c.featuresDeclared }

// VerifiedClientRelease is the release this server says is current, and
// whether it said anything at all. The second return is the whole point:
// a server that verified nothing must not read as "you are up to date".
func (c *Conn) VerifiedClientRelease() (wire.ClientRelease, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clientRelease == nil {
		return wire.ClientRelease{}, false
	}
	return *c.clientRelease, true
}

// HasFeature reports whether the server explicitly declared support for
// the named feature — meaningless (always false) when FeaturesDeclared
// is false, since a pre-v3 server has said nothing either way.
func (c *Conn) HasFeature(name string) bool {
	_, ok := c.features[name]
	return ok
}

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
// Behind reports how far this peer trails, and whether the server said so
// at all. Callers branch on >0, for which an unstated value and a stated
// zero mean the same thing — but they are different facts, and the one
// that says "caught up, as of connect" is worth not throwing away.
func (c *Conn) Behind() int {
	if c.behind == nil {
		return 0
	}
	return *c.behind
}

// BehindStated reports whether the server answered the question at all.
func (c *Conn) BehindStated() bool  { return c.behind != nil }
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
//
// Registering also reports what is ALREADY waiting, because a connect
// fills the buffer before anyone is listening: the read loop takes the
// roster frames while the caller is still wiring this up, and a callback
// that only arranges to hear the NEXT event leaves those undelivered
// until something else happens to arrive — on a quiet session, never.
// Firing here rather than at each call site keeps a future caller from
// having to remember it.
func (c *Conn) OnActivity(f func()) {
	c.mu.Lock()
	c.onActivity = f
	waiting := len(c.buffer) > 0 || c.closed
	c.mu.Unlock()
	if f != nil && waiting {
		c.signalActivity()
	}
}

// signalActivity wakes the one goroutine that runs onActivity.
//
// The callback used to be invoked by the read loop itself, which meant it
// could not do anything that needed an ANSWER from this connection: the
// answer could only be read by the loop that was waiting for the callback
// to return. Fetching a reference attachment during delivery is exactly
// that, and it did not just fail — it held the socket unread for the
// whole deadline, ping handling included, so a longer deadline made it
// worse rather than better.
//
// One goroutine, not one per signal, because deliveries must stay
// ordered: two callbacks running at once would interleave two drains of
// the same buffer.
func (c *Conn) signalActivity() {
	select {
	case c.activity <- struct{}{}:
	default:
	}
}

func (c *Conn) activityLoop() {
	run := func() {
		c.mu.Lock()
		f := c.onActivity
		c.mu.Unlock()
		if f != nil {
			f()
		}
	}
	for {
		select {
		case <-c.activity:
			run()
		case <-c.activityStop:
			// One last look before going. The signal saying the
			// connection ended is the most important one this loop ever
			// carries and is raised immediately before the stop, so
			// leaving without checking would drop exactly the callback a
			// reconnect depends on.
			select {
			case <-c.activity:
				run()
			default:
			}
			return
		}
	}
}

// stopActivity ends activityLoop, once, however the read loop got there.
func (c *Conn) stopActivity() {
	c.activityStopOnce.Do(func() { close(c.activityStop) })
	c.goneOnce.Do(func() { close(c.gone) })
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
// that's how this protocol reports a refusal (e.g. the tenant lock).
// Against a server that does not declare wire.FeatureCorrelation there is
// no per-request id linking an error back to the action that caused it,
// so an error arriving while a SINGLE claim is pending is assumed to be
// that claim's outcome — a real, acknowledged protocol limitation there,
// not something this method can fully close. Where the server does
// correlate, the id decides and the assumption is never made. cancel releases the claim (e.g. on
// timeout) so a later, unrelated event isn't wrongly attributed once the
// caller has stopped waiting — after cancel, or once the claim has fired,
// everything reverts to the normal buffer path.
func (c *Conn) claimNextAck(ackKind string) (result <-chan Event, cancel func(), err error) {
	_, result, cancel, err = c.claimNextAckCorrelated(ackKind, "")
	return result, cancel, err
}

// claimNextAckCorrelated is claimNextAck plus the correlation id to stamp
// on the request this claim is about to await — empty against a server
// that did not declare the feature, in which case nothing changes and the
// kind remains the only key.
//
// The id is minted HERE, with the claim, and handed to the caller to put
// on the frame. Registering the claim and choosing the id in one step is
// what makes the pairing impossible to get wrong: there is no window in
// which a request carries an id no claim is holding, and no way to write
// a request whose id belongs to a different claim.
func (c *Conn) claimNextAckCorrelated(ackKind, token string) (corrID string, result <-chan Event, cancel func(), err error) {
	if c.correlates {
		corrID = newCorrelationID()
	}
	result, cancel, err = c.registerClaim(ackKind, token, corrID)
	if err != nil {
		return "", nil, cancel, err
	}
	return corrID, result, cancel, nil
}

// correlationUsable reports whether this connection should correlate: the
// server declares the feature AND the cap it declares can hold the id
// this client mints.
//
// The cap is READ, not assumed. A server declaring a maxLength shorter
// than the id would refuse every correlated request with
// bad_correlation, so sending one anyway would turn a working connection
// into one where every request is refused — worse than not correlating at
// all. Declining the feature instead falls back to the late-answer debt,
// which is exactly what it is for.
//
// A declaration with no maxLength is taken at its word: the field is
// optional and its absence says the server states no limit, not that the
// limit is zero.
func correlationUsable(joined wire.Joined) bool {
	if !joined.HasFeature(wire.FeatureCorrelation) {
		return false
	}
	var decl struct {
		MaxLength *int `json:"maxLength"`
	}
	if raw, ok := joined.Features[wire.FeatureCorrelation]; ok && len(raw) > 0 {
		// A payload this client cannot parse is not a refusal: the
		// feature was declared, and an unrecognised shape is a later
		// protocol extension rather than a smaller cap.
		_ = json.Unmarshal(raw, &decl)
	}
	if decl.MaxLength != nil && *decl.MaxLength < correlationIDLen {
		debugf("correlationUsable: server declares correlation maxLength=%d, shorter than the %d-byte "+
			"id this client mints — not correlating", *decl.MaxLength, correlationIDLen)
		return false
	}
	return true
}

// correlationIDLen is the length of what newCorrelationID mints: a uuid in
// its canonical text form.
const correlationIDLen = 36

// newCorrelationID mints one. A uuid is well inside
// wire.MaxCorrelationIDLen and carries nothing about this peer — the id
// is opaque to the server by contract, and choosing a value that would
// stop being opaque if the contract were ignored is free.
func newCorrelationID() string { return uuid.NewString() }

func (c *Conn) registerClaim(ackKind, token, corrID string) (result <-chan Event, cancel func(), err error) {
	ch := make(chan Event, 1)
	c.mu.Lock()
	if c.pendingAcks == nil {
		c.pendingAcks = make(map[string]*ackClaim)
	}
	// REFUSE a second claim rather than replacing the first. Overwriting
	// produced two wrong outcomes at once: the displaced caller was never
	// delivered to and never learned — its own cancel found a different
	// claim in the map and so did not even clean up, it simply waited out
	// its timeout — while the surviving claim received the FIRST ack of
	// that kind to arrive, which may be the answer to the displaced
	// caller's request. That ack carries an outcome a caller renders to a
	// model as delivered or refused, so two concurrent sends could produce
	// one timeout and one confidently wrong answer, which is worse than
	// two timeouts.
	//
	// Failing loudly is the right shape here: the caller knows it has an
	// outstanding request of this kind, and an error it can report beats
	// inheriting someone else's outcome.
	if _, taken := c.pendingAcks[ackKind]; taken {
		c.mu.Unlock()
		return nil, func() {}, fmt.Errorf(
			"another %s request is already awaiting its answer on this connection; "+
				"retry once it has returned", ackKind)
	}
	claim := &ackClaim{result: ch, token: token, corrID: corrID}
	c.pendingAcks[ackKind] = claim
	c.mu.Unlock()
	cancel = func() {
		c.mu.Lock()
		if existing, ok := c.pendingAcks[ackKind]; ok && existing == claim {
			delete(c.pendingAcks, ackKind)
		}
		c.mu.Unlock()
	}
	return ch, cancel, nil
}

// answersAnchor reports whether an answer's own "answers" field names the
// anchor this claim asked about.
//
// A server that states nothing (a nil answers) is taken as answering the
// outstanding request: it is the only request there is, and refusing it
// would strand a caller against every server that does not echo the
// anchor back. Where the server DOES state one, it is believed — that is
// the whole point of it saying so.
func answersAnchor(answers *wire.Anchor, want wire.Anchor) bool {
	if answers == nil {
		return true
	}
	return answers.Cursor == want.Cursor && answers.At == want.At
}

// idMatches is the ONE rule every match site uses for a correlation id:
// where the claim minted an id AND the answer states one, they must be
// equal; otherwise the id decides nothing and the site's own rule stands.
//
// Both conditions are load-bearing and the second is the one that was
// learned the hard way. A claim always mints an id on a correlating
// connection, but only some ANSWERS carry one: the echo agreed with
// chat-relay covers the acks and "error", so "noMoreMessages",
// "attachmentData" and a historical "msg" arrive without one until a
// server extends it. Demanding an id on those hangs the caller for its
// whole deadline against the very server that declares the feature —
// which is a worse failure than the misattribution being fixed, and is
// this client punishing itself for an echo nobody promised.
//
// Stated once and shared because it was NOT shared, and the three sites
// drifted: the history match ignored the id entirely while the ack match
// demanded it, so one path misattributed and another hung, from the same
// commit.
func idMatches(ev Event, claim *ackClaim) bool {
	if claim.corrID == "" || ev.CorrelationID == "" {
		return true
	}
	return ev.CorrelationID == claim.corrID
}

// answersHistoryClaim reports whether ev is the answer to this pending
// MessageAfter claim.
//
// WHERE THE ANSWER CARRIES AN ID, THE ID IS THE WHOLE TEST. The anchor
// cannot separate two requests that asked the same question, and asking
// the same question twice is this client's own documented recovery — a
// timed-out catch-up is retried with the same anchor, because that is
// what the tool tells the model to do. So request 1's late answer matched
// request 2's anchor and was handed over as request 2's outcome: the
// exact misattribution the correlation id exists to end, in the one path
// that minted an id and then ignored it.
//
// WHERE IT DOES NOT, THE ANCHOR RULE STANDS. A history answer is not an
// ack, and the echo agreed with chat-relay covers the acks and "error" —
// "noMoreMessages" carries no id at all today and a historical "msg" is
// not guaranteed to. Demanding one would make every walk against such a
// server time out, which is a worse failure than the one being fixed and
// would be caused by this client assuming an echo nobody promised.
//
// So the rule is: believe an id when there is one, fall back when there
// is not. Note what that does NOT do — an id-less late answer still
// matches by anchor, so this closes the demonstrated hole and not the
// whole class. Closing the class needs the echo extended to the history
// frames, which is a wire change and not this function's to assume.
func answersHistoryClaim(ev Event, claim *ackClaim) bool {
	if !idMatches(ev, claim) {
		return false
	}
	// Where the answer stated a MATCHING id, it has already identified
	// itself and the anchor adds nothing. Where it stated none, the
	// anchor is all there is.
	if claim.corrID != "" && ev.CorrelationID != "" {
		return true
	}
	return answersAnchor(ev.Answers, claim.anchor)
}

// tryDivertToClaimLocked checks ev against any pending ack claims and, if
// it matches one, delivers it directly and reports true — the caller
// (readLoop) must skip buffering it and firing OnActivity for it entirely
// when this returns true. Must be called with c.mu held.
// spendLateAnswerLocked reports whether ev settles a debt recorded by a
// request that timed out — see expectLateAnswer — and consumes it if so.
//
// ON ANY ARRIVAL OF THAT KIND, not only when a claim is waiting. The
// first version checked the debt inside the claim branch, so a late
// answer landing with nobody waiting (the ordinary case: the caller had
// already returned) left the debt standing at one. The NEXT request's
// own prompt answer was then charged to it, dropped, and that caller
// timed out — which recorded the debt again. Every send, react, edit and
// delete on the connection then took the full timeout and reported no
// acknowledgement, with the answers arriving as unsolicited events. A
// ratchet, from the mitigation that was supposed to cost one missing
// answer. Found by an external reviewer with a reproduction, 2026-09-20,
// hours after that mitigation was written.
func (c *Conn) spendLateAnswerLocked(ev Event) bool {
	kind := ev.Kind
	if c.lateAnswers[kind] <= 0 {
		return false
	}
	// AN ANSWER THAT NAMES ITS REQUEST IS NEVER SPENT AGAINST A DEBT.
	// The debt is a guess about which request an answer belongs to, and
	// an id is knowledge; spending the debt against a correlated answer
	// would swallow an answer whose owner is certain, which is the
	// ratchet this mechanism is otherwise accused of.
	//
	// This is also what lets the debt be recorded on a correlating
	// connection at all. It can now only ever fire on an id-LESS answer
	// — exactly the case where the kind is once again the only key,
	// which is every kind a declaring server has not yet echoed.
	if ev.CorrelationID != "" {
		return false
	}
	c.lateAnswers[kind]--
	debugf("spendLateAnswerLocked: spent a late %q against the request that timed out", kind)
	return true
}

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
		case (ev.Kind == "msg" && ev.Answers != nil || ev.Kind == "noMoreMessages") &&
			answersHistoryClaim(ev, c.pendingMessageAfter):
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
		// A token-narrowed claim keeps waiting rather than swallowing
		// somebody else's answer; the mismatched event falls through to
		// the buffer, where it is reported as unsolicited — which is
		// exactly what it is.
		if claim.token != "" && ev.AttachmentToken != claim.token {
			return false
		}
		// Against a correlating server the id decides, not the kind. An
		// answer carrying a DIFFERENT id is the late answer to a request
		// that already gave up, and it falls through to the buffer as
		// the unsolicited event it is — which is what makes the
		// late-answer debt unnecessary here rather than merely unlikely.
		//
		// An answer carrying NO id, from a server that promised to echo
		// one, is the case the feature declaration exists to rule out:
		// it is not this claim's answer as far as anything here can
		// establish, so it is not handed over. Silence is the honest
		// outcome; inheriting it would be the exact misattribution the
		// id was added to end.
		if !idMatches(ev, claim) {
			debugf("tryDivertToClaimLocked: %q carries id %q, claim holds %q — not this claim's answer",
				ev.Kind, ev.CorrelationID, claim.corrID)
			return false
		}
		delete(c.pendingAcks, ev.Kind)
		claim.result <- ev
		return true
	}
	if ev.Kind == "error" && c.correlates {
		// CORRELATED, SO NOT A GUESS. An error naming an id belongs to
		// whichever claim holds that id, however many are pending — the
		// whole ambiguity below simply does not arise.
		if ev.CorrelationID != "" {
			if c.pendingMessageAfter != nil && c.pendingMessageAfter.corrID == ev.CorrelationID {
				claim := c.pendingMessageAfter
				c.pendingMessageAfter = nil
				claim.result <- ev
				return true
			}
			for kind, claim := range c.pendingAcks {
				if claim.corrID == ev.CorrelationID {
					delete(c.pendingAcks, kind)
					claim.result <- ev
					return true
				}
			}
			// The id names a request nobody is waiting for any more.
			return false
		}
		// NO ID, FROM A SERVER THAT ECHOES THEM. Two things look like
		// this and only one is anybody's outcome: a genuinely
		// unsolicited error (a close reason, a refusal with no frame
		// behind it), and wire.CodeBadCorrelation, which refuses a
		// request and cannot echo the id it is bounding.
		//
		// So the code is what distinguishes them, never the absence —
		// reading the absence is the inference this whole feature
		// exists to remove. bad_correlation falls through to the
		// uncorrelated rule below, where a single pending claim owns it
		// and two do not; every other id-less error is unsolicited and
		// is delivered to nobody.
		if ev.Code != wire.CodeBadCorrelation {
			debugf("tryDivertToClaimLocked: error %q with no correlation id from a correlating "+
				"server — unsolicited, delivered to no claim", ev.Code)
			return false
		}
	}
	if ev.Kind == "error" {
		// AMBIGUOUS BY CONSTRUCTION — on a server that does not declare
		// wire.FeatureCorrelation, which is the only way execution
		// reaches here. Such an error carries no id, so with more than
		// one claim pending, nothing can say which operation it refuses — and handing it to whichever the
		// map yields first tells one caller about the other's failure
		// with full confidence. An external reviewer demonstrated
		// exactly that, 2026-09-20: with a history read and a send both
		// outstanding, send_refused was reported to the history caller.
		//
		// So a genuinely ambiguous error is given to NOBODY. It falls
		// through to the buffer, where the reader sees it as what it is,
		// and both callers time out — two honest "no answer"s instead of
		// one confident wrong one. Where exactly ONE claim is pending
		// the attribution is not a guess and the old behaviour stands.
		//
		// THIS WILL READ LIKE A REGRESSION to whoever meets it first:
		// two callers now time out where one used to get an answer. The
		// answer they got was confidently wrong about which operation it
		// described, which is the worse failure of the two and the
		// harder one to notice.
		pending := len(c.pendingAcks)
		if c.pendingMessageAfter != nil {
			pending++
		}
		if pending > 1 {
			debugf("tryDivertToClaimLocked: %d claims pending and an error with no correlation "+
				"id — delivered to none of them", pending)
			return false
		}
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

// expectLateAnswer records that a request of this kind timed out, so the
// answer that may still arrive is not handed to whatever asks next.
//
// Without it, per-kind claims establish ORDER, not ownership: send A
// times out and releases its claim, send B takes one, and A's late
// sendAck matches B by kind alone — B is then told A's outcome and A's
// externalId, which a caller renders to a model as a delivered message.
// Demonstrated by an external reviewer, 2026-09-20.
//
// Spending the debt, rather than merely refusing to divert, is what
// makes this independent of what arrives NEXT: a rule that only refuses
// leaves the outcome depending on whether somebody happens to be
// waiting, where consuming the debt holds regardless of traffic.
//
// THIS IS THE FALLBACK, and it runs only where the wire offers nothing to
// correlate on: an ack carries externalId and ok, an error carries a
// code, and against such a server neither echoes anything the request
// chose. It is the honest half of the trade there — a late answer becomes
// UNATTRIBUTED rather than MISATTRIBUTED. The cost is that B may time out
// on an answer that was really A's, which reports "no answer" about
// something unknown. That is the true state; the alternative was a
// confident wrong one.
//
// A server declaring wire.FeatureCorrelation echoes a client-chosen id on
// the answer and on any error refusing it, which makes the attribution
// exact and this mechanism dead weight — so it is not recorded at all
// there. See Conn.correlates for why the two never run together, and for
// the condition under which this whole mechanism can be deleted.
func (c *Conn) expectLateAnswer(kind string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lateAnswers == nil {
		c.lateAnswers = make(map[string]int)
	}
	c.lateAnswers[kind]++
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
// ackClaimTakes reports whether a pending "ack" claim would actually
// accept ev, which is the same question tryDivertToClaimLocked asks and
// has to be asked HERE too: this function decides between falling through
// (so the claim can be delivered to) and consuming the event as internal
// plumbing. Answering it by the claim's mere existence was right while
// the kind was the only key; against a correlating server an ack bearing
// somebody else's id would fall through, match nothing below, and surface
// to the model as an unsolicited receipt reply — plumbing made visible.
//
// So a claim holding an id takes only that id, and anything else stays
// plumbing and is discarded exactly as a background receipt's reply
// always was. Must be called with c.mu held.
func (c *Conn) ackClaimTakes(ev Event) bool {
	claim, claimed := c.pendingAcks["ack"]
	if !claimed {
		return false
	}
	return idMatches(ev, claim)
}

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
		// here, same as every other ack kind's claim path.
		if c.ackClaimTakes(ev) {
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
		// repeat the same mistake on every future event.
		//
		// UNLESS SOMEBODY ASKED. "It never asked for this ack" is false of
		// an explicit
		// hub_confirm, which is waiting on exactly this answer. Swallowed
		// here, that caller saw only its five-second timeout — and a
		// timeout reports success, so the server's REFUSAL became a
		// confirmation and the refused cursor was persisted: the silent
		// skip bad_ack_cursor exists to prevent, one layer below it.
		// Found by an external reviewer against a real server,
		// 2026-09-20, who then pointed out that checking the kind inside
		// ConfirmReceived did not help while this consumed it first.
		//
		// Delivered to the claim instead, and receipts are NOT disabled:
		// a cursor from another conversation is a caller's mistake about
		// one call, not this client miscounting for the rest of the
		// connection's life.
		if c.ackClaimTakes(ev) {
			return false
		}
		c.ackDisabled = true
		return true
	}
	return false
}

// ConfirmReceived sends an immediate standalone read receipt (wire.Ack)
// for cursor and marks it as this connection's consumed boundary — the
// same lastConfirmed/lastAckSent state Send's
// piggybacking already read (see MarkConsumed's doc comment). Unlike
// those, which a caller drives from its own drain, this is a caller
// asserting the boundary directly: for mcp-hub-client, the model itself,
// via hub_confirm, since nothing else in this client's async delivery
// paths (wait --follow, one-shot wait) can honestly claim the model read
// anything. Sent immediately
// — a caller invoking this wants the receipt to land now, not on the
// next timer.
//
// ackReplyMissThreshold bounds ackReplyMisses — see its own doc comment
// for why this is a streak, not a single miss: a transient slow/lost
// reply must not read as "this server never answers" for the rest of
// the connection's life. Var so tests can shorten the number of stalls
// they need to pay to exercise the ratchet, the same pattern
// confirmReminderInterval already uses for its own
// tunables.
var ackReplyMissThreshold = 2

// Returns the server's reported Behind count from its reply, if one
// arrives (see wire.Ack.Behind's doc comment for why this count is worth
// having: it's measured from the position the model itself just
// asserted, not one inherited from connect time). nil, nil is a normal,
// expected result — a server that doesn't answer standalone acks is not
// an error.
func (c *Conn) ConfirmReceived(cursor string) (*int, error) {
	// WHAT THIS CONNECTION BELIEVES IT HAS READ, remembered so a refusal
	// can put it back. Adopting the cursor before the server has
	// answered meant a REFUSED confirm still left lastConsumed and
	// lastAckSent holding it, so the next send piggybacked the refused
	// cursor as its ackCursor — refused again, and that second refusal
	// arrives with no claim pending, which is the path that silently
	// turns receipts off for the connection's life. Exactly the outcome
	// this morning's fix said it had removed, reached by another route.
	// Found by an external reviewer with a reproduction, 2026-09-20.
	c.mu.Lock()
	prevConsumed, prevAckSent := c.lastConsumed, c.lastAckSent
	prevConfirmed := c.lastConfirmed
	c.mu.Unlock()
	restore := func() {
		c.mu.Lock()
		c.lastConsumed, c.lastAckSent = prevConsumed, prevAckSent
		c.lastConfirmed = prevConfirmed
		c.mu.Unlock()
	}

	c.MarkConsumed([]Event{{Cursor: cursor}})
	// A confirm names a POSITION, so it releases a prefix of the ledger
	// rather than clearing it: bytes delivered after this cursor are still
	// occupying the reader's context, unread rather than freed.
	if c.budget != nil && c.budget.release(c.budgetOwner, cursor) {
		// The confirm just moved the read position past a message the
		// delivery window refused to send. Nothing is lost — the server
		// still holds it and the gap is retrievable — but the reader
		// believes they are caught up, and a gap nobody mentions again is
		// indistinguishable from no gap. Recorded so the caller can say so.
		c.mu.Lock()
		c.skippedHeld = true
		c.mu.Unlock()
	}
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
		if err := c.writeJSON(wire.NewAck(cursor)); err != nil {
			restore()
			return nil, err
		}
		// ADOPTED WITHOUT AN ANSWER, deliberately: this server has
		// declared it does not reply to receipts, so no answer is ever
		// coming and waiting for one would mean never advancing. A
		// decision about what this client believes, not a guess about
		// what happened.
		c.mu.Lock()
		c.lastAckSent = cursor
		c.lastConfirmed = cursor
		c.mu.Unlock()
		return nil, nil
	}
	corrID, resultCh, cancel, err := c.claimNextAckCorrelated("ack", "")
	if err != nil {
		return nil, err
	}
	defer cancel()
	ackFrame := wire.NewAck(cursor)
	ackFrame.ID = corrID
	if err := c.writeJSON(ackFrame); err != nil {
		// Nothing was sent, so nothing was consumed — the skipWait path
		// above already restores here and this one did not.
		restore()
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
		// WHAT ARRIVED DECIDES, not the fact that something did. The
		// claim intercepts the next "ack" OR the next generic error, and
		// this returned success for either — so a server's explicit
		// bad_ack_cursor refusal was consumed here, reported as a
		// confirmation, and the refused cursor was then PERSISTED by the
		// caller. That is the silent skip bad_ack_cursor exists to
		// prevent, reintroduced one layer below it. Found by an external
		// reviewer against a real loopback server, 2026-09-20.
		if ev.Kind == "error" {
			code := ev.Code
			if code == "" {
				code = "no code"
			}
			// Put the position back. "Has not moved" has to be true of
			// what this connection will SAY next, not only of what was
			// persisted — otherwise the refused cursor rides out on the
			// next send's piggybacked receipt.
			restore()
			return nil, fmt.Errorf("the server REFUSED this confirm (%s): %s — your read "+
				"position has not moved", code, ev.Text)
		}
		// An ack that states ok:false is a refusal too, and its cursor
		// is the position the server actually holds rather than an echo
		// of what was sent (see wire.Ack). Absent ok is not a refusal:
		// it is a server that said nothing either way, which every
		// older server does.
		if ev.ActionOKStated && !ev.ActionOK {
			where := ""
			if ev.Cursor != "" {
				where = fmt.Sprintf(" It holds %q for this peer.", ev.Cursor)
			}
			// Same reasoning as the error branch — except that the ack
			// plumbing has already adopted the server's OWN reported
			// position into lastAckSent, which is the right value to
			// keep, so only the consumed mark goes back.
			c.mu.Lock()
			c.lastConsumed = prevConsumed
			c.mu.Unlock()
			return nil, fmt.Errorf("the server did not accept this confirm.%s Your read "+
				"position has not moved", where)
		}
		c.mu.Lock()
		c.lastConfirmed = cursor
		c.mu.Unlock()
		return ev.Behind, nil
	case <-c.gone:
		// Restored so the sentence is true of this connection's own
		// state and not only of what was persisted. The Conn is
		// discarded after a drop, so nothing downstream depends on it —
		// but a message that says a position did not move while it did
		// is the exact class of defect this function was just fixed
		// for, and leaving two instances of it here would be worse than
		// the bug.
		restore()
		return nil, fmt.Errorf("the connection dropped before the server answered this confirm — " +
			"your read position has not moved")
	case <-time.After(AckWaitTimeout):
		c.mu.Lock()
		c.ackReplyMisses++
		c.mu.Unlock()
		// The answer may still be coming. Marked so it is spent rather
		// than handed to whoever confirms next — see expectLateAnswer.
		c.expectLateAnswer("ack")
		// THE POSITION IS KEPT on an unknown outcome, and the two
		// records then disagree by design: the receipt was written, so
		// this client believes it, while the server's own column moves
		// only when a receipt actually arrives and is accepted. If it
		// did not, that column sits behind this one and the next
		// joined.behind counts from THEIRS — over-reporting, and a
		// re-walk of things already seen rather than a skip. Confirmed
		// from the server side by chat-relay's author, 2026-09-20. The
		// safe direction of a disagreement that is invisible until a
		// reconnect, and the reason this line says so.
		c.mu.Lock()
		c.lastConfirmed = cursor
		c.mu.Unlock()
		return nil, nil
	}
}

// THE IDLE RECEIPT LOOP IS GONE, and this is where it was.
//
// It existed because the reported position was lastConsumed, which moved
// silently on every drain and so needed flushing on a timer. The position
// now moves only through ConfirmReceived, and every path there writes its
// receipt before returning — so a timer could only ever find the two
// already equal.
//
// Except once, which is why it is deleted rather than left harmless: when
// a server REFUSES a confirm with ok:false, the ack plumbing adopts the
// server's own reported position into lastAckSent while the confirmed
// position rolls back. A loop comparing the two would then see them
// differ and helpfully re-send the OLD position — a backwards receipt, on
// exactly the servers that refuse those, generated by nobody asking.
//
// A mechanism whose only reachable path does the wrong thing is worse
// than one that never runs.

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
		c.mu.Unlock()
		c.signalActivity()
	}
}

// nameTaken reports whether any of these peers already answers to mine.
//
// Separated from the read loop so it can be tested against the real
// predicate rather than a copy of it: a test that reimplements the rule
// it is checking passes whenever the two copies agree, including when
// both are wrong.
//
// An empty name cannot collide — a connection that gave none is shown as
// a bare peerId, which is unambiguous by construction.
func nameTaken(mine string, others []PeerInfo) bool {
	if mine == "" {
		return false
	}
	return slices.ContainsFunc(others, func(p PeerInfo) bool { return p.Name == mine })
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
			c.mu.Unlock()
			c.signalActivity()
			c.stopActivity()
			return
		}
		ev, ok := decodeEvent(raw)
		if !ok {
			debugf("readLoop: decodeEvent failed, dropping frame raw=%s", raw)
			continue
		}
		c.mu.Lock()
		c.lastFrameKind, c.lastFrameAt = ev.Kind, time.Now()
		// Stamped here, before the ack claim can divert the event, because
		// this is where the connection's own nature is known and the
		// formatter only ever sees the event.
		ev.Mirrored = c.conversationKind != ""
		// THE DEBT IS SETTLED BEFORE ANYTHING ELSE LOOKS AT THIS EVENT,
		// and settling it only stops the DIVERT — see
		// spendLateAnswerLocked. It has to be spendable by an arrival
		// that no claim is waiting for, or a debt recorded by one
		// timeout is still standing when the next request's own answer
		// comes and the connection ratchets into permanent timeouts.
		//
		// The event still goes where it would have gone otherwise: the
		// ack plumbing keeps its bookkeeping, and anything the plumbing
		// does not consume reaches the buffer as the unsolicited event
		// it is. Swallowing it here instead cost the reader the only
		// evidence that its timed-out request had in fact landed.
		lateSpent := c.spendLateAnswerLocked(ev)
		if c.handleAckPlumbingLocked(ev) {
			c.mu.Unlock()
			continue
		}
		if !lateSpent && c.tryDivertToClaimLocked(ev) {
			c.mu.Unlock()
			continue
		}
		if ev.PeerID == SystemPeerIDOperator || ev.PeerID == SystemPeerIDSystem {
			ev.IsOperator = true
		}
		// The wire states the whole membership, this connection included.
		// Everything above works on that list; everything a READER is
		// shown is about the others, so this connection's own entry comes
		// out here — once, rather than at each place that renders or
		// counts it.
		if ev.Kind == "roster" {
			others := make([]PeerInfo, 0, len(ev.RosterPeers))
			for _, p := range ev.RosterPeers {
				if p.ID != c.peerID {
					others = append(others, p)
				}
			}
			// WHETHER THIS PEER'S NAME IS ALREADY TAKEN, decided here
			// because this is the only place that has both the roster
			// and this connection's own name — the formatter is a free
			// function and sees only the event.
			//
			// A display name is how everyone in the room refers to each
			// other; two peers answering to one name makes every later
			// "as X said" ambiguous, and neither of them can tell from
			// their own side, since each sees a roster with the other
			// in it and itself removed.
			ev.RosterNameTaken = nameTaken(c.name, others)
			ev.RosterPeers = others
		}
		c.buffer = append(c.buffer, ev)
		// A buffer nobody is draining is a reader that has stopped, and
		// past a point the honest thing is to stop pretending to be
		// connected — see maxBufferedEvents. Recorded here, acted on
		// after the lock is released, because ending the connection
		// takes a write.
		abandoned := len(c.buffer) > maxBufferedEvents
		if ev.Cursor != "" {
			c.lastSeenCursor = ev.Cursor
			if !c.liveUnconfirmed {
				c.unconfirmedSince = time.Now()
			}
			c.liveUnconfirmed = true
			c.unconfirmedCount++
		}
		switch ev.Kind {
		case "error":
			// Recorded, not merely delivered: the close that follows
			// carries no reason, so by the time anything asks whether a
			// reconnect could work, this is the only thing that knows.
			//
			// ONLY codes about the CONNECTION count. retryable=false
			// answers the request it refuses — a bad_request about one
			// send, a bad_anchor about one read — and treating any of
			// them as fatal disabled automatic reconnect for the whole
			// connection over a refusal that had nothing to do with
			// whether the conversation still exists. The close code
			// remains the primary signal (see relayCloseNotes); this is
			// the secondary one, for a server that says why before it
			// goes.
			if ev.Code != "" && !ev.Retryable && fatalErrorCodes[ev.Code] {
				c.fatalText = fmt.Sprintf("%s (%s)", ev.Text, ev.Code)
			}
		case "serverStopping":
			// Kept so the disconnect that follows can report an estimate.
			// Not what decides "was this graceful" — the close code is,
			// and it arrives whether or not this frame did.
			c.reconnectAfter = ev.ReconnectAfter
		case "roster":
			// The server stated the membership, so this REPLACES what is
			// known rather than adding to it — that is the whole point of
			// sending state instead of deltas, and the reason a client
			// that missed a frame repairs on the next one instead of
			// drifting.
			c.peers = make(map[string]PeerInfo, len(ev.RosterPeers))
			for _, p := range ev.RosterPeers {
				c.peers[p.ID] = p
			}
			c.rosterAnnounced = true
		}
		c.mu.Unlock()
		if abandoned {
			c.abandon()
			c.stopActivity()
			return
		}
		c.signalActivity()
	}
}

// maxBufferedEvents is how much undelivered traffic this connection will
// hold before giving up on being read at all.
//
// Deliberately far above any working session: a reader that is reading
// never approaches it, and the number exists for the case where nothing
// is draining — a harness that stopped accepting deliveries, a follower
// that died unnoticed, a pull client that never calls hub_receive. Left
// alone that buffer grows with traffic and is freed only when the
// process ends, which for a long-lived MCP server is not a bound.
const maxBufferedEvents = 5000

// abandon ends a connection whose reader has plainly stopped.
//
// Dropping the oldest events instead would keep a live connection whose
// stream has a hole in it, and every later message would arrive looking
// perfectly normal — the reader would have no way to know it had missed
// anything. Disconnecting makes the state unambiguous: not connected.
// Nothing is lost, because everything is on the server and the position
// only advances on confirmation, so a reconnect plus catch-up rebuilds
// exactly what was missed.
//
// No special close code. A close frame is a write, and the condition
// being announced is precisely that writes are not being consumed — so
// an ordinary close is what can be relied on. (chat-relay reserved one
// for this and withdrew it for the same reason.)
func (c *Conn) abandon() {
	c.mu.Lock()
	c.abandoned = true
	c.mu.Unlock()
	_ = c.Close()
	// The callback runs AFTER the close, so whoever wakes on it sees a
	// connection that is already ended rather than one about to be.
	c.signalActivity()
}

// AbandonedForBacklog reports whether this connection ended because its
// events were piling up unread, rather than for any other reason. The
// difference matters to a reader: everything else is "the connection
// dropped", while this one is "you were not reading, and reconnecting is
// only worth it if you still want this conversation".
func (c *Conn) AbandonedForBacklog() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.abandoned
}

// DecodeEvent decodes a single raw wire-protocol JSON event into an Event —
// exported so other in-process code that constructs the same wire.* values
// directly (see internal/httpmcp) can reuse this decoding/normalization
// logic instead of duplicating it, by marshaling the wire value and
// decoding it right back rather than hand-rolling a second conversion path.
func DecodeEvent(raw []byte) (Event, bool) {
	return decodeEvent(raw)
}

// statesOK reports whether an ack frame carried an "ok" field at all.
// Go cannot tell an absent bool from an explicit false, so this is read
// from the raw frame rather than from the decoded struct — the only place
// the difference still exists.
func statesOK(raw []byte) bool {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return false
	}
	_, ok := fields["ok"]
	return ok
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
			Mentions: m.Mentions, MentionedMe: m.MentionedMe, Answers: m.Answers,
			Matching: m.Matching, CorrelationID: m.ID}, true
	case wire.TypeServerStopping:
		var st wire.ServerStopping
		if err := json.Unmarshal(raw, &st); err != nil {
			return Event{}, false
		}
		return Event{Kind: "serverStopping", ReconnectAfter: st.ReconnectAfter}, true
	case wire.TypeNoMoreMessages:
		var n wire.NoMoreMessages
		if err := json.Unmarshal(raw, &n); err != nil {
			return Event{}, false
		}
		return Event{Kind: "noMoreMessages", Answers: n.Answers, Matching: n.Matching,
			CorrelationID: n.ID}, true
	case wire.TypeError:
		var e wire.Error
		if err := json.Unmarshal(raw, &e); err != nil {
			return Event{}, false
		}
		return Event{Kind: "error", Text: e.Message, Code: e.Code, Retryable: e.Retryable,
			CorrelationID: e.ID}, true
	case wire.TypeRoster:
		var r wire.Roster
		if err := json.Unmarshal(raw, &r); err != nil {
			return Event{}, false
		}
		members := make([]PeerInfo, 0, len(r.Members))
		for _, m := range r.Members {
			if !wire.IsValidID(m.PeerID) {
				return Event{}, false
			}
			members = append(members, PeerInfo{ID: m.PeerID, Name: m.Name, AgePublicKey: m.AgePublicKey})
		}
		return Event{Kind: "roster", RosterPeers: members, RosterReadAt: r.ReadAt}, true
	case wire.TypeSendAck:
		var a wire.SendAck
		if err := json.Unmarshal(raw, &a); err != nil {
			return Event{}, false
		}
		return Event{Kind: "sendAck", ExternalID: a.ExternalID, ActionOK: a.OK,
			ActionOKStated: statesOK(raw), CorrelationID: a.ID}, true
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
			ReactionAction: a.Action, ActionOK: a.OK, ActionOKStated: statesOK(raw),
			CorrelationID: a.ID}, true
	case wire.TypeEditAck:
		var a wire.EditAck
		if err := json.Unmarshal(raw, &a); err != nil {
			return Event{}, false
		}
		return Event{Kind: "editAck", ExternalID: a.ExternalID, ActionOK: a.OK,
			ActionOKStated: statesOK(raw), CorrelationID: a.ID}, true
	case wire.TypeDeleteAck:
		var a wire.DeleteAck
		if err := json.Unmarshal(raw, &a); err != nil {
			return Event{}, false
		}
		return Event{Kind: "deleteAck", ExternalID: a.ExternalID, ActionOK: a.OK,
			ActionOKStated: statesOK(raw), CorrelationID: a.ID}, true
	case wire.TypePinned:
		var p wire.Pinned
		if err := json.Unmarshal(raw, &p); err != nil {
			return Event{}, false
		}
		return Event{Kind: "pinned", ExternalID: p.ExternalID, ByName: p.By.Name, ByID: p.By.ID, TS: p.At}, true
	case wire.TypeUnpinned:
		var p wire.Unpinned
		if err := json.Unmarshal(raw, &p); err != nil {
			return Event{}, false
		}
		return Event{Kind: "unpinned", ExternalID: p.ExternalID, ByName: p.By.Name, ByID: p.By.ID, TS: p.At}, true
	case wire.TypePinAck:
		var a wire.PinAck
		if err := json.Unmarshal(raw, &a); err != nil {
			return Event{}, false
		}
		return Event{Kind: "pinAck", ExternalID: a.ExternalID, ActionOK: a.OK,
			ActionOKStated: statesOK(raw), CorrelationID: a.ID}, true
	case wire.TypeUnpinAck:
		var a wire.UnpinAck
		if err := json.Unmarshal(raw, &a); err != nil {
			return Event{}, false
		}
		return Event{Kind: "unpinAck", ExternalID: a.ExternalID, ActionOK: a.OK,
			ActionOKStated: statesOK(raw), CorrelationID: a.ID}, true
	case wire.TypePins:
		var p wire.PinsResponse
		if err := json.Unmarshal(raw, &p); err != nil {
			return Event{}, false
		}
		// Never nil: an answer naming no pins is an answer, and a caller
		// must be able to tell it from having received nothing.
		list := []string{}
		if p.List != nil {
			list = *p.List
		}
		return Event{Kind: "pins", PinnedList: list, TS: p.At, CorrelationID: p.ID}, true
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
		// The pointer now carries what statesOK(raw) had to re-derive from
		// the raw JSON: present means the sender stated an outcome,
		// absent means it said nothing, and false is a refusal rather
		// than a default.
		return Event{Kind: "ack", Cursor: a.AckCursor, ActionOK: a.OK != nil && *a.OK,
			Behind: a.Behind, ActionOKStated: a.OK != nil, CorrelationID: a.ID}, true
	case wire.TypeAttachmentData:
		var a wire.AttachmentData
		if err := json.Unmarshal(raw, &a); err != nil {
			return Event{}, false
		}
		return Event{Kind: "attachmentData", AttachmentToken: a.Token, AttachmentName: a.Name,
			AttachmentContentType: a.ContentType, AttachmentContentBytes: a.ContentBytes,
			CorrelationID: a.ID}, true
	default:
		return Event{}, false
	}
}

// ackCursorForOutbound returns the read-receipt cursor to piggyback on the
// next outbound message, and records that it's about to be sent (updating
// lastAckSent) — see the Conn field doc comments for the full contract.
// Every outbound method sends this unconditionally (rule: "piggyback it on
// every outbound message"), even when unchanged from the last one sent;
// unchanged from the last one sent.
// Empty once ackDisabled (a prior bad_ack/bad_ack_cursor) or before
// anything has been consumed yet.
//
// What this sends is lastCONFIRMED — the position the model said it has
// complete — and never lastConsumed, which only means handed over. Those
// are different facts and this column holds the second one. Offering the
// first made this client assert, on the reader's behalf, something the
// reader had not decided, and the reader then met its own assertion
// coming back as a refusal when it answered the confirm reminder
// honestly with a lower position.
//
// It is sent without ever being answered: the server records a
// piggybacked receipt fire-and-forget and replies to nothing about it,
// refusal included. So a wrong value here is invisible from the wire — it
// can only be prevented, never detected. That is why ConfirmReceived
// rolls its position back on every path that does not end in an accepted
// confirm: a position the server refused must never survive to be
// piggybacked, because nothing downstream would ever notice.
//
// A reader that never confirms therefore sends no receipt, and that is
// the intended shape rather than an oversight: the server's column then
// says what the reader actually vouched for. The pressure to confirm
// belongs on the confirm reminder, which exists, and not on this client
// quietly answering for the model.
func (c *Conn) ackCursorForOutbound() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ackDisabled || c.lastConfirmed == "" {
		return ""
	}
	c.lastAckSent = c.lastConfirmed
	return c.lastConfirmed
}

// ConfirmedCursor is the last position the model confirmed, or "" if it
// has confirmed nothing on this connection. This is what a receipt would
// carry, so a caller deciding whether to send one asks this rather than
// ConsumedCursor.
func (c *Conn) ConfirmedCursor() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastConfirmed
}

func (c *Conn) Send(text string, attachments []wire.Attachment, format, replyTo string, mentions []wire.Mention) error {
	return c.sendWithID(text, attachments, format, replyTo, mentions, "")
}

func (c *Conn) sendWithID(text string, attachments []wire.Attachment, format, replyTo string, mentions []wire.Mention, corrID string) error {
	m := wire.NewOutgoingMsg(text)
	m.ID = corrID
	m.AckCursor = c.ackCursorForOutbound()
	m.Attachments = attachments
	m.Format = format
	m.ReplyTo = replyTo
	m.Mentions = mentions
	return c.writeJSON(m)
}

// SendTo sends text privately to a single peer, identified by peerId. The
// server processes this asynchronously: a delivery failure (e.g. an unknown
// or departed peer) does not surface as a returned error here, but as a
// buffered "error" event picked up by a later Peek/Drain.
func (c *Conn) SendTo(text, peerID string, attachments []wire.Attachment, format, replyTo string, mentions []wire.Mention) error {
	return c.sendToWithID(text, peerID, attachments, format, replyTo, mentions, "")
}

func (c *Conn) sendToWithID(text, peerID string, attachments []wire.Attachment, format, replyTo string, mentions []wire.Mention, corrID string) error {
	m := wire.NewOutgoingDirectedMsg(text, peerID)
	m.ID = corrID
	m.AckCursor = c.ackCursorForOutbound()
	m.Attachments = attachments
	m.Format = format
	m.ReplyTo = replyTo
	m.Mentions = mentions
	return c.writeJSON(m)
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
	return c.RequestMessageAfterFiltered(anchor, wire.Filter{})
}

// MessageAfterFilters lists the filter names this server declares it can
// apply, from features.messageAfter's own payload. Empty when the server
// declares none, when it declares messageAfter as a bare marker rather
// than an object, or when it declares no features at all — all of which
// mean the same thing to a caller: do not promise a filter will hold.
//
// This exists so filter support is never inferred from a version number
// or from messageAfter merely being present. The two questions differ:
// messageAfter says the pull path exists, filters says which constraints
// that path can honour.
func (c *Conn) MessageAfterFilters() []string {
	c.mu.Lock()
	raw, ok := c.features["messageAfter"]
	c.mu.Unlock()
	if !ok {
		return nil
	}
	var payload struct {
		Filters []string `json:"filters"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	return payload.Filters
}

// SupportsMessageAfterFilter reports whether the server declared it can
// apply this named filter.
func (c *Conn) SupportsMessageAfterFilter(name string) bool {
	for _, f := range c.MessageAfterFilters() {
		if f == name {
			return true
		}
	}
	return false
}

// RequestMessageAfterFiltered is RequestMessageAfterAwaiting with a
// filter — see wire.Filter. The answer's own Matching says what was
// actually applied, and the caller must read that rather than assume the
// filter it sent was honoured.
func (c *Conn) RequestMessageAfterFiltered(anchor wire.Anchor, filter wire.Filter) (Event, bool, error) {
	ch := make(chan Event, 1)
	c.mu.Lock()
	// REFUSED, not replaced — the same reasoning as claimNextAck. A
	// displaced caller was never delivered to and never told: it waited
	// out its own timeout while the survivor took the first answer to
	// arrive, which may well have been the displaced request's.
	if c.pendingMessageAfter != nil {
		c.mu.Unlock()
		return Event{}, false, fmt.Errorf(
			"another history request is already awaiting its answer on this connection; " +
				"retry once it has returned")
	}
	claim := &ackClaim{result: ch, anchor: anchor}
	if c.correlates {
		claim.corrID = newCorrelationID()
	}
	c.pendingMessageAfter = claim
	c.mu.Unlock()
	debugf("RequestMessageAfterAwaiting: anchor=%+v claim=%p", anchor, claim)
	cancel := func() {
		c.mu.Lock()
		if c.pendingMessageAfter == claim {
			c.pendingMessageAfter = nil
		}
		c.mu.Unlock()
	}
	defer cancel()

	m := wire.MessageAfter{Type: wire.TypeMessageAfter, Anchor: anchor, Filter: filter, ID: claim.corrID}
	if err := c.writeJSON(m); err != nil {
		debugf("RequestMessageAfterAwaiting: claim=%p write error: %v", claim, err)
		return Event{}, false, err
	}
	select {
	case ev := <-ch:
		debugf("RequestMessageAfterAwaiting: claim=%p resolved kind=%q cursor=%q answers=%+v",
			claim, ev.Kind, ev.Cursor, ev.Answers)
		return ev, true, nil
	case <-c.gone:
		return Event{}, false, nil
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
	return c.reactWithID(externalID, reaction, action, "")
}

func (c *Conn) reactWithID(externalID, reaction, action, corrID string) error {
	if err := c.requireAction("reactions"); err != nil {
		return err
	}
	r := wire.NewReactionRequest(externalID, reaction, action)
	r.ID = corrID
	r.AckCursor = c.ackCursorForOutbound()
	return c.writeJSON(r)
}

// EditMessage asks the server to change an earlier message's content,
// identified by externalID — see wire.Edit. Not meaningful for
// mcp-hub-server. Success/failure arrives asynchronously as an "editAck"
// (or an "error" event on refusal), like React. attachments, if non-nil,
// replaces the message's attachments (always the inline form — see
// wire.Edit.Attachments); pass nil to leave existing attachments alone.
func (c *Conn) EditMessage(externalID, text string, attachments []wire.Attachment, format, replyTo string, mentions []wire.Mention) error {
	return c.editMessageWithID(externalID, text, attachments, format, replyTo, mentions, "")
}

func (c *Conn) editMessageWithID(externalID, text string, attachments []wire.Attachment, format, replyTo string, mentions []wire.Mention, corrID string) error {
	if err := c.requireAction("edit"); err != nil {
		return err
	}
	e := wire.NewEditRequest(externalID, text, attachments, format, replyTo, mentions)
	e.ID = corrID
	e.AckCursor = c.ackCursorForOutbound()
	return c.writeJSON(e)
}

// DeleteMessage asks the server to remove an earlier message, identified
// by externalID — see wire.Delete. Not meaningful for mcp-hub-server.
// Success/failure arrives asynchronously as a "deleteAck" (or an "error"
// event on refusal), like React/EditMessage.
func (c *Conn) DeleteMessage(externalID string) error {
	return c.deleteMessageWithID(externalID, "")
}

func (c *Conn) deleteMessageWithID(externalID, corrID string) error {
	if err := c.requireAction("delete"); err != nil {
		return err
	}
	d := wire.NewDeleteRequest(externalID)
	d.ID = corrID
	d.AckCursor = c.ackCursorForOutbound()
	return c.writeJSON(d)
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

// AttachmentWaitTimeout is how long RequestAttachment waits, and it is
// deliberately NOT AckWaitTimeout. An ack is the server saying "yes" to
// something it has already done, so five seconds is generous. An
// attachment reply carries the bytes themselves, and the server may have
// to fetch them from somewhere else — a relay backed by Teams/Graph goes
// out to that API on demand — before it can answer at all. Sizing a
// transfer by the deadline for a "yes" cost a whole report: the request
// timed out at 5s, the data arrived a moment later, and by then the claim
// was gone and the bytes were discarded as unsolicited.
//
// This is the FLOOR, used whenever the served size is unknown, and the
// number comes from the serving side rather than from taste: a relay's
// own outbound writer gives up on a frame it cannot hand to the kernel
// within 20s and closes that peer. Past that point no answer is coming on
// this connection at all, so waiting longer buys nothing. This sits just
// above it — long enough to cover a large frame that is still moving,
// short enough not to outlive the connection that would carry it.
//
// Where the server states a size (wire.Attachment.Size, declared as the
// "attachments.size" feature, added by chat-relay 2026-09-19 at this
// client's request), AttachmentWaitFor scales past this floor: the 20s
// writer ceiling bounds handing ONE frame to the kernel, not the fetch
// from an upstream API that has to happen before there is a frame at
// all, and that part does grow with the file.
var AttachmentWaitTimeout = 30 * time.Second

// attachmentFetchFloorRate is the slowest end-to-end throughput this
// client will still wait out, used only to turn a stated size into a
// deadline. Deliberately pessimistic: the cost of guessing too fast is a
// timeout that discards bytes already on their way — which is the exact
// failure that set AttachmentWaitTimeout apart from AckWaitTimeout — and
// the cost of guessing too slow is waiting a little longer for something
// that was never coming.
const attachmentFetchFloorRate = 1 << 20 // bytes per second

// AttachmentMaxWait bounds the scaling, so a server that misstates a size
// (or states an enormous one) cannot make this client wait indefinitely
// on a single fetch.
var AttachmentMaxWait = 3 * time.Minute

// AttachmentWaitFor is how long to wait for the bytes behind a reference
// whose served length the server stated, or AttachmentWaitTimeout where
// it did not.
//
// A nil size is "the server did not say", which is not the same as zero
// and must not be scaled from: it means this is the old, sizeless
// reference form, and the floor is the only honest bound for it.
func AttachmentWaitFor(size *int64) time.Duration {
	if size == nil || *size <= 0 {
		return AttachmentWaitTimeout
	}
	// Divided BEFORE the multiply. Nanoseconds times a byte count
	// overflows int64 at a few gigabytes and comes back negative — which
	// a misstated size would reach on its own, producing a deadline in
	// the past and a fetch that fails instantly.
	secs := *size / attachmentFetchFloorRate
	if secs > int64(AttachmentMaxWait/time.Second) {
		return AttachmentMaxWait
	}
	d := AttachmentWaitTimeout + time.Duration(secs)*time.Second
	if d > AttachmentMaxWait {
		return AttachmentMaxWait
	}
	return d
}

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
	corrID, resultCh, cancel, claimErr := c.claimNextAckCorrelated("sendAck", "")
	if claimErr != nil {
		return Event{}, false, claimErr
	}
	defer cancel()
	var err error
	if to == "" {
		err = c.sendWithID(text, attachments, format, replyTo, mentions, corrID)
	} else {
		err = c.sendToWithID(text, to, attachments, format, replyTo, mentions, corrID)
	}
	if err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-resultCh:
		return ev, true, nil
	case <-c.gone:
		// The connection dropped. Waiting out the rest of the deadline
		// would delay the teardown that this very callback is about to
		// run — see Conn.gone.
		return Event{}, false, nil
	case <-time.After(AckWaitTimeout):
		// See expectLateAnswer: a late sendAck must not answer the next
		// request of the same kind.
		c.expectLateAnswer("sendAck")
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
	corrID, resultCh, cancel, claimErr := c.claimNextAckCorrelated("reactionAck", "")
	if claimErr != nil {
		return Event{}, false, claimErr
	}
	defer cancel()
	if err := c.reactWithID(externalID, reaction, action, corrID); err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-resultCh:
		return ev, true, nil
	case <-c.gone:
		// The connection dropped. Waiting out the rest of the deadline
		// would delay the teardown that this very callback is about to
		// run — see Conn.gone.
		return Event{}, false, nil
	case <-time.After(AckWaitTimeout):
		// See expectLateAnswer: a late reactionAck must not answer the next
		// request of the same kind.
		c.expectLateAnswer("reactionAck")
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
	corrID, resultCh, cancel, claimErr := c.claimNextAckCorrelated("editAck", "")
	if claimErr != nil {
		return Event{}, false, claimErr
	}
	defer cancel()
	if err := c.editMessageWithID(externalID, text, attachments, format, replyTo, mentions, corrID); err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-resultCh:
		return ev, true, nil
	case <-c.gone:
		// The connection dropped. Waiting out the rest of the deadline
		// would delay the teardown that this very callback is about to
		// run — see Conn.gone.
		return Event{}, false, nil
	case <-time.After(AckWaitTimeout):
		// See expectLateAnswer: a late editAck must not answer the next
		// request of the same kind.
		c.expectLateAnswer("editAck")
		return Event{}, false, nil
	}
}

// RequestAttachment fetches the actual bytes behind a reference-form
// attachment's Token (see wire.Attachment.IsReference) by sending an
// AttachmentRequest and waiting up to AttachmentWaitFor(size) for the server's
// reply — an "attachmentData" event on success, an "error" event
// (bad_attachment/not_found/unavailable) on refusal. Returns (Event{},
// false, nil) on a bare timeout, same convention as SendAwaitingAck and
// friends — most notably including mcp-hub-server's own relay, which
// never emits a Token in the first place and so never replies to this at
// all; a caller should only ever call this for a Token actually seen on
// an Attachment.IsReference()==true entry.
func (c *Conn) RequestAttachment(token string, size *int64) (Event, bool, error) {
	// Narrowed to this token: an attachmentData for any other one is not
	// this request's answer and must not consume this wait. An error
	// event carries no token, so it is still taken as this request's
	// outcome — the only reading available for it.
	corrID, resultCh, cancel, claimErr := c.claimNextAckCorrelated("attachmentData", token)
	if claimErr != nil {
		return Event{}, false, claimErr
	}
	defer cancel()
	req := wire.NewAttachmentRequest(token)
	req.ID = corrID
	if err := c.writeJSON(req); err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-resultCh:
		return ev, true, nil
	case <-c.gone:
		// See Conn.gone: an attachment deadline runs to three minutes,
		// inside the activity callback, so waiting it out on a dead
		// connection delays the teardown and the reconnect by that much.
		return Event{}, false, nil
	case <-time.After(AttachmentWaitFor(size)):
		return Event{}, false, nil
	}
}

// pinnedOrNil flattens the wire's three-state pointer into the same three
// states a caller can read: nil for "no pinning here", an empty slice for
// "pinning exists and nothing is pinned".
func pinnedOrNil(p *[]string) []string {
	if p == nil {
		return nil
	}
	if *p == nil {
		return []string{}
	}
	return *p
}

// PinnedAtConnect is what the server said was pinned when this connection
// opened, by externalId. Nil when the server does not declare "pins" —
// distinct from an empty slice, which means the feature exists and nothing
// is pinned.
//
// A snapshot, deliberately not maintained as a live set here: pinned and
// unpinned events move it on, and Pins() re-reads it authoritatively. A
// client that tracked it internally would hold a set that silently drifts
// the moment one event is missed, which is the failure the pull path
// exists to make repairable.
func (c *Conn) PinnedAtConnect() []string { return c.pinnedAtConnect }

// Pin and Unpin ask the server to change the pinned set. Refused locally
// where the server declares no "pins" feature, rather than sent into
// silence.
func (c *Conn) Pin(externalID string) error {
	return c.pinWithID(externalID, "")
}

func (c *Conn) pinWithID(externalID, corrID string) error {
	if err := c.requireAction("pins"); err != nil {
		return err
	}
	r := wire.NewPinRequest(externalID)
	r.ID = corrID
	r.AckCursor = c.ackCursorForOutbound()
	return c.writeJSON(r)
}

func (c *Conn) Unpin(externalID string) error {
	return c.unpinWithID(externalID, "")
}

func (c *Conn) unpinWithID(externalID, corrID string) error {
	if err := c.requireAction("pins"); err != nil {
		return err
	}
	r := wire.NewUnpinRequest(externalID)
	r.ID = corrID
	r.AckCursor = c.ackCursorForOutbound()
	return c.writeJSON(r)
}

// PinAwaitingAck and UnpinAwaitingAck are Pin/Unpin, but wait for the
// server's own answer where it declares actionAcks — the same division as
// every other write action: "pins" decides whether to send, "actionAcks"
// decides whether an answer is worth waiting for.
func (c *Conn) PinAwaitingAck(externalID string) (Event, bool, error) {
	return c.pinAwaiting(externalID, c.pinWithID, "pinAck")
}

func (c *Conn) UnpinAwaitingAck(externalID string) (Event, bool, error) {
	return c.pinAwaiting(externalID, c.unpinWithID, "unpinAck")
}

func (c *Conn) pinAwaiting(externalID string, send func(externalID, corrID string) error, ackKind string) (Event, bool, error) {
	if !c.WantsActionAcks() {
		return Event{}, false, send(externalID, "")
	}
	corrID, resultCh, cancel, claimErr := c.claimNextAckCorrelated(ackKind, "")
	if claimErr != nil {
		return Event{}, false, claimErr
	}
	defer cancel()
	if err := send(externalID, corrID); err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-resultCh:
		return ev, true, nil
	case <-time.After(AckWaitTimeout):
		return Event{}, false, nil
	}
}

// Pins asks the server for the pinned set as it currently stands. This is
// the repair path for state that is otherwise only pushed: a client that
// missed a pinned or unpinned event has no way to notice, so it needs a
// way to ask rather than a rule saying the set should not drift.
func (c *Conn) Pins() (Event, bool, error) {
	if err := c.requireAction("pins"); err != nil {
		return Event{}, false, err
	}
	corrID, resultCh, cancel, claimErr := c.claimNextAckCorrelated("pins", "")
	if claimErr != nil {
		return Event{}, false, claimErr
	}
	defer cancel()
	r := wire.NewPinsRequest()
	r.ID = corrID
	r.AckCursor = c.ackCursorForOutbound()
	if err := c.writeJSON(r); err != nil {
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
	corrID, resultCh, cancel, claimErr := c.claimNextAckCorrelated("deleteAck", "")
	if claimErr != nil {
		return Event{}, false, claimErr
	}
	defer cancel()
	if err := c.deleteMessageWithID(externalID, corrID); err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-resultCh:
		return ev, true, nil
	case <-c.gone:
		// The connection dropped. Waiting out the rest of the deadline
		// would delay the teardown that this very callback is about to
		// run — see Conn.gone.
		return Event{}, false, nil
	case <-time.After(AckWaitTimeout):
		// See expectLateAnswer: a late deleteAck must not answer the next
		// request of the same kind.
		c.expectLateAnswer("deleteAck")
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
	// Through writeMu, not around it. WriteControl is exempt from the
	// single-writer contract and so would be SAFE unsynchronised — but
	// safety is not the point here: taking the lock orders the goodbye
	// after whatever frame is currently being written, so a caller cannot
	// have a send it believes went out overtaken by the close announcing
	// there will be no more.
	c.writeMu.Lock()
	writeErr := c.ws.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client disconnect"),
		time.Now().Add(writeWait))
	c.writeMu.Unlock()
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
	return FormatEventsBatch(c.applyBudget(events, deliveredCost)), connected
}

// TakeSkippedHeldNotice reports — once — that a confirm moved the read
// position past a message live delivery had withheld, and clears the
// flag. Read once rather than left set, because it describes an event
// that happened, not a state that persists: repeating it after the reader
// has been told would make a resolved gap look like an unresolved one.
func (c *Conn) TakeSkippedHeldNotice() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	skipped := c.skippedHeld
	c.skippedHeld = false
	return skipped
}

// writeJSON is the ONLY way a frame should reach the socket. Every write
// goes through here so the single-writer contract holds by construction
// rather than by each call site remembering.
func (c *Conn) writeJSON(v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// A write with no deadline can block for as long as the peer declines
	// to read, and it blocks holding writeMu — so one stalled send stops
	// every other write on this connection, including the pong that keeps
	// it alive, and it does so before any ack timeout has even started
	// counting. The deadline is the same one the keepalive writes use.
	_ = c.ws.SetWriteDeadline(time.Now().Add(c.writeWait))
	return c.ws.WriteJSON(v)
}

// ShapeForPush renders one event for the push path, spilling an
// oversized body to a file exactly as live delivery does — so a backlog
// message and a live message of the same size arrive the same way, and a
// catch-up cannot hand a reader something live delivery would have
// refused to.
//
// Not charged against the delivery window: this is a backlog the reader
// ASKED for, and the run that drives it has a budget of its own. Charging
// twice would close the live window on the strength of a pull.
func (c *Conn) ShapeForPush(e Event) string {
	if c.budget == nil {
		return NameNotice(c.budgetOwner, FormatEventForPush(e))
	}
	return NameNotice(c.budgetOwner, FormatEventForPush(c.budget.shape(e)))
}

// NoteHandedOver records cursors delivered to the model by a path that
// spends no push budget — hub_catch_up, hub_read, a synchronous tool
// result. It costs nothing and is not optional: a confirm locates a
// prefix by POSITION in the delivery ledger, so a cursor that was pulled
// rather than pushed must still have a position, or confirming it
// releases nothing and the delivery window never reopens.
func (c *Conn) NoteHandedOver(events []Event) {
	if c.budget == nil {
		return
	}
	for _, e := range events {
		c.budget.note(c.budgetOwner, e.Cursor)
	}
}

// ChargeDelivered records what a pushed message actually cost, once the
// caller has finished assembling it. Rendering is not the last step —
// attachment notes are appended afterwards — and a budget that charges
// before the text is final is measuring something the reader never sees.
func (c *Conn) ChargeDelivered(cursor string, n int) {
	if c.budget == nil || cursor == "" {
		return
	}
	c.budget.adjust(c.budgetOwner, cursor, n)
}

// PushItem is one event ready to be pushed into a model harness: the
// formatted text, and the cursor that text can be re-fetched from.
//
// Both fields matter and they are not redundant. The cursor is the
// contract — everything in Text is expected to be recoverable from it, so
// a push that silently fails costs nothing that a catch-up cannot recover.
// Text is the optimization: it saves the reader a round trip it would
// otherwise have to make.
type PushItem struct {
	Cursor string
	Text   string
	// Event is the shaped event this was rendered from, so a caller can do
	// the work that only it can do — resolving and saving attachments,
	// which needs the connection and the local filesystem, neither of
	// which belongs in a formatter.
	Event Event
}

// DrainForPush is DrainBatch for a caller that delivers each event as its
// own message rather than writing them down one stream — it pairs every
// formatted chunk with its cursor instead of discarding it.
//
// The pairing is built here rather than by zipping two lists, because
// FormatEvent renders some events as the empty string and those are
// dropped; indexing a separate cursor list against the surviving chunks
// would silently misalign every cursor after the first such event, which
// is the kind of off-by-one that reports the wrong message as delivered.
func (c *Conn) DrainForPush() (items []PushItem, connected bool) {
	events, connected := c.DrainEvents()
	for _, e := range c.applyBudget(events, pushDeliveredCost) {
		// Numbered before formatting and only for what will actually be
		// pushed: a number that skips because this client declined to
		// render something would report a gap that never existed, which
		// is worse than no number at all.
		if text := FormatEventForPush(e); text == "" {
			continue
		}
		c.mu.Lock()
		c.pushSeq++
		e.PushSeq = c.pushSeq
		c.mu.Unlock()
		text := NameNotice(c.budgetOwner, FormatEventForPush(e))
		items = append(items, PushItem{Cursor: e.Cursor, Text: text, Event: e})
	}
	return items, connected
}

// applyBudget is the only place a delivery is SHAPED rather than merely
// formatted, and it runs on the live push path alone. A reader that asked
// for events — hub_receive, hub_catch_up, hub_read — gets them whole,
// because it chose to spend its own context; charging it would make
// reading expensive, which is the opposite of the intent.
//
// Urgency crosses a closed window. An operator message or a direct mention
// is exactly what must not be held behind a mechanism meant to protect
// attention, or the guard against volume becomes the reason the one
// message that mattered never arrived.
func (c *Conn) applyBudget(events []Event, cost func(Event) int) []Event {
	if c.budget == nil {
		return events
	}
	out := make([]Event, 0, len(events))
	for _, e := range events {
		// Only peer MESSAGES are held. This client's own notices — the
		// held-window announcement, the confirm reminder, roster and
		// shutdown notices — are the escape hatch from a closed window,
		// and holding them meant the mechanism suppressed the only thing
		// that could reopen it. They are a line or two each; withholding
		// them saves nothing worth having.
		if e.Kind == "msg" && !(e.IsOperator || e.MentionedMe) && c.budget.closed() {
			if announce, held := c.budget.hold(c.budgetOwner, e.Cursor); announce {
				out = append(out, Event{Kind: "deliveryHeld", HeldCount: held})
			}
			continue
		}
		shaped := c.budget.shape(e)
		out = append(out, shaped)
		if shaped.Cursor != "" {
			c.budget.charge(c.budgetOwner, shaped.Cursor, cost(shaped))
		}
	}
	return out
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
	events = c.freshenRemindersLocked(c.buffer)
	c.buffer = nil
	connected = !c.closed
	c.mu.Unlock()
	return events, connected
}

// freshenRemindersLocked brings buffered confirm reminders up to date
// with the position as it stands NOW, and drops any whose premise has
// been answered since.
//
// confirmReminderLoop composes a reminder on a ticker and appends it to
// the buffer; the model sees it whenever it next drains. Everything in
// it — the cursor to confirm, how many are outstanding, how long the run
// has been going — is a snapshot of the moment it was composed. Between
// that moment and the drain, the reader can confirm, and then the
// reminder arrives telling it to confirm a position it has already
// confirmed, quoting a count that is no longer true.
//
// Seen live, four times: a reader piggybacked a confirm on its reply, and
// the reminder that followed named the cursor it had just confirmed, once
// with a count of 2 and once with an age that had advanced five minutes
// while nothing arrived. Reported by the peer receiving it, who could see
// the text; the cause is only visible from here.
//
// A reminder that survives is rewritten rather than passed through, since
// a stale count is the same defect one size smaller. Only the last one
// survives: several ticks can pass before a drain, and repeating one
// instruction four times says nothing the first did not.
func (c *Conn) freshenRemindersLocked(evs []Event) []Event {
	out := evs[:0]
	var pending *Event
	for _, e := range evs {
		if e.Kind != "confirmReminder" {
			out = append(out, e)
			continue
		}
		// Resolved since it was composed: there is nothing to remind
		// about, and saying so anyway teaches a reader that this
		// instruction can be ignored.
		if !c.liveUnconfirmed || c.lastSeenCursor == "" {
			continue
		}
		fresh := e
		fresh.Text = c.lastSeenCursor
		fresh.UnconfirmedCount = c.unconfirmedCount
		fresh.UnconfirmedSince = c.unconfirmedSince
		pending = &fresh
	}
	if pending != nil {
		out = append(out, *pending)
	}
	return out
}

// MarkConsumed records the given events' cursors as delivered to the
// model, for the read-receipt system (ack piggybacking, and the
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
// is about to receive directly, mirroring mcptools' session.recordHandedOver's
// identical reasoning and sharing its call site.
// ConsumedCursor is the last cursor this connection has marked consumed,
// or "" if none. Exported so a caller about to send a receipt for an
// OLDER position can decline instead: cursors are opaque, so nothing
// here can order two of them, and "anything already consumed" is the
// only comparison available.
func (c *Conn) ConsumedCursor() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastConsumed
}

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
