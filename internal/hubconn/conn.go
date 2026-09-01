package hubconn

import (
	"encoding/json"
	"fmt"
	"net"
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
)

type Event struct {
	Kind         string
	PeerID       string
	Text         string
	TS           string
	Private      bool
	Name         string
	AgePublicKey string
	// Historical marks a "msg" delivered in answer to RequestHistory rather
	// than live traffic — see wire.Msg.Historical.
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
	// pass it back as History's before to page further past it), or a
	// "messageDeleted"'s, for placing its tombstone (see
	// wire.MessageDeleted.Cursor).
	Cursor string
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
	latestCursor     *string
	historyLimitMax  int
	canSend          bool
	conversationKind string
	topic            *string
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

	// isBridge is true only for a connection made via DialRelay — set once
	// during construction, read only after Dial/DialRelay has returned, so
	// (like peerID/name/etc. above) it needs no locking: the happens-before
	// edge is the same construct-then-return-then-use pattern the rest of
	// this immutable-after-construction group already relies on.
	isBridge bool

	mu              sync.Mutex
	buffer          []Event
	closed          bool
	closeCode       int  // set from the WebSocket close frame's code, if any — see DisconnectNote
	timedOut        bool // set when the connection was dropped by our own pongWait deadline, not a close frame — see DisconnectNote
	onActivity      func()
	peers           map[string]PeerInfo
	rosterAnnounced bool
	// pendingAcks holds at most one outstanding claim per expected ack
	// kind ("sendAck"/"reactionAck"/"editAck") — see claimNextAck.
	pendingAcks map[string]*ackClaim
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
			" this does not by itself indicate which side or which network hop failed)", c.pongWait)
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
	snapPongWait, snapWriteWait := pongWait, writeWait

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
	ws, _, err := websocket.DefaultDialer.Dial(target, nil)
	if err != nil {
		return nil, err
	}
	return finishHandshake(ws, snapPongWait, snapWriteWait, false)
}

// finishHandshake reads the server's initial "joined" message off an
// already-connected ws, validates it, and builds the running Conn —
// the tail shared by Dial and DialRelay, which differ only in how they
// reach an open *websocket.Conn (a normalized host+sessionId URL with no
// custom headers, vs. an arbitrary caller-supplied URL with an
// Authorization/etc. header) and in isBridge, which DialRelay passes true.
func finishHandshake(ws *websocket.Conn, snapPongWait, snapWriteWait time.Duration, isBridge bool) (*Conn, error) {
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
	ws.SetPingHandler(func(appData string) error {
		ws.SetReadDeadline(time.Now().Add(snapPongWait))
		return ws.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(snapWriteWait))
	})

	c := &Conn{
		ws:               ws,
		peerID:           joined.PeerID,
		name:             joined.Name,
		agePublicKey:     joined.AgePublicKey,
		serverVersion:    joined.ServerVersion,
		expectedPeers:    joined.PeerCount,
		peers:            make(map[string]PeerInfo),
		pongWait:         snapPongWait,
		latestCursor:     joined.LatestCursor,
		historyLimitMax:  joined.HistoryLimitMax,
		canSend:          joined.CanSend,
		conversationKind: joined.ConversationKind,
		topic:            joined.Topic,
		isBridge:         isBridge,
	}
	go c.readLoop()
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

// ExpectedPeerCount is how many peers were already in the session at join
// time, as reported by the server's "joined" message — i.e. how many
// peerJoined events make up the initial roster catch-up.
func (c *Conn) ExpectedPeerCount() int { return c.expectedPeers }

// The accessors below only ever return a non-zero/non-nil value for a
// bridge-style session (see wire.Joined) — nil/zero for every
// mcp-hub-server connection, since the server never sets these.

// LatestCursor is the cursor of the newest message the server currently
// holds, or nil if there are none yet (or the server doesn't support
// history at all).
func (c *Conn) LatestCursor() *string { return c.latestCursor }

// HistoryLimitMax is the server's cap on a single History request's
// limit, or zero if the server didn't set one.
func (c *Conn) HistoryLimitMax() int { return c.historyLimitMax }

// CanSend reports whether sending was permitted as of connect time — not
// a guarantee for any send made after, since the underlying policy can
// change mid-session.
func (c *Conn) CanSend() bool { return c.canSend }

// ConversationKind and Topic describe what was joined (e.g.
// "oneOnOne"/"group"/"meeting", and a display name where one exists).
func (c *Conn) ConversationKind() string { return c.conversationKind }
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
	if len(c.pendingAcks) == 0 {
		return false
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
		for kind, claim := range c.pendingAcks {
			delete(c.pendingAcks, kind)
			claim.result <- ev
			return true
		}
	}
	return false
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
		if c.tryDivertToClaimLocked(ev) {
			c.mu.Unlock()
			continue
		}
		c.buffer = append(c.buffer, ev)
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
			Historical: m.Historical, ExternalID: m.ExternalID, Own: m.Own, Cursor: m.Cursor}, true
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
	case wire.TypeHistoryComplete:
		return Event{Kind: "historyComplete"}, true
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
		return Event{Kind: "messageEdited", ExternalID: m.ExternalID, Text: m.Text, TS: m.TS, Own: m.Own}, true
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
	default:
		return Event{}, false
	}
}

func (c *Conn) Send(text string) error {
	return c.ws.WriteJSON(wire.NewOutgoingMsg(text))
}

// SendTo sends text privately to a single peer, identified by peerId. The
// server processes this asynchronously: a delivery failure (e.g. an unknown
// or departed peer) does not surface as a returned error here, but as a
// buffered "error" event picked up by a later Peek/Drain.
func (c *Conn) SendTo(text, peerID string) error {
	return c.ws.WriteJSON(wire.NewOutgoingDirectedMsg(text, peerID))
}

// RequestHistory asks the server for messages preceding before (a
// server-defined cursor; empty means "the most recent limit messages") —
// see wire.History. Not meaningful for mcp-hub-server itself, which has no
// history concept; for a bridge server backed by a channel with real
// retained history. The answering burst arrives as ordinary buffered "msg"
// events (Event.Historical true) terminated by a "historyComplete" event,
// picked up by a later Peek/Drain like anything else.
func (c *Conn) RequestHistory(before string, limit int) error {
	return c.ws.WriteJSON(wire.NewHistoryRequest(before, limit))
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
	return c.ws.WriteJSON(wire.NewReactionRequest(externalID, reaction, action))
}

// EditMessage asks the server to change an earlier message's content,
// identified by externalID — see wire.Edit. Not meaningful for
// mcp-hub-server. Success/failure arrives asynchronously as an "editAck"
// (or an "error" event on refusal), like React.
func (c *Conn) EditMessage(externalID, text string) error {
	return c.ws.WriteJSON(wire.NewEditRequest(externalID, text))
}

// DeleteMessage asks the server to remove an earlier message, identified
// by externalID — see wire.Delete. Not meaningful for mcp-hub-server.
// Success/failure arrives asynchronously as a "deleteAck" (or an "error"
// event on refusal), like React/EditMessage.
func (c *Conn) DeleteMessage(externalID string) error {
	return c.ws.WriteJSON(wire.NewDeleteRequest(externalID))
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
func (c *Conn) SendAwaitingAck(text, to string) (Event, bool, error) {
	if !c.isBridge {
		if to == "" {
			return Event{}, false, c.Send(text)
		}
		return Event{}, false, c.SendTo(text, to)
	}
	resultCh, cancel := c.claimNextAck("sendAck")
	defer cancel()
	var err error
	if to == "" {
		err = c.Send(text)
	} else {
		err = c.SendTo(text, to)
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
func (c *Conn) EditMessageAwaitingAck(externalID, text string) (Event, bool, error) {
	if !c.isBridge {
		return Event{}, false, c.EditMessage(externalID, text)
	}
	resultCh, cancel := c.claimNextAck("editAck")
	defer cancel()
	if err := c.EditMessage(externalID, text); err != nil {
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

func (c *Conn) Close() error {
	return c.ws.Close()
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
// whether the connection is still open.
func (c *Conn) Drain() (formatted string, connected bool) {
	c.mu.Lock()
	events := c.buffer
	c.buffer = nil
	connected = !c.closed
	c.mu.Unlock()
	return FormatEvents(events), connected
}
