package mcptools

import (
	"context"
	"encoding/base64"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/secforge/mcp-hub/internal/agekey"
	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/waiter"
	"github.com/secforge/mcp-hub/internal/wire"
)

// Hub bundles the single active hub connection + wait socket for one
// mcp-hub-client process.
type Hub struct {
	// mu guards conn/waiter/connTarget. Needed because, unlike every other
	// mutation of these fields (which happens synchronously inside a tool
	// call), conn.OnActivity's disconnect callback (see handleConnect) can
	// clear them from the Conn's own background read goroutine at any
	// moment — automatic detection of a dead connection, not just the
	// reactive per-call check a tool handler does.
	mu     sync.Mutex
	conn   *hubconn.Conn
	waiter *waiter.Waiter
	// connTarget is the connstore.Target the active conn was reached
	// through, so teardownIfCurrent/clearActiveConn can mark it
	// disconnected in the store — zero-valued for a connection not tracked
	// there at all (teams_relay_connect; see handleTeamsRelayConnect).
	connTarget connstore.Target
	// lastHandedOverCursor is the contiguous high-water mark of message
	// positions actually returned to the model via hub_catch_up — "the
	// last position before which everything has been handed over", never
	// just "the newest position seen". See handleCatchUp for the full
	// rationale (advance-only-on-hand-over, not on-fetch).
	//
	// Persisted via connstore.Target.Get/Set, keyed by catchUpID
	// (below) — durable across a process restart, which a plain
	// in-memory field wouldn't be. catchUpID, not connTarget, is what a
	// plain hub_connect session AND a teams_relay_connect session (which
	// has no connstore.Target at all — see connTarget's own comment) can
	// both derive a stable identity from; see setCatchUpKey and
	// catchUpKeyForRelay.
	lastHandedOverCursor string
	// seekedSinceConnect records that this connection has already seeked
	// past a large backlog, so it does so at most once. Conn.Behind() is a
	// connect-time snapshot that never moves, so without this every
	// subsequent hub_catch_up would see the same large number, seek again,
	// and overwrite the recorded gap — losing the retrieval progress of
	// the range the first seek skipped.
	seekedSinceConnect bool
	// catchUpID is the current connection's stable identity for
	// lastHandedOverCursor's persistence — set once per successful
	// connect via setCatchUpKey, which also loads whatever was
	// previously persisted for it. The zero value (Valid() false) means
	// "nothing to key persistence on" (not connected, or a connection
	// kind that hasn't called setCatchUpKey).
	catchUpID connstore.Target
	// handedOverAhead is the set of Msg.Cursor values confirmed handed
	// to the model — via a synchronous tool result (hub_receive/
	// hub_wait/hub_catch_up, anything routed through
	// resultWithReceivedAttachments — see recordHandedOver) — at a
	// position AHEAD of lastHandedOverCursor's own contiguous mark.
	// handleCatchUp checks this before showing a message: one already in
	// this set was already shown live, so catch-up advances past it
	// (a confirmed hand-over, so the mark legitimately moves) instead of
	// showing a duplicate. Entries are pruned as handleCatchUp's walk
	// reaches and consumes them; an entry the walk never reaches (a
	// message only ever seen via hub_receive/hub_wait, never followed by
	// a catch-up call that walks past it) is not otherwise pruned by
	// that path — but it IS persisted (connstore.CatchUpState.Ahead,
	// alongside Cursor/Gap) and fully cleared on every hub_confirm, so
	// it stays bounded by traffic since the last confirm rather than by
	// the whole conversation's history.
	handedOverAhead map[string]bool

	// waitMu, waitCancel, and waitGen let a new handleWait call supersede
	// one already in flight, mirroring waiter.Waiter's single-registered-
	// waiter design for the CLI wait socket — see handleWait.
	waitMu     sync.Mutex
	waitCancel context.CancelFunc
	waitGen    uint64

	// attachMu guards attachDir/attachSeq — the local temp directory
	// received image attachments are saved into (see attachmentDir) and a
	// counter for unique filenames within it. Separate from mu since it's
	// touched by handleReceive/handleWait, which don't otherwise need the
	// conn-state lock.
	attachMu  sync.Mutex
	attachDir string
	attachSeq int
}

func NewHub() *Hub {
	return &Hub{}
}

// messageStyleNote is appended to the description of every tool that
// composes outbound message text (hub_send, hub_edit). The other side of a
// hub session is a chat window — often a real one, via a teams relay — where a
// multi-paragraph answer reads as a wall of text.
const messageStyleNote = "\n\nSTYLE: keep the message short — a chat turn, not a " +
	"document. Answer, then stop: no preamble, no restating the question, no summary " +
	"of what you are about to do. A few sentences is normal; several paragraphs is not. " +
	"If the full answer really is long, send the conclusion first and offer the detail " +
	"rather than dumping it unasked."

// mentionsToolDescription documents hub_send/hub_edit's "mentions" parameter
// — a chat-relay extension (see the wire spec's §8), not part of the core
// wire protocol. Kept in sync with httpmcp's description of the same name.
const mentionsToolDescription = "Optional, server-specific: real platform-native @-mentions to " +
	"attach — a chat-relay extension. Each entry sets exactly one of id (the platform's own " +
	"directory id), peerId (a hub peerId, resolved server-side to that peer's own identity), " +
	"or name (a display name — refused if ambiguous, never guessed); optionally text, the exact " +
	"substring already present in this message's own text to turn into the mention (defaults " +
	"to \"@\" + the resolved display name if omitted). A server that doesn't implement this " +
	"simply ignores the field; one that does refuses the WHOLE send/edit (not a partial one " +
	"without the mention) if any entry violates these rules."

// confirmCursorToolDescription documents hub_send/hub_edit's optional
// "confirmCursor" parameter — built 2026-09-08, the project owner's own
// proposal ("part 1"), coordinated live with chat-relay's author and
// customer-portal: rather than this client silently computing what to
// acknowledge from its own lastConsumed bookkeeping on every outbound
// send, the model can state explicitly what it has actually read,
// exactly the deliberate act hub_confirm already requires — this just
// folds it into the send/edit call instead of a separate round trip.
const confirmCursorToolDescription = "Optional: the cursor of the last message you have actually " +
	"read complete and intact, if you want to confirm it as part of this call — has exactly the " +
	"same effect as calling hub_confirm(cursor) first, just without the extra round trip: sends a " +
	"genuine read receipt and advances this session's persisted catch-up position, so a later " +
	"hub_catch_up or reconnect resumes from here. Every send/edit already piggybacks a read " +
	"receipt automatically from this client's own delivery bookkeeping regardless of this field — " +
	"that part isn't new. What this adds is a deliberate, explicit statement of what YOU confirm " +
	"having read, which is what actually advances hub_catch_up's position (the automatic " +
	"piggyback doesn't). On a server that reports it, the result also states how many messages " +
	"remain after the position you confirmed. Only pass a cursor from a message you actually " +
	"received complete — see hub_confirm's own guidance on what \"complete\" means and why " +
	"confirming a truncated one is unsafe."

// parseMentions decodes the "mentions" tool argument (a JSON array of
// objects, as delivered by mcp-go's GetArguments) into wire.Mention
// entries, validating the exactly-one-of id/peerId/name rule client-side
// — malformed input here is a clear MCP-level error, not something to
// discover only via an async server refusal. Kept in sync with httpmcp's
// function of the same name. Nil input is a no-op (no mentions), not an
// error.
func parseMentions(raw any) ([]wire.Mention, error) {
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, fmt.Errorf("mentions must be an array")
	}
	mentions := make([]wire.Mention, 0, len(items))
	for i, item := range items {
		obj, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("mentions[%d] must be an object", i)
		}
		id, _ := obj["id"].(string)
		peerID, _ := obj["peerId"].(string)
		name, _ := obj["name"].(string)
		text, _ := obj["text"].(string)
		set := 0
		for _, v := range []string{id, peerID, name} {
			if v != "" {
				set++
			}
		}
		if set != 1 {
			return nil, fmt.Errorf("mentions[%d] must set exactly one of id, peerId, or name", i)
		}
		mentions = append(mentions, wire.Mention{ID: id, PeerID: peerID, Name: name, Text: text})
	}
	return mentions, nil
}

// startupConnectionsNote returns text to append to hub_connect's own
// description when connstore has any entry still marked Connected from a
// prior process — computed once, when Register() runs, i.e. at process
// launch ("startup"). This is the only mechanism available for surfacing
// this: MCP gives a server no way to push anything into the model's
// context on its own, so the note is only ever seen once the model
// actually looks at hub_connect's description (which, with tool search
// deferring full schemas by default, may not be immediately) — a
// best-effort notice, not a guaranteed one.
func startupConnectionsNote() string {
	entries, err := connstore.List()
	if err != nil {
		return ""
	}
	var openCount int
	for _, e := range entries {
		if e.Entry.Connected {
			openCount++
		}
	}
	if openCount == 0 {
		return ""
	}
	plural := ""
	if openCount != 1 {
		plural = "s"
	}
	return fmt.Sprintf(
		"\n\nNOTE: %d session%s still marked open when this client last ran (it may have "+
			"simply ended mid-conversation, not necessarily crashed) — call "+
			"hub_list_connections() to see them.", openCount, plural)
}

// rootsRequestTimeout bounds how long projectForConnect waits for the MCP
// client's roots/list reply before falling back to connstore.CurrentProject
// — RequestRoots itself has no built-in timeout (it just blocks on ctx or a
// reply), so a client that claims roots support but never actually answers
// would otherwise hang a connect call indefinitely.
var rootsRequestTimeout = 2 * time.Second

// projectForConnect resolves the project scope for a connstore.Target,
// preferring the MCP client's own advertised roots (see mcp-go's
// server.MCPServer.RequestRoots) over connstore.CurrentProject's
// $PWD-based fallback — roots is the actual spec mechanism for "what
// project(s) does the client have open," whereas $PWD is merely what this
// process happened to inherit at launch, which is usually but not
// necessarily the same thing. Falls back silently (no error surfaced) if
// the client has no ClientSession in ctx, doesn't support roots, times
// out, or reports none — connstore.CurrentProject's own fallback already
// degrades safely to "" in the worst case.
func projectForConnect(ctx context.Context) string {
	mcpServer := server.ServerFromContext(ctx)
	if mcpServer != nil {
		rootsCtx, cancel := context.WithTimeout(ctx, rootsRequestTimeout)
		result, err := mcpServer.RequestRoots(rootsCtx, mcp.ListRootsRequest{
			Request: mcp.Request{Method: string(mcp.MethodListRoots)},
		})
		cancel()
		if err == nil && len(result.Roots) > 0 {
			if p := strings.TrimPrefix(result.Roots[0].URI, "file://"); p != "" {
				return p
			}
		}
	}
	return connstore.CurrentProject()
}

// catchUpIDForRelay derives connstore's persistence identity for a
// teams_relay_connect session — connstore has no Target for one (see
// connTarget's comment: teams-session identity/reconnectSecret has
// never been connstore's to track), so this uses connstore.TeamsID
// targetForLink identifies a connection in connstore: the WHOLE link,
// credential included, scoped to one project (see
// connstore.CurrentProject) so two agents on the same machine connected to
// different conversations — or to the same conversation from two different
// project directories — never collide.
//
// The credential has to be part of the key, not stripped from it: on a hub
// link the fragment IS the session id, so keying on the address alone
// would collapse every session on one relay into a single entry sharing
// one identity and one read position. The whole link is the only part of
// what a caller passes that is unique per conversation.
func targetForLink(ctx context.Context, link string) connstore.Target {
	return connstore.Target{Link: link, Project: projectForConnect(ctx)}
}

// setCatchUpGap persists {from, to} as id's current abandoned range,
// overwriting whatever was recorded before (including any retrieval
// progress a previous gap had — a fresh seek means a fresh, unretrieved
// range). Recorded as ongoing STATE, not a one-time notice: a seek's
// skipped range doesn't stop existing once the call that performed it
// returns, so any later "am I caught up" check (a fresh hub_catch_up
// call, a fresh hub_connect) should keep saying so until something
// actually walks that range, not just the one time it happened.
//
// From/To are both timestamps (RFC3339) — From is where the abandoned
// range starts (Conn.BehindSince() at the time of the seek), To is where
// it ends (the seek's own landing point, so everything from there
// onward is already covered by ordinary hub_catch_up).
func setCatchUpGap(id connstore.Target, from, to string) {
	saveCatchUpGap(id, connstore.GapState{From: from, To: to})
}

// saveCatchUpGap persists g as id's current gap record — an empty g (the
// zero value) clears it, since loadCatchUpGap already treats a blank
// From as "no gap recorded."
func saveCatchUpGap(id connstore.Target, g connstore.GapState) {
	if id.Link == "" {
		return
	}
	cs, _ := connstore.GetCatchUp(id)
	if g.From == "" {
		cs.Gap = nil
	} else {
		cs.Gap = &g
	}
	_ = connstore.SetCatchUp(id, cs)
}

// clearCatchUpGap removes id's gap record — called once
// handleCatchUpGap's walk has retrieved everything in the range (a
// "noMoreMessages" answer, or a message whose own timestamp reaches To).
func clearCatchUpGap(id connstore.Target) {
	saveCatchUpGap(id, connstore.GapState{})
}

// discardCatchUpGap writes off id's recorded gap unread, moving it into
// CatchUpState.Discarded rather than deleting it. That distinction is the
// whole point: a range nobody ever read, deliberately, is a different
// fact from a range that was read, and erasing the record would collapse
// the two into "no gap here" — the same silence the gap mechanism exists
// to prevent.
//
// Without this the only exit from a recorded gap is walking it to the end,
// so a large one has no exit a caller can actually take, and a note that
// cannot be acted on is one that gets scrolled past.
func discardCatchUpGap(id connstore.Target) (connstore.GapState, bool) {
	if id.Link == "" {
		return connstore.GapState{}, false
	}
	cs, _ := connstore.GetCatchUp(id)
	if cs.Gap == nil || cs.Gap.From == "" {
		return connstore.GapState{}, false
	}
	discarded := *cs.Gap
	cs.Gap = nil
	cs.Discarded = append(cs.Discarded, connstore.DiscardedGap{
		From: discarded.From, To: discarded.To, At: time.Now().UTC(),
	})
	_ = connstore.SetCatchUp(id, cs)
	return discarded, true
}

// loadCatchUpGap returns id's full currently-recorded gap record, if
// any — including retrieval progress (AnchorCursor), unlike
// getCatchUpGap below which only surfaces the display-facing From/To.
// AnchorCursor is the actual retrieval progress once
// hub_catch_up(gap: true) has walked at least one message into the
// range — see handleCatchUpGap. Empty until then, meaning "resume via
// At: From" (a coarse, inclusive seek); once set, retrieval switches to
// the message's own opaque Cursor for precision, the same
// walk-forward-by-cursor logic the ordinary (non-gap) walk already uses.
func loadCatchUpGap(id connstore.Target) (connstore.GapState, bool) {
	cs, ok := connstore.GetCatchUp(id)
	if !ok || cs.Gap == nil || cs.Gap.From == "" {
		return connstore.GapState{}, false
	}
	return *cs.Gap, true
}

// getCatchUpGap returns id's currently-recorded abandoned range, if
// any — the display-facing half of loadCatchUpGap, for callers (the
// connect/catch-up note text) that only care about From/To, not
// retrieval progress.
func getCatchUpGap(id connstore.Target) (from, to string, ok bool) {
	g, ok := loadCatchUpGap(id)
	if !ok {
		return "", "", false
	}
	return g.From, g.To, true
}

// setCatchUpCursor persists cursor as id's hub_catch_up position,
// preserving id's existing gap/ahead state (a read-modify-write of the
// whole connstore.CatchUpState, not a standalone key, now that the three
// used to live under separate namespaced keys in one string-keyed
// store — see connstore's package doc comment for the 2026-09-08
// rewrite).
func setCatchUpCursor(id connstore.Target, cursor string) {
	if id.Link == "" {
		return
	}
	cs, _ := connstore.GetCatchUp(id)
	cs.Cursor = cursor
	_ = connstore.SetCatchUp(id, cs)
}

// saveHandedOverAhead persists ahead (a snapshot, not a delta) under id,
// overwriting whatever was recorded before. Found live, 2026-09-07,
// coordinating with chat-relay's author and a third party on the hub:
// handedOverAhead used to be purely in-memory, reset to nil by every
// setCatchUpKey call (i.e. every reconnect — see its own comment), which
// is exactly why a reconnect after a drop re-presented messages
// hub_catch_up should have silently skipped as already seen. Persisting
// it makes the skip survive a reconnect the same way lastHandedOverCursor
// already does. Safe to do independently of any live-delivery marker
// work: this set is only ever populated by recordHandedOver, itself only
// reachable via resultWithReceivedAttachments — a genuinely synchronous
// hand-over — so nothing async/best-effort ever enters it. A no-op on
// the zero CatchUpID.
func saveHandedOverAhead(id connstore.Target, ahead map[string]bool) {
	if id.Link == "" {
		return
	}
	cursors := make([]string, 0, len(ahead))
	for c := range ahead {
		cursors = append(cursors, c)
	}
	cs, _ := connstore.GetCatchUp(id)
	cs.Ahead = cursors
	_ = connstore.SetCatchUp(id, cs)
}

// loadHandedOverAhead returns id's persisted set, or nil if there isn't
// one (a fresh id, or one whose set has emptied out entirely — see
// saveHandedOverAhead).
func loadHandedOverAhead(id connstore.Target) map[string]bool {
	cs, ok := connstore.GetCatchUp(id)
	if !ok || len(cs.Ahead) == 0 {
		return nil
	}
	ahead := make(map[string]bool, len(cs.Ahead))
	for _, c := range cs.Ahead {
		ahead[c] = true
	}
	return ahead
}

// activeConn returns the current connection and its wait socket, or (nil,
// nil) if not connected.
func (h *Hub) activeConn() (*hubconn.Conn, *waiter.Waiter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.conn, h.waiter
}

// setActiveConn records a newly established connection as the active one.
// target identifies it in connstore for later teardown bookkeeping — the
// zero Target for a connection kind connstore doesn't track at all.
func (h *Hub) setActiveConn(conn *hubconn.Conn, w *waiter.Waiter, target connstore.Target) {
	h.mu.Lock()
	h.conn, h.waiter, h.connTarget = conn, w, target
	h.mu.Unlock()
}

// setCatchUpKey records id as the current connection's stable identity
// for hub_catch_up's persisted position, and loads whatever was
// previously stored under it (nothing, for a first-ever connection to
// this identity) — called once per successful connect, after
// setActiveConn, with an identity derived from the connection's own
// stable identity: connstore.HubCatchUpID(target) for a plain
// hub_connect session, or catchUpIDForRelay's derivation for a
// teams_relay_connect session (which has no connstore.Target at all —
// see connTarget's own comment).
//
// Every call resets lastHandedOverCursor to match id, even when id is
// unchanged from before — reconnecting re-reads the persisted value
// rather than trusting whatever's already in memory, so a second
// process/session sharing the same identity (or this same process after
// an external edit to the store) can't silently diverge from what's on
// disk. The zero CatchUpID (a connection kind that doesn't call this at
// all) leaves catchUpID/lastHandedOverCursor at their zero values, and
// handleCatchUp treats that the same as "nothing recorded yet."
func (h *Hub) setCatchUpKey(id connstore.Target) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.catchUpID = id
	h.lastHandedOverCursor = ""
	h.seekedSinceConnect = false
	// handedOverAhead's entries are cursors witnessed live for THIS
	// connection's message stream — carrying them into a DIFFERENT
	// identity (a different conversation entirely) would be the same
	// stale-cursor-bleed hazard the old target-based reset guarded
	// against, just one map over. So start from nil, then load whatever
	// this exact identity persisted (see saveHandedOverAhead) — a
	// reconnect to the SAME conversation restores the set, a switch to a
	// different one starts clean.
	h.handedOverAhead = nil
	if id.Link != "" {
		if cs, ok := connstore.GetCatchUp(id); ok {
			h.lastHandedOverCursor = cs.Cursor
		}
		h.handedOverAhead = loadHandedOverAhead(id)
	}
}

// clearActiveConn unconditionally forgets whatever connection is currently
// active and returns it plus the connstore.Target it was reached through,
// for an explicit hub_disconnect — which should tear down whatever is
// active right now, regardless of which Conn instance a caller happens to
// be holding a reference to.
func (h *Hub) clearActiveConn() (*hubconn.Conn, *waiter.Waiter, connstore.Target) {
	h.mu.Lock()
	conn, w, target := h.conn, h.waiter, h.connTarget
	h.conn, h.waiter, h.connTarget = nil, nil, connstore.Target{}
	h.mu.Unlock()
	h.clearAttachDir()
	return conn, w, target
}

// teardownIfCurrent tears down the active connection, but only if it's
// still exactly the Conn passed in. A caller that noticed conn had died —
// including conn's own background read goroutine, via conn.OnActivity — is
// racing against a fresh hub_connect (or an explicit hub_disconnect) that
// may have already replaced or cleared it; without this check, a stale
// notification about a connection nobody cares about anymore could wrongly
// tear down whatever legitimately replaced it. Also marks the connection's
// connstore entry (if any) disconnected — this is the automatic-drop path,
// not just explicit hub_disconnect, so a session that ends this way isn't
// wrongly reported as "still open" by a later process's startup note.
func (h *Hub) teardownIfCurrent(conn *hubconn.Conn) {
	h.mu.Lock()
	if h.conn != conn {
		h.mu.Unlock()
		return
	}
	w := h.waiter
	target := h.connTarget
	h.conn, h.waiter, h.connTarget = nil, nil, connstore.Target{}
	h.mu.Unlock()
	h.clearAttachDir()
	if w != nil {
		w.Close()
	}
	if target != (connstore.Target{}) {
		_ = connstore.MarkDisconnected(target)
	}
}

// disconnectedText renders the bare "hub disconnected" outcome, appending
// conn.DisconnectNote() when the connection ended for a reason more
// specific than an ordinary drop (e.g. a teams relay's close code
// signaling a dead credential) — empty, and so a no-op here, for every
// mcp-hub-server disconnect. Also names the last message cursor this
// connection actually delivered, if any, so a reconnecting client knows it
// isn't strictly necessary to remember it — hub_catch_up's own persisted
// cursor (see connstore's catchup store) already resumes from here on the
// next hub_connect, but naming it explicitly here means a client checking
// this text mid-session doesn't need to trust that silently.
func disconnectedText(conn *hubconn.Conn) string {
	text := "hub disconnected" + conn.DisconnectNote()
	if cursor := conn.LastSeenCursor(); cursor != "" {
		text += fmt.Sprintf("\nLast message cursor you saw on this connection: %q. On your "+
			"next hub_connect, call hub_catch_up() to pick up anything that arrived while "+
			"disconnected — do this by default, don't wait to notice something is missing.", cursor)
	}
	return text
}

func (h *Hub) Register(s *server.MCPServer) {
	s.AddTool(
		mcp.NewTool("hub_connect",
			mcp.WithDescription("Connect to a hub session via a link the user was given — one "+
				"opaque string that identifies both where to connect and what authorizes it. "+
				"The link may address an ordinary hub session or a conversation mirrored from a "+
				"real chat platform (e.g. Microsoft Teams); that is the server's business, not "+
				"something to work out from the link, and hub_send/hub_receive/hub_wait/"+
				"hub_peers/hub_catch_up work the same way either way. The connect result states "+
				"what this particular server declared about itself"+
				startupConnectionsNote()),
			mcp.WithString("link", mcp.Description(
				"REQUIRED: the exact link string the user gave you, unmodified — do not parse, "+
					"reformat, split, or strip anything from it, and do not infer anything about "+
					"the server from how it looks. Keep the exact string: reconnecting after a "+
					"drop presents this same link again. Many links are single-use, and what "+
					"authorizes resuming one is a secret this client stores per link and presents "+
					"for you — there is nothing for you to keep alongside the link")),
			mcp.WithString("name", mcp.Description(
				"Optional untrusted display name, sanitized server-side (control characters "+
					"stripped, length capped). Whether anyone sees it depends on the server: on an "+
					"ordinary hub session other peers do, via hub_peers(); a relay mirroring a "+
					"real conversation may keep it only for its own audit log and never show it to "+
					"the people on the other side. Don't assume it functions as an in-conversation "+
					"display name unless told otherwise")),
			mcp.WithString("agePublicKey", mcp.Description(
				"Optional age (https://age-encryption.org) public key ('age1...'), "+
					"format-validated but otherwise untouched by the hub — it's distributed to "+
					"other peers (via hub_peers()) so they can encrypt to you; the hub itself "+
					"never uses it cryptographically. Deliberately NOT an identity credential: "+
					"every other peer can see it, so granting peerId reuse on it would let anyone "+
					"who saw it impersonate you. Identity is resumed by a secret this client "+
					"manages for you, which no peer ever sees")),
			mcp.WithString("createToken", mcp.Description(
				"Optional, server-specific: a capability for creating and claiming a brand-new "+
					"session in this same handshake, for a server that refuses an unknown session "+
					"by design rather than creating one on first connect. Ignored when the link "+
					"already names a session that exists. The user gives you this token (it's "+
					"shown once when issued); this tool never generates or discovers one")),
			mcp.WithString("topic", mcp.Description(
				"Optional, server-specific: a display name for a session being CREATED (see "+
					"createToken). Meaningless when joining one that already exists — a "+
					"conversation's name is the server's to report, not a client's to set")),
		),
		h.handleConnect,
	)
	s.AddTool(
		mcp.NewTool("hub_send",
			mcp.WithDescription("Send a text message to the current hub session. On a teams "+
				"session (e.g. via hub_connect's link form), this call itself waits briefly for the "+
				"real outcome — the send actually being accepted, or refused — and reports it "+
				"directly rather than a bare confirmation that doesn't mean the send succeeded; if "+
				"nothing arrives in time it falls back to a plain confirmation, with the actual "+
				"outcome then arriving later via wait/hub_receive/hub_wait instead. On a plain "+
				"hub_connect session this always returns immediately, since mcp-hub-server has no "+
				"equivalent asynchronous confirmation to wait for"+messageStyleNote),
			mcp.WithString("text", mcp.Required(), mcp.Description("Message text")),
			mcp.WithString("to", mcp.Description(
				"Optional peerId to send this privately to a single peer instead of "+
					"broadcasting to everyone in the session")),
			mcp.WithString("imagePath", mcp.Description(
				"Optional local filesystem path to an image to attach — read and base64-"+
					"encoded here, not something you inline yourself (saves you the tokens). "+
					"Only .png, .jpg/.jpeg, .gif, .webp are accepted, up to 32MB raw; anything "+
					"else is refused before sending. Not every server supports attachments — a "+
					"server that doesn't will simply ignore this field. Mutually exclusive with "+
					"filePath — pass at most one")),
			mcp.WithString("filePath", mcp.Description(
				"Optional local filesystem path to attach as binary content — any file type, "+
					"not just images (use imagePath for images against a server, like a Teams "+
					"teams relay, that only accepts those). Read and base64-encoded here, up to 32MB "+
					"raw. Works against mcp-hub-server's own relay, which never restricts "+
					"attachment content types; a server that does validate more strictly (e.g. "+
					"images-only) may refuse a non-image sent this way. Mutually exclusive with "+
					"imagePath — pass at most one")),
			mcp.WithString("format", mcp.Description(
				"Optional, server-specific: how to interpret text — \"text\" (default) or "+
					"\"html\" for real bold/lists/code/quotes/tables/links instead of literal "+
					"markdown characters (markdown is NOT interpreted by any server here — "+
					"\"**bold**\" renders as four literal asterisks unless you use format=\"html\" "+
					"against a server that supports it). A server that validates this field "+
					"refuses an unrecognized value outright rather than silently falling back to "+
					"plain text — only pass \"html\" against a server confirmed to accept it. "+
					"mcp-hub-server's own relay ignores this field entirely")),
			mcp.WithString("replyTo", mcp.Description(
				"Optional, server-specific: the externalId of a message this send should be a "+
					"threaded reply/citation to (from an earlier msg/sendAck event) — gets native "+
					"reply UI treatment on a server that supports it, rather than just quoting the "+
					"text yourself. Must name a message the target server actually holds in this "+
					"exact session/conversation; a server that validates it refuses the whole send "+
					"outright (nothing sent) for an unrecognized, foreign, or malformed value, "+
					"since resolving the citation can surface that message's own preview text. "+
					"mcp-hub-server's own relay ignores this field entirely")),
			mcp.WithArray("mentions",
				mcp.Description(mentionsToolDescription),
				mcp.Items(map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id":     map[string]any{"type": "string"},
						"peerId": map[string]any{"type": "string"},
						"name":   map[string]any{"type": "string"},
						"text":   map[string]any{"type": "string"},
					},
				}),
			),
			mcp.WithString("confirmCursor", mcp.Description(confirmCursorToolDescription)),
		),
		h.handleSend,
	)
	s.AddTool(
		mcp.NewTool("hub_disconnect",
			mcp.WithDescription("Disconnect from the current hub session")),
		h.handleDisconnect,
	)
	s.AddTool(
		mcp.NewTool("hub_list_connections",
			mcp.WithDescription("List the links this client has connected to before FROM THIS "+
				"PROJECT, with the peerId and display name each one last used, the "+
				"conversation's name where the server reported one, and whether it is still "+
				"marked open from a connect that never got an explicit hub_disconnect. "+
				"Read-only, no side effects. Never includes the secret behind an identity — "+
				"that stays out of your context by design; you don't need it, hub_connect "+
				"presents it for you")),
		h.handleListConnections,
	)
	s.AddTool(
		mcp.NewTool("hub_receive",
			mcp.WithDescription("Drain and return currently buffered hub events without blocking. "+
				"An image attached to a received message is saved to a local temp file, not "+
				"inlined as base64 — the result names the path; read that file yourself (e.g. "+
				"with a Read tool) to view it. The file is removed automatically on disconnect")),
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
		mcp.NewTool("hub_catch_up",
			mcp.WithDescription("Read the single next message you missed. A server that doesn't "+
				"implement messageAfter (including every mcp-hub-server, and any teams relay "+
				"that hasn't added it yet) silently ignores the request; this call then times out "+
				"after a few seconds and reports that rather than any message — the connect result's "+
				"protocol-version note explains what to tell the server operator in that case. "+
				"On a server that DOES support it: always returns exactly one message, or a plain "+
				"\"caught up\" result — never a batch, and never more than one. This deliberately "+
				"does NOT take a limit — there is no larger-page option here, "+
				"because the whole point of this call is that a returned message can never hide "+
				"beside another one. Call it in a loop — read what comes back in full, then call it "+
				"again — until you "+
				"see \"caught up\"; that terminator is unambiguous and distinct from a timeout or a "+
				"refusal, so you never have to infer completion from silence. If you were behind by "+
				"more than a small threshold (or have no prior position recorded for this process — "+
				"see the tool's own known-limitations note), this seeks to recent context first "+
				"rather than walking a potentially huge backlog message-by-message; the skipped "+
				"range isn't lost, just not walked through by this call — pass gap: true to retrieve "+
				"it later. Errors if not connected."),
			mcp.WithBoolean("discardGap", mcp.Description(
				"If true, WRITE OFF the recorded skipped range instead of retrieving it: those "+
					"messages stay unread permanently and nothing will mention them again. Use "+
					"this only when the backlog genuinely doesn't matter for what you're doing — "+
					"it is a deliberate decision to proceed on incomplete history, and it is "+
					"recorded as such against this connection, so it reads later as a choice "+
					"rather than as there having been nothing to read. Retrieving the range "+
					"instead (gap: true) is always available and always safer; prefer it unless "+
					"the range is large enough that walking it is the real problem. Does nothing "+
					"if there is no recorded gap. Ignored if false or omitted")),
			mcp.WithBoolean("gap", mcp.Description(
				"If true, retrieve the RECORDED SKIPPED RANGE from an earlier seek (see the connect/"+
					"catch-up note about one) instead of continuing from your normal position — the "+
					"two are independent, so gap retrieval never re-delivers or interferes with what "+
					"you've already read normally, and vice versa. Same one-message-per-call contract "+
					"as ordinary hub_catch_up: call repeatedly until it reports the gap fully "+
					"retrieved. If there's no recorded gap for this session, reports that and does "+
					"nothing. Ignored if false or omitted (the default, ordinary behavior)")),
		),
		h.handleCatchUp,
	)
	s.AddTool(
		mcp.NewTool("hub_confirm",
			mcp.WithDescription("Explicitly confirm you received a message INTACT, by its cursor — "+
				"for when you've been reading mostly via wait --follow (or the wait CLI's one-shot "+
				"mode), where nothing else tells this session that a live-delivered message was "+
				"actually handed to you, as opposed to merely written to a socket you may not have "+
				"read from yet. Advances this session's persisted catch-up position to cursor and "+
				"sends an immediate read receipt on the wire. Unlike hub_receive/hub_wait/"+
				"hub_catch_up, this does NOT return or re-deliver any message content — it only "+
				"marks a position you already saw as confirmed, so a later hub_catch_up (including "+
				"after a reconnect) resumes from here instead of re-walking everything back to your "+
				"last synchronous call. Only pass a cursor from a message whose body you actually "+
				"received COMPLETE — if it looked truncated, cut off, or otherwise wrong, do NOT "+
				"confirm it; call hub_catch_up instead so the position stays put and a later walk "+
				"can re-deliver it properly. Calling this periodically while reading mostly via "+
				"wait --follow bounds how much gets re-walked after a drop, without needing to make "+
				"a synchronous hub_receive/hub_wait call just to checkpoint. On a server that "+
				"reports it, the result also states how many messages remain after the position "+
				"you confirmed. Errors if not connected."),
			mcp.WithString("cursor", mcp.Required(), mcp.Description(
				"The opaque cursor of the last message you received intact — copy it verbatim from "+
					"that message's own \"cursor=...\" field, rendered inline wherever a message is "+
					"delivered. Everything at or before this position is marked confirmed handed "+
					"over; never construct, guess, or advance this value yourself.")),
		),
		h.handleConfirmReceived,
	)
	s.AddTool(
		mcp.NewTool("hub_react",
			mcp.WithDescription("Add or remove a reaction on an earlier message — only where the "+
				"server declares it can do this; against one that doesn't, the call is refused "+
				"here with that reason rather than sent into silence. Errors if not "+
				"connected. Where a server answers actions, this call waits briefly for the real "+
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
			mcp.WithDescription("Change an earlier message's content — only where the server declares "+
				"it can do this; against one that doesn't, the call is refused here with that "+
				"reason rather than sent into silence. Typically only possible on a message this "+
				"connection itself sent — platform rules usually restrict editing to your own "+
				"messages, and that's enforced by the platform, not pre-judged here. Errors if not "+
				"connected. On a teams session this call itself waits briefly for the real outcome "+
				"and reports it directly, falling back to an async confirmation (see hub_react) if "+
				"nothing arrives in time"+messageStyleNote),
			mcp.WithString("externalId", mcp.Required(), mcp.Description(
				"The target message's externalId, from an earlier msg or sendAck event")),
			mcp.WithString("text", mcp.Required(), mcp.Description("The new message content")),
			mcp.WithString("imagePath", mcp.Description(
				"Optional local file path to an image (.png/.jpg/.jpeg/.gif/.webp, 32MB raw max) "+
					"that REPLACES this message's attachments — there is no way to keep some and "+
					"add more, or to remove attachments while leaving the text alone. Omit entirely "+
					"to leave existing attachments exactly as they are; this is different from "+
					"passing an empty value, which this tool treats the same as omitting it (there "+
					"is deliberately no way to clear attachments via edit). Mutually exclusive "+
					"with filePath")),
			mcp.WithString("filePath", mcp.Description(
				"Optional local file path to any file (not just images, 32MB raw max) that "+
					"REPLACES this message's attachments — see hub_send's filePath for the full "+
					"contract. Same replace-only semantics as imagePath above. Mutually "+
					"exclusive with imagePath")),
			mcp.WithString("format", mcp.Description(
				"Optional, server-specific: how to interpret the new text — \"text\" (default) "+
					"or \"html\". See hub_send's format parameter for the full contract; applies "+
					"the same way here")),
			mcp.WithString("replyTo", mcp.Description(
				"Optional, server-specific: set/replace this message's threaded reply/citation "+
					"target (an externalId — see hub_send's replyTo for the full contract). Unlike "+
					"attachments, most servers can add or change a citation on an existing message "+
					"even though they cannot add an image on edit — but that is server-specific, "+
					"not guaranteed here")),
			mcp.WithArray("mentions",
				mcp.Description(mentionsToolDescription+" Omitted entirely leaves existing "+
					"mentions as they are, same as attachments — there is no way to clear mentions "+
					"via edit either."),
				mcp.Items(map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id":     map[string]any{"type": "string"},
						"peerId": map[string]any{"type": "string"},
						"name":   map[string]any{"type": "string"},
						"text":   map[string]any{"type": "string"},
					},
				}),
			),
			mcp.WithString("confirmCursor", mcp.Description(confirmCursorToolDescription)),
		),
		h.handleEdit,
	)
	s.AddTool(
		mcp.NewTool("hub_delete",
			mcp.WithDescription("Remove an earlier message — only where the server declares it can do "+
				"this; against one that doesn't, the call is refused here with that reason rather "+
				"than sent into silence. Typically only possible on a message this "+
				"connection itself sent, same as hub_edit; enforced by the platform, not pre-judged "+
				"here. This is a genuine deletion, not an edit to empty text — the platform renders "+
				"a tombstone rather than a blank message, and other clients learn about it via a "+
				"distinct messageDeleted event, not an edited one. Errors if not connected. On a "+
				"teams session this call itself waits briefly for the real outcome and reports it "+
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
// which differs between hub_connect (the same sessionId) and
// teams_relay_connect (the same link used the first time). The secret
// behind either is this client's own and never the model's to present.
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
			// running to clear it — normally torn down automatically (see
			// the OnActivity wiring below) well before a caller gets here,
			// but don't make the caller issue hub_disconnect itself in the
			// rare case it hasn't yet.
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
	agePublicKey := req.GetString("agePublicKey", "")
	if agePublicKey != "" && !agekey.Valid(agePublicKey) {
		return mcp.NewToolResultError("agePublicKey is not a validly formatted age public key"), nil
	}

	target := targetForLink(ctx, link)
	stored, _ := connstore.Get(target)
	// The secret is mandatory on the wire but never the caller's to
	// remember: reuse the one stored for this link, or mint one now.
	// Presenting the same one every time is what keeps the identity
	// resumable, so a caller passing nothing must not mean sending nothing.
	reconnectSecret := stored.ReconnectSecret
	if reconnectSecret == "" {
		reconnectSecret = uuid.NewString()
		if err := connstore.SetReconnectSecret(target, reconnectSecret); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("could not record this connection's identity: %v", err)), nil
		}
	}

	conn, err := hubconn.Dial(link, hubconn.DialOptions{
		ReconnectSecret: reconnectSecret,
		AgentID:         stored.PeerID,
		Name:            name,
		AgePublicKey:    agePublicKey,
		CreateToken:     req.GetString("createToken", ""),
		Topic:           req.GetString("topic", ""),
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("connect failed: %v", err)), nil
	}
	w, err := waiter.Listen(conn)
	if err != nil {
		conn.Close()
		return mcp.NewToolResultError(fmt.Sprintf("could not start wait socket: %v", err)), nil
	}
	conn.OnActivity(func() {
		w.Poke()
		if !conn.Connected() {
			// The read loop that just invoked us is the one that detected
			// this — a silent drop, a server-side close, anything short of
			// our own hub_disconnect(). Tear down proactively rather than
			// leaving a dead-but-unnoticed connection (and its wait socket
			// still listening) around until the next tool call.
			h.teardownIfCurrent(conn)
		}
	})
	h.setActiveConn(conn, w, target)
	h.setCatchUpKey(target)

	topic := ""
	if t := conn.Topic(); t != nil {
		topic = *t
	}
	_ = connstore.Upsert(target, connstore.Entry{
		PeerID: conn.PeerID(), Name: conn.Name(), Topic: topic,
		ReconnectSecret: reconnectSecret, LastConnectedAt: time.Now().UTC(), Connected: true,
	})

	waitBlock := buildWaitBlock(ctx, w, "reconnect via hub_connect with the same link — your identity resumes automatically, there is no secret for you to keep")

	// A conversation mirrored from a real chat platform is the one thing
	// that genuinely changes what a caller should expect, and the server
	// says so itself via conversationKind. Nothing here is inferred from
	// the link.
	mirrored := conn.ConversationKind() != ""

	var rosterNote string
	who, whose := "peer", "session"
	if mirrored {
		who, whose = "participant", "conversation"
	}
	if n := conn.ExpectedPeerCount(); n == 0 {
		rosterNote = fmt.Sprintf("No other %ss are in this %s yet.", who, whose)
	} else {
		rosterNote = fmt.Sprintf(
			"%d other %s(s) already in this %s — you'll get a \"roster complete\" "+
				"notification (via wait/hub_receive) once you've caught up on who they are; "+
				"call hub_peers() after that to see the list.", n, who, whose)
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
	// What to say about identity is decided by comparing what the server
	// actually assigned against what was asked for — never by what this
	// client had stored. Telling a model it kept an identity it did not
	// keep is the failure being avoided here: joined.peerId is the only
	// answer to "who am I", and it can disagree with the request.
	switch {
	case stored.PeerID == "":
		identityNote += "\nThis is a first connection to this link, so the identity the server " +
			"assigned has been recorded for it — connecting again with the same link asks for " +
			"this same peerId back. Nothing about that is yours to remember or pass."
	case conn.PeerID() == stored.PeerID:
		identityNote += "\nYour previous identity here was resumed: the server reassigned the " +
			"same peerId this link last used. Nothing about that is yours to remember or pass."
	default:
		identityNote += fmt.Sprintf(
			"\nNOTE: this connection asked for the peerId this link last used (%s) and the "+
				"server assigned a DIFFERENT one (%s). To everyone else in the session you are "+
				"a new participant, so anything keyed to the old identity — read position on "+
				"the server's side, which messages count as your own — does not carry over. "+
				"The new one has been recorded and will be asked for next time.",
			stored.PeerID, conn.PeerID())
	}

	notes := ""
	if mirrored {
		notes = "\nThis mirrors a real chat conversation, not an ordinary hub session — some " +
			"things behave differently: a directed hub_send (`to`) has no meaning here and " +
			"will be refused rather than delivered; sends may be routinely refused for policy " +
			"reasons (e.g. a participant outside the relay's home organization) — expected, " +
			"not a bug, and won't succeed on retry; peerJoined/peerLeft reflect real " +
			"membership changes, not other clients connecting; and this link may be " +
			"single-use — keep the exact link, since resuming after a drop means presenting " +
			"it again. What authorizes that resumption is stored and presented for you."
		notes += "\nConversation kind: " + conn.ConversationKind()
		if topic != "" {
			notes += fmt.Sprintf(" (%q)", topic)
		}
		if conn.CanSend() {
			notes += "\nSending is currently permitted here — but that is a snapshot from " +
				"connect time, not a guarantee: policy is re-checked on every send, so a later " +
				"hub_send can still come back refused."
		} else {
			notes += "\nSending is NOT currently permitted here (e.g. a policy restriction such " +
				"as an external participant) — a hub_send will be refused. Expected and " +
				"routine, not a bug; don't compose a message assuming it can be sent without " +
				"checking, and don't retry a refused send."
		}
	}
	notes += "\nREQUIRED: call hub_catch_up() before doing anything else — do this " +
		"unconditionally on every connect, whether this is a fresh session or a reconnect, " +
		"don't wait until something looks missing. It resumes from wherever this identity last " +
		"left off (persisted across restarts), or reports there's nothing to catch up on."

	return mcp.NewToolResultText(fmt.Sprintf(
		"Connected as peer %s.\n%s\n%s%s%s%s%s",
		conn.PeerID(), rosterNote, waitBlock, versionNote, identityNote, notes, h.behindNote(conn),
	)), nil
}

// behindNote surfaces wire.Joined.Behind/BehindSince, when a server set
// them, as an explicit connect-time statement rather than something a
// model has to notice is missing — a server-reported fact should be
// stated, not left to be inferred or discovered later. Empty when a
// server didn't set Behind (including every mcp-hub-server).
func (h *Hub) behindNote(conn *hubconn.Conn) string {
	note := ""
	if conn.Behind() > 0 {
		if conn.Behind() > catchUpSeekThreshold && conn.BehindSince() != "" {
			note += fmt.Sprintf("\nYou were away: %d messages arrived since %s, more than this "+
				"client walks one at a time — call hub_catch_up (if this server supports "+
				"messageAfter) and it will seek to recent context rather than read the whole "+
				"backlog. What it skips is recorded and retrievable with "+
				"hub_catch_up(gap: true); it is not dropped.",
				conn.Behind(), conn.BehindSince())
		} else {
			note += fmt.Sprintf("\nYou were away: %d message(s) arrived since %s — call "+
				"hub_catch_up (if this server supports messageAfter) to read them one at a time.",
				conn.Behind(), conn.BehindSince())
		}
	}
	h.mu.Lock()
	id := h.catchUpID
	h.mu.Unlock()
	if from, to, ok := getCatchUpGap(id); ok {
		note += fmt.Sprintf("\nAlso still on record: an earlier catch-up seek skipped the range %s "+
			"to %s rather than walk it. Not lost — still on the server — call "+
			"hub_catch_up(gap: true) to retrieve it.", from, to)
	}
	return note
}

// attachmentExtension picks a file extension to save a received
// attachment under: the images-only names this predates (kept first
// since they're a known-good, deterministic mapping — mime.ExtensionsByType
// can return more than one candidate or an installation-dependent answer
// for the same type), then a best-effort fallback via the standard
// library's mime package for any other content type, then ".bin" if even
// that comes up empty (an unrecognized or missing content type is not a
// reason to fail the save — a generic byte blob is exactly as usable
// under a generic extension as under no extension at all).
func attachmentExtension(contentType string) string {
	switch contentType {
	case "image/png":
		return ".png"
	case "image/jpeg":
		return ".jpg"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	}
	if exts, err := mime.ExtensionsByType(contentType); err == nil && len(exts) > 0 {
		return exts[0]
	}
	return ".bin"
}

// attachmentDir lazily creates (once per connection) the local temp
// directory received attachments are saved into — under os.TempDir(),
// removed entirely by clearAttachDir when the connection tears down. Not
// created eagerly at connect time, so a session that never receives an
// attachment never touches disk for this.
func (h *Hub) attachmentDir() (string, error) {
	h.attachMu.Lock()
	defer h.attachMu.Unlock()
	if h.attachDir != "" {
		return h.attachDir, nil
	}
	dir, err := os.MkdirTemp("", "mcp-hub-attachments-")
	if err != nil {
		return "", err
	}
	h.attachDir = dir
	return dir, nil
}

// clearAttachDir removes the local temp directory (if any) used for the
// connection that just tore down — called from clearActiveConn and
// teardownIfCurrent, the two chokepoints every disconnect path (explicit
// hub_disconnect, automatic dead-connection detection, process Shutdown)
// already goes through, so saved attachments never outlive the session
// that received them.
func (h *Hub) clearAttachDir() {
	h.attachMu.Lock()
	dir := h.attachDir
	h.attachDir = ""
	h.attachMu.Unlock()
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
}

// resolveAttachment returns a's actual bytes + content type, fetching them
// via conn.RequestAttachment first if a is the reference form (see
// wire.Attachment.IsReference) — a chat-relay-style server delivers only a
// token on the msg/messageEdited itself, not inline bytes; mcp-hub-server's
// own relay never does this, so this is a no-op fetch-skip for it.
func (h *Hub) resolveAttachment(conn *hubconn.Conn, a wire.Attachment) (raw []byte, contentType string, err error) {
	if !a.IsReference() {
		raw, err = base64.StdEncoding.DecodeString(a.ContentBytes)
		return raw, a.ContentType, err
	}
	ev, ok, err := conn.RequestAttachment(a.Token)
	if err != nil {
		return nil, "", fmt.Errorf("attachment request failed: %w", err)
	}
	if !ok {
		return nil, "", fmt.Errorf("no reply to attachment request within %v", hubconn.AckWaitTimeout)
	}
	if ev.Kind == "error" {
		return nil, "", fmt.Errorf("server refused attachment request (code=%s): %s", ev.Code, ev.Text)
	}
	raw, err = base64.StdEncoding.DecodeString(ev.AttachmentContentBytes)
	return raw, ev.AttachmentContentType, err
}

// saveReceivedAttachments resolves (see resolveAttachment) and writes each
// attachment found across events — of any content type, not just images —
// to a local temp file (see attachmentDir), and returns text describing
// where each one landed, to append to the formatted event text. Unlike
// the HTTP-MCP endpoint (which has no local-filesystem relationship to a
// remote caller and so returns inline content blocks instead — an image
// as ImageContent, anything else as an EmbeddedResource/BlobResourceContents),
// mcp-hub-client runs right next to the model — handing back a path it
// can read with its own file tool costs far fewer tokens than embedding
// base64 in every tool result, especially across a long-running hub_wait
// loop.
func (h *Hub) saveReceivedAttachments(conn *hubconn.Conn, events []hubconn.Event) string {
	var b strings.Builder
	for _, ev := range events {
		for _, a := range ev.Attachments {
			raw, contentType, err := h.resolveAttachment(conn, a)
			if err != nil {
				fmt.Fprintf(&b, "\n\n[attachment on the message from %s at %s could not be fetched: %v]",
					ev.PeerID, ev.TS, err)
				continue
			}
			dir, err := h.attachmentDir()
			if err != nil {
				fmt.Fprintf(&b, "\n\n[attachment on the message from %s at %s could not be saved locally: %v]",
					ev.PeerID, ev.TS, err)
				continue
			}
			h.attachMu.Lock()
			h.attachSeq++
			seq := h.attachSeq
			h.attachMu.Unlock()
			// Prefer the attachment's own filename when it has one (a
			// generic file benefits far more from this than an image
			// does) — sanitized to base name only, so a maliciously
			// path-like server-supplied Name (e.g. "../../etc/passwd")
			// can't escape attachmentDir. The extension always comes from
			// the actually-resolved contentType, never from whatever
			// extension Name happens to carry: a reference-form
			// attachment's Name reflects the ORIGINAL sender's filename,
			// but a recoding server (chat-relay decodes and re-encodes
			// every image) can legitimately serve different bytes under a
			// different contentType than that name implies — trusting the
			// name's extension there would mislabel the file actually
			// written to disk.
			ext := attachmentExtension(contentType)
			base := fmt.Sprintf("attachment-%d%s", seq, ext)
			if a.Name != "" {
				cleaned := filepath.Base(a.Name)
				if cleaned != "." && cleaned != string(filepath.Separator) {
					stem := strings.TrimSuffix(cleaned, filepath.Ext(cleaned))
					base = fmt.Sprintf("%d-%s%s", seq, stem, ext)
				}
			}
			path := filepath.Join(dir, base)
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				fmt.Fprintf(&b, "\n\n[attachment on the message from %s at %s could not be saved locally: %v]",
					ev.PeerID, ev.TS, err)
				continue
			}
			fmt.Fprintf(&b, "\n\n[attachment on the message from %s at %s: saved to %s (%s, %d bytes) — "+
				"read the file to view/use it]", ev.PeerID, ev.TS, path, contentType, len(raw))
		}
	}
	return b.String()
}

// resultWithReceivedAttachments wraps formatted plus
// saveReceivedAttachments' output for events into a single text tool
// result. Every synchronous delivery of events to the model (hub_receive,
// hub_wait, hub_catch_up) goes through this one function, which is
// exactly why recordHandedOver and conn.MarkConsumed both live here
// rather than being called separately at each site — one place that
// can't be forgotten at a new call site later. See MarkConsumed's doc
// comment for why the wire-level read-receipt boundary needs the same
// synchronous-hand-over guarantee recordHandedOver already relies on.
func (h *Hub) resultWithReceivedAttachments(conn *hubconn.Conn, formatted string, events []hubconn.Event) *mcp.CallToolResult {
	h.recordHandedOver(events)
	conn.MarkConsumed(events)
	return mcp.NewToolResultText(formatted + h.saveReceivedAttachments(conn, events))
}

// recordHandedOver marks each event's own Cursor as confirmed delivered
// to the model — see handedOverAhead's doc comment for what this enables
// (hub_catch_up deduping a message already shown live) and why it's safe
// to record with full confidence here specifically: this function is
// only ever reached via resultWithReceivedAttachments, i.e. a
// SYNCHRONOUS tool result, which — unlike wait --follow's async
// notification path — IS the delivery, not a best-effort guess at one.
func (h *Hub) recordHandedOver(events []hubconn.Event) {
	h.mu.Lock()
	changed := false
	for _, e := range events {
		if e.Cursor == "" {
			continue
		}
		if h.handedOverAhead == nil {
			h.handedOverAhead = make(map[string]bool)
		}
		h.handedOverAhead[e.Cursor] = true
		changed = true
	}
	var id connstore.Target
	var snapshot map[string]bool
	if changed {
		id = h.catchUpID
		snapshot = make(map[string]bool, len(h.handedOverAhead))
		for c := range h.handedOverAhead {
			snapshot[c] = true
		}
	}
	h.mu.Unlock()
	if changed {
		saveHandedOverAhead(id, snapshot)
	}
}

// readAttachmentParam resolves hub_send/hub_edit's imagePath (images
// only, works against any server that relays attachments — including
// chat-relay) and filePath (any file, works against mcp-hub-server's own
// relay, which never validates attachment content types — a server that
// does, like chat-relay, may refuse a non-image sent this way) into a
// single attachments slice, erroring if both are given at once rather
// than silently picking one.
func readAttachmentParam(req mcp.CallToolRequest) ([]wire.Attachment, error) {
	imagePath := req.GetString("imagePath", "")
	filePath := req.GetString("filePath", "")
	if imagePath != "" && filePath != "" {
		return nil, fmt.Errorf("pass at most one of imagePath and filePath, not both")
	}
	if filePath != "" {
		return wire.ReadFileAttachment(filePath)
	}
	return wire.ReadAttachmentFile(imagePath)
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
	attachments, err := readAttachmentParam(req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	mentions, err := parseMentions(req.GetArguments()["mentions"])
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	behindNote := ""
	if confirmCursor := req.GetString("confirmCursor", ""); confirmCursor != "" {
		behind, err := h.confirmCursor(conn, confirmCursor)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("confirmCursor failed: %v", err)), nil
		}
		behindNote = formatBehindNote(behind)
	}
	ev, ok, err := conn.SendAwaitingAck(text, to, attachments, req.GetString("format", ""), req.GetString("replyTo", ""), mentions)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("send failed: %v", err)), nil
	}
	if ok {
		// A teams connection's own outcome (sendAck on success, an error
		// event on refusal) arrived in time — report it directly rather
		// than a bare "sent" that doesn't actually confirm anything on a
		// teams session. See hubconn.Conn.SendAwaitingAck.
		return mcp.NewToolResultText(hubconn.FormatEvent(ev) + behindNote), nil
	}
	if !conn.WantsActionAcks() {
		if to == "" {
			return mcp.NewToolResultText("sent" + behindNote), nil
		}
		return mcp.NewToolResultText("sent (private)" + behindNote), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf(
		"sent — no acknowledgement within %v; check wait/hub_receive/hub_wait for the actual "+
			"outcome (a sendAck or an error) rather than assuming this succeeded%s", hubconn.AckWaitTimeout, behindNote,
	)), nil
}

func (h *Hub) handleDisconnect(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, w, target := h.clearActiveConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	if w != nil {
		w.Close()
	}
	conn.Close()
	if target != (connstore.Target{}) {
		_ = connstore.MarkDisconnected(target)
	}
	return mcp.NewToolResultText("disconnected"), nil
}

// Shutdown tears down the active connection exactly as handleDisconnect
// does (same reasoning: closing the wait socket makes a backgrounded
// `wait --follow` CLI process see its connection end and exit on its own,
// rather than lingering as a background process the harness has to warn
// about) — but is meant to be called once, at process shutdown, not from
// a tool call. A no-op if nothing is connected.
func (h *Hub) Shutdown() {
	conn, w, target := h.clearActiveConn()
	if conn == nil {
		return
	}
	if w != nil {
		w.Close()
	}
	conn.Close()
	if target != (connstore.Target{}) {
		_ = connstore.MarkDisconnected(target)
	}
}

func (h *Hub) handleListConnections(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	entries, err := connstore.ListForProject(projectForConnect(ctx))
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("could not list stored connections: %v", err)), nil
	}
	if len(entries) == 0 {
		return mcp.NewToolResultText("no stored connections for this project"), nil
	}
	lines := make([]string, 0, len(entries))
	for _, le := range entries {
		line := fmt.Sprintf("link=%s peerId=%s", le.Target.Link, le.Entry.PeerID)
		if le.Entry.Name != "" {
			line += fmt.Sprintf(" name=%q", le.Entry.Name)
		}
		if le.Entry.Topic != "" {
			line += fmt.Sprintf(" topic=%q", le.Entry.Topic)
		}
		line += fmt.Sprintf(" lastConnectedAt=%s", le.Entry.LastConnectedAt.Format(time.RFC3339))
		if le.Entry.Connected {
			line += " (still marked open)"
		}
		if gap := le.Entry.CatchUp.Gap; gap != nil && gap.From != "" {
			line += fmt.Sprintf("\n    unretrieved gap: %s to %s — hub_catch_up(gap: true) once connected", gap.From, gap.To)
		}
		for _, d := range le.Entry.CatchUp.Discarded {
			line += fmt.Sprintf("\n    gap written off unread: %s to %s (decided %s)",
				d.From, d.To, d.At.Format(time.RFC3339))
		}
		lines = append(lines, line)
	}
	return mcp.NewToolResultText(strings.Join(lines, "\n")), nil
}

func (h *Hub) handleReceive(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, _ := h.activeConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	events, connected := conn.DrainEvents()
	formatted := hubconn.FormatEvents(events)
	if !connected {
		h.teardownIfCurrent(conn)
		// Still surface anything that arrived right before the disconnect
		// (e.g. a final message buffered just ahead of the read loop
		// erroring out) instead of silently discarding it in favor of a
		// bare "hub disconnected" — the caller can always tell the two
		// apart since disconnected-with-content still ends with the note.
		if formatted != "" {
			return h.resultWithReceivedAttachments(conn, formatted+"\n\n"+disconnectedText(conn), events), nil
		}
		return mcp.NewToolResultText(disconnectedText(conn)), nil
	}
	if formatted == "" {
		return mcp.NewToolResultText("no messages"), nil
	}
	return h.resultWithReceivedAttachments(conn, formatted, events), nil
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
			events, connected := conn.DrainEvents()
			formatted := hubconn.FormatEvents(events)
			if !connected {
				h.teardownIfCurrent(conn)
				if formatted == "" {
					return mcp.NewToolResultText(disconnectedText(conn)), nil
				}
				return h.resultWithReceivedAttachments(conn, formatted+"\n\n"+disconnectedText(conn), events), nil
			}
			return h.resultWithReceivedAttachments(conn, waitAgainReminder+formatted, events), nil
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

// catchUpSeekThreshold is how large a gap (Conn.Behind, reported at
// connect) triggers seeking to recent context instead of walking
// message-by-message from a known position — found live, 2026-09-04:
// walking is cheap for a short gap but a peer back after days offline
// could otherwise cost thousands of round trips to reach the tail.
// Tunable for round-trip cost/context, unlike catch-up's own per-call
// bound (always exactly one message — see handleCatchUp), which is
// correctness-critical and not meant to be adjusted the same way.
const catchUpSeekThreshold = 20

// catchUpSeekWindow is how far back from "now" a seek lands when the
// threshold is exceeded (or when there's no known position to walk from
// at all — see handleCatchUp's KNOWN GAP note) — recent enough to be
// useful context, short enough that walking forward from it to live
// stays a handful of calls.
const catchUpSeekWindow = 10 * time.Minute

// handleCatchUp implements the bound=1 "read the next message" pull
// primitive — see wire.MessageAfter's doc comment for the full protocol
// contract, and Hub.lastHandedOverCursor's for why the bound is exactly
// one message and never a batch: a returned tool result is the hand-over
// moment, and a message skimmed inside a larger batch would be
// permanently marked as seen and never re-delivered — this is what
// actually happened live, 2026-09-04, in a 50-message hub_history page.
//
// lastHandedOverCursor is persisted (connstore.Target.Get/Set, keyed
// by catchUpID — see setCatchUpKey) and survives a process restart, for
// both a plain hub_connect session and a teams_relay_connect one (see
// catchUpIDForRelay for the latter's identity derivation, since
// connstore has no Target for a teams session at all).
//
// NOT YET IMPLEMENTED: dedup of a live message that arrived (and
// was shown) while a gap was still open — the day's "suppress only if it
// could also have advanced the cursor" rule needs shared state between
// this call and the live delivery path (DrainEvents/Peek) — see
// recordHandedOver and handedOverAhead. This is only reachable for the
// synchronous-tool-result paths (hub_receive/hub_wait/hub_catch_up
// itself, everything routed through resultWithReceivedAttachments): the
// async wait --follow notification path has no read-ack (see the design
// doc's live-emission-pacing section), so a message delivered only that
// way is never added to handedOverAhead and can still be shown again by
// a later catch-up walk that reaches it — a visible, cheap duplicate
// (deduped by the model itself on externalId, if it cares to), which is
// the accepted-safe outcome when hand-over genuinely can't be confirmed.
//
// catchUpDedupSkipLimit bounds the internal skip loop below: capped
// rather than unbounded so a very long run of already-seen messages (a
// heavy hub_receive/hub_wait user) can't turn one hub_catch_up call into
// an unbounded number of server round trips.
const catchUpDedupSkipLimit = 20

func (h *Hub) handleCatchUp(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, _ := h.activeConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	if !conn.Connected() {
		h.teardownIfCurrent(conn)
		return mcp.NewToolResultText(disconnectedText(conn)), nil
	}

	if req.GetBool("discardGap", false) {
		h.mu.Lock()
		catchUpIDNow := h.catchUpID
		h.mu.Unlock()
		discarded, ok := discardCatchUpGap(catchUpIDNow)
		if !ok {
			return mcp.NewToolResultText(
				"no recorded gap for this session — nothing to discard, and nothing was changed"), nil
		}
		return mcp.NewToolResultText(fmt.Sprintf(
			"[hub: gap %s to %s written off UNREAD at your request. Those messages were never "+
				"retrieved and now never will be by this session — they remain on the server, but "+
				"nothing here will mention them again. The decision is recorded against this "+
				"connection so it stays visible as a choice rather than looking like there was "+
				"never a gap. Normal hub_catch_up is unaffected.]",
			discarded.From, discarded.To)), nil
	}

	if req.GetBool("gap", false) {
		h.mu.Lock()
		catchUpIDNow := h.catchUpID
		h.mu.Unlock()
		return h.handleCatchUpGap(conn, catchUpIDNow)
	}

	h.mu.Lock()
	cursor := h.lastHandedOverCursor
	catchUpIDNow := h.catchUpID
	alreadySeeked := h.seekedSinceConnect
	h.mu.Unlock()

	// Included in every terminal "you're done" message below, not just
	// the seek that created it — a recorded gap doesn't stop existing
	// just because this particular call didn't create or mention it, and
	// "caught up" claimed without it would be exactly the "acted on
	// incomplete as if complete" failure this whole feature exists to
	// avoid. See setCatchUpGap's doc comment.
	gapNote := ""
	if from, to, ok := getCatchUpGap(catchUpIDNow); ok {
		gapNote = fmt.Sprintf("\n[hub: note — an earlier catch-up seek also skipped %s to %s, "+
			"still on the server but not yet walked — call hub_catch_up(gap: true) to retrieve it]", from, to)
	}

	var anchor wire.Anchor
	var seekNote string
	switch {
	// A large backlog is seeked past rather than walked, whether or not a
	// position is known: walking is cheap for a short gap, but a peer back
	// after days offline would otherwise spend thousands of round trips
	// reaching the tail one message at a time.
	//
	// Conditional on being able to RECORD what gets skipped. A seek that
	// cannot be recorded is a silent skip, and the whole point here is
	// that a skipped range is both announced and retrievable — so with no
	// BehindSince to anchor the record, this walks instead however far
	// behind it is. The range starts at the server's own last-acked
	// position, which can sit earlier than what this client has actually
	// handed over; retrieving it then re-delivers a few already-seen
	// messages, which is the safe direction to err in.
	case !alreadySeeked && conn.Behind() > catchUpSeekThreshold && conn.BehindSince() != "":
		seekAt := time.Now().UTC().Add(-catchUpSeekWindow).Format(time.RFC3339)
		anchor = wire.Anchor{At: seekAt}
		h.mu.Lock()
		id := h.catchUpID
		h.seekedSinceConnect = true
		h.mu.Unlock()
		setCatchUpGap(id, conn.BehindSince(), seekAt)
		seekNote = fmt.Sprintf(
			"You were %d messages behind — seeking to recent context (%s) instead of walking the "+
				"whole backlog. Everything before that point is not lost, just not fetched here: "+
				"it remains reachable from the server, this call just didn't walk through it. This "+
				"gap (%s to %s) is now recorded and will keep being mentioned until something "+
				"actually walks it — call hub_catch_up(gap: true) to retrieve it.\n\n",
			conn.Behind(), seekAt, conn.BehindSince(), seekAt)
	case cursor != "":
		anchor = wire.Anchor{Cursor: cursor}
	case conn.Behind() > 0:
		seekAt := time.Now().UTC().Add(-catchUpSeekWindow).Format(time.RFC3339)
		anchor = wire.Anchor{At: seekAt}
		seekNote = "No prior position recorded for this session — seeking to recent context " +
			"instead of walking from the start.\n\n"
	default:
		return mcp.NewToolResultText(
			"nothing to catch up — no prior position recorded and the server reports nothing behind; "+
				"live traffic will arrive normally"+gapNote,
		), nil
	}

	for skipped := 0; skipped < catchUpDedupSkipLimit; skipped++ {
		ev, ok, err := conn.RequestMessageAfterAwaiting(anchor)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("catch-up request failed: %v", err)), nil
		}
		if !ok {
			return mcp.NewToolResultText(seekNote +
				"catch-up request timed out waiting for the server — call hub_catch_up again to retry"), nil
		}
		switch ev.Kind {
		case "noMoreMessages":
			return mcp.NewToolResultText(seekNote +
				"[hub: caught up — no more messages after your last known position]" + gapNote), nil
		case "error":
			return mcp.NewToolResultError(
				fmt.Sprintf("catch-up refused (code=%s, retryable=%t): %s", ev.Code, ev.Retryable, ev.Text),
			), nil
		case "msg":
			h.mu.Lock()
			alreadyHandedOver := ev.Cursor != "" && h.handedOverAhead[ev.Cursor]
			h.mu.Unlock()
			if alreadyHandedOver {
				// Already shown to the model via a synchronous
				// hub_receive/hub_wait — the hand-over moment already
				// happened, just not through this call. Advance past it
				// silently (this IS a confirmed hand-over, so the mark
				// legitimately moves) and try the next position instead
				// of showing a duplicate the model has already read.
				h.mu.Lock()
				h.lastHandedOverCursor = ev.Cursor
				delete(h.handedOverAhead, ev.Cursor)
				id := h.catchUpID
				snapshot := make(map[string]bool, len(h.handedOverAhead))
				for c := range h.handedOverAhead {
					snapshot[c] = true
				}
				h.mu.Unlock()
				setCatchUpCursor(id, ev.Cursor)
				saveHandedOverAhead(id, snapshot)
				anchor = wire.Anchor{Cursor: ev.Cursor}
				continue
			}
			// The hand-over moment: this synchronous tool result IS the
			// delivery, so the persisted position advances right here,
			// not when the server answered — see lastHandedOverCursor's
			// comment. Persisted immediately (not just kept in memory)
			// so a process restart resumes from here rather than
			// falling back to a seek.
			if ev.Cursor != "" {
				h.mu.Lock()
				h.lastHandedOverCursor = ev.Cursor
				id := h.catchUpID
				h.mu.Unlock()
				setCatchUpCursor(id, ev.Cursor)
			}
			formatted := seekNote + hubconn.FormatEvent(ev) +
				"\n\n[hub: more may remain — call hub_catch_up again; you'll be told \"caught up\" once " +
				"there's nothing further]"
			return h.resultWithReceivedAttachments(conn, formatted, []hubconn.Event{ev}), nil
		default:
			return mcp.NewToolResultError(fmt.Sprintf("unexpected catch-up response kind %q", ev.Kind)), nil
		}
	}
	return mcp.NewToolResultText(seekNote + fmt.Sprintf(
		"[hub: skipped %d already-seen message(s) without finding a new one — call hub_catch_up "+
			"again to continue]", catchUpDedupSkipLimit)), nil
}

// handleCatchUpGap implements hub_catch_up(gap: true) — retrieving a
// previously-recorded seek's skipped range, requested directly by the
// project owner, 2026-09-07: until this existed, getCatchUpGap's note
// was purely informational (see handleCatchUp's gapNote) — the range was
// "still on the server" in name only, since nothing in this client could
// actually walk back into it. This can, independently of the ordinary
// (non-gap) walk: it reads its own persisted position (catchUpGap.
// AnchorCursor, falling back to the coarser catchUpGap.From timestamp
// until the first message is retrieved) rather than
// Hub.lastHandedOverCursor, so retrieving the gap never re-delivers or
// otherwise interferes with normal hub_catch_up progress, and vice
// versa. Same bound=1, same dedup-skip-loop shape as the ordinary walk,
// for the identical reason (see handleCatchUp's own doc comment on why
// a returned result is never a batch).
//
// Closing condition: a "noMoreMessages" answer, or a retrieved message
// whose own TS reaches catchUpGap.To (the seek's landing point) — from
// there on, ordinary hub_catch_up already covers everything, so
// continuing to walk the gap would just re-deliver what normal catch-up
// already has (or will). TS comparison is best-effort string comparison
// (both are RFC3339 UTC on this client's own side; a server's own
// message TS format may vary) — if it's ever wrong, the failure mode is
// walking a little further than strictly needed, not losing anything.
func (h *Hub) handleCatchUpGap(conn *hubconn.Conn, id connstore.Target) (*mcp.CallToolResult, error) {
	gap, ok := loadCatchUpGap(id)
	if !ok {
		return mcp.NewToolResultText(
			"no recorded gap for this session — nothing to retrieve. hub_catch_up (without gap: " +
				"true) resumes normal reading.",
		), nil
	}

	anchor := wire.Anchor{At: gap.From}
	if gap.AnchorCursor != "" {
		anchor = wire.Anchor{Cursor: gap.AnchorCursor}
	}

	for skipped := 0; skipped < catchUpDedupSkipLimit; skipped++ {
		ev, ok, err := conn.RequestMessageAfterAwaiting(anchor)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("gap retrieval failed: %v", err)), nil
		}
		if !ok {
			return mcp.NewToolResultText(
				"gap retrieval request timed out waiting for the server — call " +
					"hub_catch_up(gap: true) again to retry",
			), nil
		}
		switch ev.Kind {
		case "noMoreMessages":
			clearCatchUpGap(id)
			return mcp.NewToolResultText(fmt.Sprintf(
				"[hub: gap fully retrieved — nothing more between %s and %s. Normal hub_catch_up "+
					"already covers everything from here onward]", gap.From, gap.To,
			)), nil
		case "error":
			return mcp.NewToolResultError(
				fmt.Sprintf("gap retrieval refused (code=%s, retryable=%t): %s", ev.Code, ev.Retryable, ev.Text),
			), nil
		case "msg":
			h.mu.Lock()
			alreadyHandedOver := ev.Cursor != "" && h.handedOverAhead[ev.Cursor]
			h.mu.Unlock()
			reachedEnd := gap.To != "" && ev.TS != "" && ev.TS >= gap.To
			if alreadyHandedOver {
				// Same reasoning as the ordinary walk's dedup branch —
				// already shown to the model via a synchronous call, so
				// advance past it silently rather than re-present it. Also
				// prunes the entry the same way the ordinary walk's own
				// dedup branch already does — found live, 2026-09-08,
				// coordinating with chat-relay's author and customer-portal
				// on the hub: this branch used to leave the entry in
				// handedOverAhead forever (only the ordinary walk's mirror
				// branch pruned), so a session doing most of its gap
				// retrieval through repeated hub_catch_up(gap: true) calls
				// (as happened live while chasing the decodeEvent bug)
				// accumulated dead entries without bound.
				h.mu.Lock()
				delete(h.handedOverAhead, ev.Cursor)
				aheadID := h.catchUpID
				snapshot := make(map[string]bool, len(h.handedOverAhead))
				for c := range h.handedOverAhead {
					snapshot[c] = true
				}
				h.mu.Unlock()
				saveHandedOverAhead(aheadID, snapshot)
				if reachedEnd {
					clearCatchUpGap(id)
					return mcp.NewToolResultText(fmt.Sprintf(
						"[hub: gap fully retrieved (the remainder was already shown to you earlier) — "+
							"nothing more between %s and %s]", gap.From, gap.To,
					)), nil
				}
				gap.AnchorCursor = ev.Cursor
				saveCatchUpGap(id, gap)
				anchor = wire.Anchor{Cursor: ev.Cursor}
				continue
			}
			gap.AnchorCursor = ev.Cursor
			if reachedEnd {
				clearCatchUpGap(id)
			} else {
				saveCatchUpGap(id, gap)
			}
			formatted := hubconn.FormatEvent(ev)
			if reachedEnd {
				formatted += fmt.Sprintf(
					"\n\n[hub: gap fully retrieved — this was the last message between %s and %s]",
					gap.From, gap.To,
				)
			} else {
				formatted += fmt.Sprintf(
					"\n\n[hub: more of the gap (%s to %s) may remain — call hub_catch_up(gap: true) "+
						"again to continue retrieving it]", gap.From, gap.To,
				)
			}
			return h.resultWithReceivedAttachments(conn, formatted, []hubconn.Event{ev}), nil
		default:
			return mcp.NewToolResultError(fmt.Sprintf("unexpected gap retrieval response kind %q", ev.Kind)), nil
		}
	}
	return mcp.NewToolResultText(fmt.Sprintf(
		"[hub: skipped %d already-seen message(s) in the gap without finding a new one — call "+
			"hub_catch_up(gap: true) again to continue]", catchUpDedupSkipLimit)), nil
}

// handleConfirmReceived implements hub_confirm — a model-issued read
// receipt, the primitive converged on live, 2026-09-07, coordinating with
// chat-relay's author and a third party on the hub: nothing in this
// client's async delivery paths (wait --follow, one-shot wait) can
// honestly claim the model read a message, only that this process wrote
// it onward (see hubconn.Conn.MarkConsumed's doc comment) — so ackLoop's
// automatic receipt and the server-side Behind/behindSince it can feed
// both report on delivery, not consumption, no matter how truthfully
// this client tries to compute them otherwise. A tool the model calls
// itself closes that gap by construction: the call can only originate
// once the model has actually seen cursor.
func (h *Hub) handleConfirmReceived(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	conn, _ := h.activeConn()
	if conn == nil {
		return mcp.NewToolResultError("not connected"), nil
	}
	if !conn.Connected() {
		h.teardownIfCurrent(conn)
		return mcp.NewToolResultText(disconnectedText(conn)), nil
	}
	cursor, err := req.RequireString("cursor")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	behind, err := h.confirmCursor(conn, cursor)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("confirm failed: %v", err)), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf(
		"confirmed handed over up to %q — persisted; a future hub_catch_up or reconnect resumes "+
			"from here instead of re-walking anything at or before it%s", cursor, formatBehindNote(behind),
	)), nil
}

// formatBehindNote renders a standalone ack's reported Behind count, if
// the server sent one — see wire.Ack.Behind and Conn.ConfirmReceived's
// doc comments. nil (the common case against a server that doesn't
// support this) renders nothing; the caller shouldn't imply a count that
// was never actually answered.
func formatBehindNote(behind *int) string {
	if behind == nil {
		return ""
	}
	if *behind == 0 {
		return " — you are fully caught up from this position"
	}
	return fmt.Sprintf(" — %d message(s) remain after this position", *behind)
}

// confirmCursor is hub_confirm's actual effect, factored out so
// hub_send/hub_edit can offer the exact same guarantee inline via their
// own optional confirmCursor parameter — built 2026-09-08, the project
// owner's own proposal ("part 1"), coordinated live with chat-relay's
// author and customer-portal: rather than the client silently
// auto-computing what to acknowledge from its own lastConsumed
// bookkeeping on every outbound send, the model can state explicitly
// what it has actually read, the same deliberate act hub_confirm already
// requires — this just saves the round trip of calling hub_confirm
// separately before sending. Sends a genuine standalone ack (via
// conn.ConfirmReceived), not a piggybacked one — chat-relay's own note:
// a piggybacked receipt is fire-and-forget by design and never answers
// with a pending count, where a standalone one does.
func (h *Hub) confirmCursor(conn *hubconn.Conn, cursor string) (*int, error) {
	behind, err := conn.ConfirmReceived(cursor)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	h.lastHandedOverCursor = cursor
	// handedOverAhead's entries exist to dedup a walk that hasn't reached
	// them yet — but a confirm jump moves the walk's own starting point
	// straight past all of them without ever waking on any individual
	// one (unlike hub_catch_up's own dedup loop, which deletes each
	// entry as it walks over it — see below). Left alone, every entry
	// recorded before this confirm becomes permanently orphaned: nothing
	// will ever walk back far enough to clean it up, and it just grows
	// the persisted set forever. Clearing it here is safe by the same
	// reasoning as everywhere else in this design — anything it held is
	// now at-or-before the new watermark, so a future walk starting from
	// cursor would never have reached those entries anyway; worst case a
	// genuinely-newer entry gets dropped too, costing one avoidable
	// duplicate later, not a loss.
	h.handedOverAhead = nil
	id := h.catchUpID
	h.mu.Unlock()
	setCatchUpCursor(id, cursor)
	saveHandedOverAhead(id, nil)
	return behind, nil
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
	if !conn.WantsActionAcks() {
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
	attachments, err := readAttachmentParam(req)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	mentions, err := parseMentions(req.GetArguments()["mentions"])
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	behindNote := ""
	if confirmCursor := req.GetString("confirmCursor", ""); confirmCursor != "" {
		behind, err := h.confirmCursor(conn, confirmCursor)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("confirmCursor failed: %v", err)), nil
		}
		behindNote = formatBehindNote(behind)
	}
	ev, ok, err := conn.EditMessageAwaitingAck(externalID, text, attachments, req.GetString("format", ""), req.GetString("replyTo", ""), mentions)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("edit request failed: %v", err)), nil
	}
	if ok {
		return mcp.NewToolResultText(hubconn.FormatEvent(ev) + behindNote), nil
	}
	if !conn.WantsActionAcks() {
		return mcp.NewToolResultText(
			"edit request sent — confirmation (or a refusal) will arrive via wait/hub_receive/" +
				"hub_wait, not from this call" + behindNote,
		), nil
	}
	return mcp.NewToolResultText(fmt.Sprintf(
		"edit request sent — no acknowledgement within %v; check wait/hub_receive/hub_wait for "+
			"the actual outcome rather than assuming this succeeded%s", hubconn.AckWaitTimeout, behindNote,
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
	if !conn.WantsActionAcks() {
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
