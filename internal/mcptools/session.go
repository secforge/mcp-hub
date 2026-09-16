package mcptools

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/harness"
	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/waiter"
)

// session is one hub connection and everything that describes only that
// connection: where it is, where its reader has got to, and what it would
// take to bring it back.
//
// One process holds several of these, and the reason they are a struct
// rather than fields on the Hub is not tidiness. A reading position, a
// "nothing precedes this" claim, and a set of cursors witnessed live are
// all statements about ONE message stream. Shared between two connections
// they become statements about neither: a confirmation on one would
// advance a position on the other, and cursors from two servers are
// opaque, so nothing downstream could notice.
//
// name is the address. It is chosen by the caller at connect, never
// derived, and never reused while a connection holds it — see Hub.open.
type session struct {
	name string
	// hub is the process this connection belongs to, for the things that
	// are genuinely process-wide: the queued notes (one reader), the
	// harness push target, the delivery budget.
	hub *Hub

	// mu guards everything below. Needed because, unlike a tool call,
	// conn.OnActivity's disconnect callback runs on the Conn's own read
	// goroutine and can clear these at any moment.
	//
	// Lock order: Hub.mu before session.mu, never the reverse. The Hub
	// lock only ever covers the session map, so nothing needs to take it
	// while holding this one.
	mu sync.Mutex
	// everConnected distinguishes the two ways conn is nil: a session
	// that has not dialled yet, and one whose connection has ended. They
	// look identical and want opposite handling — the first must not have
	// its name taken away by a second connect, the second must release
	// it — so the difference is recorded rather than guessed at.
	everConnected bool
	conn          *hubconn.Conn
	waiter        *waiter.Waiter
	// connTarget is the connstore.Target this connection was reached
	// through, so teardown can mark it disconnected in the store —
	// zero-valued for a connection not tracked there at all
	// (teams_relay_connect; see handleTeamsRelayConnect).
	connTarget connstore.Target
	// catchUpID is this connection's stable identity for
	// lastHandedOverCursor's persistence — set once per successful
	// connect via setCatchUpKey. The zero value (Valid() false) means
	// "nothing to key persistence on".
	catchUpID connstore.Target
	// lastHandedOverCursor is the contiguous high-water mark of message
	// positions actually handed to the model — "the last position before
	// which everything has been handed over", never just "the newest
	// position seen". See handleCatchUp for the full rationale
	// (advance-only-on-hand-over, not on-fetch).
	lastHandedOverCursor string
	// knownContiguous records that this connection has been told, by its
	// server, that nothing precedes live traffic: a catch-up walk that
	// answered "caught up", or a connect reporting nothing behind and no
	// recorded gap.
	//
	// It is what makes auto-confirming a live message safe. A returning
	// tool call IS delivery to the model, so a synchronous hand-over is
	// exactly the confirmation hub_confirm would give. What it cannot do
	// on its own is prove the message FOLLOWS the last confirmed one:
	// cursors are opaque, so a client holding two of them cannot tell
	// whether anything sits between. Advancing anyway would silently skip
	// that middle, which is the one failure this mechanism exists to
	// prevent. Knowing the gap is empty is the missing premise, and only
	// the server can supply it.
	knownContiguous bool
	// seekedSinceConnect records that this connection has already seeked
	// past a large backlog, so it does so at most once. Conn.Behind() is a
	// connect-time snapshot that never moves, so without this every
	// subsequent hub_catch_up would see the same large number, seek again,
	// and overwrite the recorded gap.
	seekedSinceConnect bool
	// handedOverAhead is the set of Msg.Cursor values confirmed handed to
	// the model at a position AHEAD of lastHandedOverCursor's contiguous
	// mark. handleCatchUp checks this before showing a message: one
	// already in this set was already shown live, so catch-up advances
	// past it instead of showing a duplicate. Persisted alongside the
	// cursor, and fully cleared on every hub_confirm, so it stays bounded
	// by traffic since the last confirm rather than by the whole
	// conversation's history.
	handedOverAhead map[string]bool
	// delivered/deliveredOrder remember which cursors THIS connection has
	// handed to the model, most recent last. They exist for one check: a
	// cursor confirmed against the wrong connection would advance a
	// position past messages nobody read, and nothing about the cursor
	// itself could reveal that — they are opaque, and two servers' cursors
	// are not comparable. Who delivered it is knowable, so it is kept.
	delivered      map[string]bool
	deliveredOrder []string

	// redialLink/redialName are what a graceful restart needs in order to
	// come back without the model having to act. Held only after a
	// successful connect, and cleared by an explicit hub_disconnect — a
	// caller that chose to leave must not be dragged back in.
	redialLink string
	redialName string
	// reconnecting guards against two automatic attempts overlapping, and
	// against one racing a hub_connect the model issued itself.
	reconnecting bool
	// reconnectAt is when the pending automatic attempt is due. Without it
	// every tool answers a planned outage with a bare "not connected",
	// which is true and useless: it reads identically to a hub that is
	// simply gone.
	reconnectAt time.Time

	// pusher delivers this connection's events into the harness under this
	// connection's name, and inbox is the address replies to it come back
	// to. One of each per connection: the attribution a reader sees and
	// the address they answer have to describe the same conversation, or
	// answering a message answers a different one.
	//
	// Both are nil where the harness offers no messaging socket, or where
	// binding failed — in which case hub_send remains the way to speak,
	// which is what it was before.
	pusher *harness.Pusher
	inbox  *harness.Inbox

	// attachMu guards attachDir/attachSeq — the local temp directory
	// received attachments are saved into and a counter for unique
	// filenames within it. Separate from mu since the paths that touch it
	// don't otherwise need the connection state.
	attachMu  sync.Mutex
	attachDir string
	attachSeq int
}

// maxSessions caps how many connections one process holds at once.
//
// Not a resource limit — eight sockets and eight websockets are nothing.
// It is an attention limit: every connection delivers into the same
// reader, they share one delivery budget, and a ninth conversation buys
// nothing that the eight are not already competing for. A cap is the
// honest way to say that, rather than letting the budget quietly starve
// whichever connection happens to be slowest.
const maxSessions = 8

// validSessionName accepts the name a connection will be addressed by.
// Deliberately narrow: this string is typed back by a model into every
// later call, so anything that invites a near-miss — whitespace, case
// variation, punctuation that renders differently in two places — is
// refused at the one moment a refusal is cheap.
func validSessionName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("a connection needs a name — it is how every later call refers to it")
	case len(name) > 32:
		return fmt.Errorf("%q is too long for a name (max 32)", name)
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return fmt.Errorf("%q: a name is lowercase letters, digits, - and _ (got %q)", name, r)
		}
	}
	return nil
}

// open registers a new session under name, or refuses.
//
// A name already in use is refused rather than reattached to the existing
// connection. "Connect me as X" when X exists means one of two things —
// reconnect that one, or a second connection whose name collided — and
// they want opposite handling. Guessing wrong reattaches a caller to a
// conversation it did not ask for, or replaces one it was still reading.
func (h *Hub) open(name string) (*session, error) {
	if err := validSessionName(name); err != nil {
		return nil, err
	}
	// A connection that died without a clean hub_disconnect still holds
	// its name until something notices — normally the read goroutine's own
	// teardown, but a drop during connect's setup can leave it sitting
	// there. Reclaim it here rather than making a caller disconnect
	// something that is already gone: "already open" would be true of the
	// map and false of the world.
	if prev, err := h.session(name); err == nil {
		conn, _ := prev.activeConn()
		prev.mu.Lock()
		pending, dialled := prev.reconnecting, prev.everConnected
		prev.mu.Unlock()
		switch {
		case pending:
			// Left alone: the refusal below explains it, and tearing down
			// what a reconnect is holding is exactly what must not happen.
		case conn == nil && !dialled:
			// Mid-dial, not dead. Taking this name would strand the
			// connect that is holding it.
		case conn == nil:
			h.close(name)
		case !conn.Connected():
			prev.teardownIfCurrent(conn)
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if prev, ok := h.sessions[name]; ok {
		// A name held by a connection this client is bringing back by
		// itself is a different refusal, and the caller needs to know
		// which: that attempt already holds the identity, the stored
		// secret and a follower kept open across the gap, so a second
		// dial would either lose the race or win it and strand what the
		// first was holding.
		prev.mu.Lock()
		pending, at := prev.reconnecting, prev.reconnectAt
		prev.mu.Unlock()
		if pending {
			when := "right now"
			if in := time.Until(at).Round(time.Second); in > 0 {
				when = fmt.Sprintf("in about %s", in)
			}
			return nil, fmt.Errorf("reconnection in progress on %q — this client is already coming "+
				"back on its own after the server announced a restart, %s. Do not dial a second "+
				"time; wait for it. To stop waiting instead, call hub_disconnect on that name, "+
				"which gives up and releases the follower being held for it", name, when)
		}
		return nil, fmt.Errorf("a connection named %q is already open — disconnect it first, or "+
			"pick another name; this is never silently reattached to the existing one", name)
	}
	if len(h.sessions) >= maxSessions {
		return nil, fmt.Errorf("already holding %d connections (%s) — the limit is %d; "+
			"disconnect one first", len(h.sessions), strings.Join(h.names(), ", "), maxSessions)
	}
	if h.sessions == nil {
		h.sessions = map[string]*session{}
	}
	s := &session{name: name, hub: h}
	h.sessions[name] = s
	return s, nil
}

// session resolves a name to its connection, or explains what is open.
//
// An unknown name is an error rather than a fallback to "the only one
// open", including when exactly one is. A caller that did not say which
// connection it meant has not been understood, and the one time that is
// cheap to say so is now: answering from the only open connection teaches
// the habit of omitting the name, which fails silently the moment a
// second connection exists.
func (h *Hub) session(name string) (*session, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.sessions[name]; ok {
		return s, nil
	}
	open := h.names()
	if len(open) == 0 {
		return nil, fmt.Errorf("no connection named %q — not connected to anything; hub_connect "+
			"opens a connection and gives it a name", name)
	}
	return nil, fmt.Errorf("no connection named %q — open: %s", name, strings.Join(open, ", "))
}

// close forgets a session entirely, so its name is free again.
func (h *Hub) close(name string) {
	h.mu.Lock()
	delete(h.sessions, name)
	h.mu.Unlock()
}

// names lists the open connections, sorted so two calls read the same.
// Caller holds h.mu.
func (h *Hub) names() []string {
	out := make([]string, 0, len(h.sessions))
	for n := range h.sessions {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// allSessions snapshots every open session, for the paths that genuinely
// address all of them at once — a shutdown, or a note that has to say
// what happened across connections rather than to one.
func (h *Hub) allSessions() []*session {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*session, 0, len(h.sessions))
	for _, n := range h.names() {
		out = append(out, h.sessions[n])
	}
	return out
}

// activeConn returns this session's connection and wait socket, or (nil,
// nil) when it is not connected.
func (s *session) activeConn() (*hubconn.Conn, *waiter.Waiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, s.waiter
}

// setActiveConn records a newly established connection. target identifies
// it in connstore for later teardown bookkeeping — the zero Target for a
// connection kind connstore doesn't track at all.
func (s *session) setActiveConn(conn *hubconn.Conn, w *waiter.Waiter, target connstore.Target) {
	s.mu.Lock()
	s.everConnected = true
	s.conn, s.waiter, s.connTarget = conn, w, target
	s.mu.Unlock()
}

// setCatchUpKey records id as this connection's stable identity for
// hub_catch_up's persisted position, and loads whatever was previously
// stored under it (nothing, for a first-ever connection to this identity)
// — called once per successful connect, after setActiveConn.
//
// Every call resets lastHandedOverCursor to match id, even when id is
// unchanged: reconnecting re-reads the persisted value rather than
// trusting what is already in memory, so another process sharing the same
// identity (or an external edit to the store) cannot silently diverge from
// what is on disk.
func (s *session) setCatchUpKey(id connstore.Target) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.catchUpID = id
	s.lastHandedOverCursor = ""
	s.seekedSinceConnect = false
	// Nothing is known about what precedes live traffic until a server
	// says so. Assuming otherwise on a fresh connection is how a backlog
	// gets skipped.
	s.knownContiguous = false
	// handedOverAhead's entries are cursors witnessed live for THIS
	// connection's message stream — carrying them into a DIFFERENT
	// identity would be a stale-cursor bleed. So start from nil, then load
	// whatever this exact identity persisted: a reconnect to the SAME
	// conversation restores the set, a switch to a different one starts
	// clean.
	s.handedOverAhead = nil
	if id.Link != "" {
		if cs, ok := connstore.GetCatchUp(id); ok {
			s.lastHandedOverCursor = cs.Cursor
		}
		s.handedOverAhead = loadHandedOverAhead(id)
	}
}

// clearActiveConn unconditionally forgets whatever connection this session
// holds and returns it plus the connstore.Target it was reached through,
// for an explicit hub_disconnect — which should tear down what is active
// right now, regardless of which Conn instance a caller happens to hold.
func (s *session) clearActiveConn() (*hubconn.Conn, *waiter.Waiter, connstore.Target) {
	s.mu.Lock()
	conn, w, target := s.conn, s.waiter, s.connTarget
	s.conn, s.waiter, s.connTarget = nil, nil, connstore.Target{}
	s.mu.Unlock()
	s.clearAttachDir()
	return conn, w, target
}

// note queues something for the model that has no tool result to go in,
// naming the connection it is about. The name is not decoration: with
// several connections open, "reconnected automatically" is a different
// fact about each one, and a notice that does not say which is a notice
// the reader cannot act on.
func (s *session) note(text string) {
	s.hub.noteAutoReconnect("on " + s.name + ": " + text)
}

// forRequest resolves the connection a tool call names, and answers for
// it when there is nothing to act on.
//
// The name is required on every call, including when only one connection
// is open. Resolving it from "the only one" would work today and fail
// silently the first time a second connection exists — by which point the
// habit of leaving it out is established, and the call that used to be
// unambiguous now picks a conversation at random.
func (h *Hub) forRequest(req mcp.CallToolRequest) (*session, *hubconn.Conn, *mcp.CallToolResult) {
	name, err := req.RequireString("connection")
	if err != nil {
		return nil, nil, mcp.NewToolResultError(err.Error())
	}
	s, err := h.session(name)
	if err != nil {
		return nil, nil, mcp.NewToolResultError(err.Error())
	}
	conn, _ := s.activeConn()
	if conn == nil {
		return s, nil, s.notConnected()
	}
	return s, conn, nil
}

// connectionParam is the required name argument every connection-bound
// tool carries.
func connectionParam() mcp.ToolOption {
	return mcp.WithString("connection",
		mcp.Required(),
		mcp.Description("REQUIRED: which connection this acts on — the name you gave it at "+
			"hub_connect. hub_list_connections shows what is open. Always name it, even when "+
			"only one connection is open: there is no default and none is inferred, because a "+
			"call that picks a conversation for you picks the wrong one eventually."))
}

// closeReturnPath gives up this connection's inbox address. Called
// wherever the connection itself is given up: an address that outlives
// its connection accepts replies for a conversation this process is no
// longer in, and nothing would report where they went.
func (s *session) closeReturnPath() {
	if s.inbox != nil {
		_ = s.inbox.Close()
		s.inbox = nil
	}
}

// maxRecentCursors bounds what a connection remembers having delivered.
// The set exists to catch a cursor confirmed against the wrong
// connection, which is a mistake made within a turn or two of seeing the
// message — so a long memory buys nothing and an unbounded one grows for
// the life of the process.
const maxRecentCursors = 1000

// noteDelivered records that this connection handed cursor to the model.
func (s *session) noteDelivered(cursor string) {
	if cursor == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.delivered == nil {
		s.delivered = map[string]bool{}
	}
	if s.delivered[cursor] {
		return
	}
	s.delivered[cursor] = true
	s.deliveredOrder = append(s.deliveredOrder, cursor)
	for len(s.deliveredOrder) > maxRecentCursors {
		delete(s.delivered, s.deliveredOrder[0])
		s.deliveredOrder = s.deliveredOrder[1:]
	}
}

// hasDelivered reports whether this connection handed cursor over.
func (s *session) hasDelivered(cursor string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delivered[cursor]
}

// cursorBelongsElsewhere names another open connection that delivered
// this cursor, when this one did not.
//
// Cursors are opaque and not comparable between servers, so a cursor
// confirmed against the wrong connection cannot be detected by looking at
// it — it would simply advance a position past messages nobody read, and
// the next walk would start after them. What CAN be known is who handed
// it over, so that is recorded and asked.
//
// Deliberately silent when no open connection claims it: that includes a
// cursor delivered before a restart, which is a legitimate thing to
// confirm. Only a cursor demonstrably belonging to another live
// connection is refused — a proof, not a suspicion.
func (h *Hub) cursorBelongsElsewhere(mine *session, cursor string) string {
	if cursor == "" {
		return ""
	}
	for _, other := range h.allSessions() {
		if other == mine {
			continue
		}
		if other.hasDelivered(cursor) {
			return other.name
		}
	}
	return ""
}
