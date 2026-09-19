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
	// redialTarget is the connstore.Target the successful connect
	// actually resolved, kept so a reconnect reuses it rather than
	// recomputing one. Recomputing happens off a background context with
	// no MCP session in it, so the roots the first connect asked for are
	// unavailable and the answer falls back to this process's working
	// directory — a DIFFERENT project whenever the two differ, whose
	// identity for the same link is somebody else's.
	redialTarget connstore.Target
	// reconnecting guards against two automatic attempts overlapping.
	reconnecting bool
	// reconnectGen ends the loop currently running. A manual hub_connect
	// bumps it and takes over, which is why the loop compares it rather
	// than only checking whether the link is still set: a caller dialling
	// by hand is not a caller leaving, so the link stays exactly where it
	// is and something else has to say "stop".
	reconnectGen int
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
		// A name held by an automatic reconnect YIELDS to the caller.
		// Every drop is retried now, so refusing here would make the
		// ordinary case — a reader that noticed the drop and dialled —
		// wait out a backoff of up to a minute for an attempt that is
		// doing exactly what it just asked for. The loop is cancelled and
		// this connect proceeds; what the loop was holding (the stored
		// identity, the follower kept across the gap) is held by the
		// session, not by the loop, so nothing is stranded by ending it.
		prev.mu.Lock()
		pending := prev.reconnecting || !prev.reconnectAt.IsZero()
		stillConnected := prev.conn != nil
		if pending && !stillConnected {
			// Ends the loop: it checks this before every attempt, and a
			// caller dialling by hand is not a caller leaving, so the
			// redial link stays exactly where it is.
			prev.reconnectGen++
			prev.reconnecting, prev.reconnectAt = false, time.Time{}
		}
		prev.mu.Unlock()
		if pending && !stillConnected {
			// h.mu is held here, so the map is edited directly rather
			// than through h.close, which takes it.
			delete(h.sessions, name)
		} else {
			return nil, fmt.Errorf("a connection named %q is already open — disconnect it first, "+
				"or pick another name; this is never silently reattached to the existing one", name)
		}
	}
	if len(h.sessions) >= maxSessions {
		return nil, fmt.Errorf("already holding %d connections (%s) — the limit is %d; "+
			"disconnect one first", len(h.sessions), strings.Join(h.names(), ", "), maxSessions)
	}
	if h.sessions == nil {
		h.sessions = map[string]*session{}
	}
	s := &session{name: name, hub: h}
	// Built HERE, before anything can run against this session. Assigning
	// it later raced the connection's own goroutines, which read it to
	// decide whether an arriving event can be delivered.
	//
	// Pointed at whatever thread the last request came from, since a
	// connection opened by that request would otherwise have no target
	// until an unrelated later call arrived. h.mu is already held, so
	// lastMeta is read directly rather than through adoptInto.
	s.pusher = harness.OpenAs(name)
	if len(h.lastMeta) > 0 {
		_ = s.pusher.Adopt(h.lastMeta)
	}
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

// rememberMeta keeps the most recent request's _meta so a connection
// opened later can be pointed at the same harness thread.
//
// IN MEMORY ONLY, and deliberately. It names which harness thread is
// talking to this process right now; it is not an identity, not a
// credential, and a value from a previous run describes a thread that no
// longer exists — persisting it would mean a restarted process
// confidently delivering into somewhere nobody is listening.
func (h *Hub) rememberMeta(meta map[string]any) {
	if len(meta) == 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastMeta = meta
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
// activeConn returns this session's connection and the wait socket
// serving it, or (nil, …) when it is not connected.
//
// The waiter is the PROCESS's, not this session's: a pull harness runs
// one `wait` process and cannot run one per connection, so every
// connection is carried on the one socket and identified by name in what
// it writes. See waiter.attached.
func (s *session) activeConn() (*hubconn.Conn, *waiter.Waiter) {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	return conn, s.hub.currentWaiter()
}

// setActiveConn records a newly established connection. target identifies
// it in connstore for later teardown bookkeeping — the zero Target for a
// connection kind connstore doesn't track at all.
func (s *session) setActiveConn(conn *hubconn.Conn, w *waiter.Waiter, target connstore.Target) {
	s.mu.Lock()
	s.everConnected = true
	s.conn, s.connTarget = conn, target
	s.mu.Unlock()
	// Attached under this connection's name, so every line the reader
	// receives says which conversation it belongs to.
	w.SetSource(s.name, conn)
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
	conn, target := s.conn, s.connTarget
	s.conn, s.connTarget = nil, connstore.Target{}
	s.mu.Unlock()
	s.clearAttachDir()
	return conn, s.hub.currentWaiter(), target
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
	// THIS connection's own delivery settles it. Two servers can issue
	// the same opaque value, and asking only "did anyone else deliver
	// this" refused a confirm of a cursor this very connection had handed
	// over — the reader was told to confirm what it read, did exactly
	// that, and was turned away. A cursor delivered here is ours whoever
	// else happens to use the same string.
	if mine != nil && mine.hasDelivered(cursor) {
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

// currentWaiter is the process's wait socket, or nil where none is bound
// (push mode, or a harness that never needed one).
func (h *Hub) currentWaiter() *waiter.Waiter {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.waiter
}

// ensureWaiter binds the process's one wait socket, on the first
// connection that needs it.
//
// One socket, because a pull harness runs one `wait` process and cannot
// run one per connection: a second socket would be a conversation nothing
// is listening to, which is the silence this whole component exists to
// prevent. Connections are told apart by the label on every line rather
// than by which socket carried it.
func (h *Hub) ensureWaiter() (*waiter.Waiter, error) {
	h.mu.Lock()
	if h.waiter != nil {
		w := h.waiter
		h.mu.Unlock()
		return w, nil
	}
	h.mu.Unlock()
	w, err := waiter.Listen()
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.waiter != nil {
		// Another connect bound one while this was listening. Keep the
		// first: a second socket is one nothing was told to read.
		go w.Close()
		return h.waiter, nil
	}
	h.waiter = w
	return w, nil
}

// owns reports whether name still maps to this exact session. A session
// that was closed (hub_disconnect) keeps working as an object while
// anything holds a pointer to it, so "am I still the one" is a question
// only the hub can answer — see reconnectOnce, which must not install a
// live connection into a session the name no longer reaches.
func (h *Hub) owns(s *session) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sessions[s.name] == s
}
