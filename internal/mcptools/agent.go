package mcptools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/harness"
	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/hubenc"
	"github.com/secforge/mcp-hub/internal/wire"
)

// agentSession is one agent connection: one socket, and every conversation
// the server has shared with the agent, each open as its own session under
// a name derived from this one. The model addresses conversations by those
// names exactly as it addresses ordinary connections; the agent's own name
// is only for the agent as a whole (listing it, disconnecting it).
//
// The agent decides what happens to its conversations — opening them,
// reopening one whose grant changed, ending one that was revoked, and
// bringing all of them back after the socket drops — so a conversation's
// session never dials or reconnects on its own.
type agentSession struct {
	hub     *Hub
	name    string
	link    string
	display string
	target  connstore.Target
	pusher  *harness.Pusher

	// opMu serializes everything that opens or ends conversations: agent
	// events, a reconnect reopening everything, a disconnect. Two of them
	// interleaved could open a conversation the other just ended.
	opMu sync.Mutex

	mu         sync.Mutex
	agent      *hubconn.Agent
	grants     map[string]wire.Assignment
	children   map[string]*session
	userClosed map[string]bool
	// failed is why a granted conversation could not be opened, kept so a
	// listing says so instead of the conversation simply being absent.
	failed map[string]string
	// pendingNew holds conversations opened at runtime whose notice has
	// not gone out yet: shared conversations are announced together, in
	// one notice, newConvWindow after the first. A conversation's own
	// content never goes out before its notice — see announcedBefore.
	pendingNew []newConv
	newTimer   *time.Timer
	// newWindow and remindEvery are newConvWindow and
	// agentReminderInterval as they were at connect.
	newWindow    time.Duration
	remindEvery  time.Duration
	leaving      bool
	reconnecting bool
	reconnectAt  time.Time
	gen          int
}

// rosterQuiet is how long a conversation of an agent connection goes
// without a message before the next one is pushed with its member list in
// front of it again.
const rosterQuiet = 30 * time.Minute

// newConvWindow is how long newly shared conversations are collected into
// one notice.
var newConvWindow = 5 * time.Second

// agentReminderInterval is how often one notice reminds about every
// conversation of the agent with unconfirmed deliveries.
var agentReminderInterval = 5 * time.Minute

type newConv struct {
	s     *session
	title string
	note  string
}

// maxAgentNameLen bounds an agent connection's name, because every
// conversation's name starts with it and a name is at most 32 characters:
// what is left has to say which conversation it is.
const maxAgentNameLen = 16

// openWorkers is how many conversations are opened at once. Each open is
// a round trip, and a thousand of them one after another is a connect
// that takes minutes on a remote server.
const openWorkers = 16

// agentConnect is what hub_connect resolved before it learned the link was
// an agent's.
type agentConnect struct {
	link    string
	display string
	target  connstore.Target
	secret  string
}

// connectAgent finishes hub_connect for a link that turned out to be an
// agent connection. placeholder is the session hub_connect reserved the
// name with; the agent takes the name over from it.
func (h *Hub) connectAgent(placeholder *session, a *hubconn.Agent, c agentConnect, keyFile string) (*mcp.CallToolResult, error) {
	as := placeholder.name
	if keyFile != "" {
		a.Close()
		return mcp.NewToolResultError("this link is an AGENT connection, which carries many " +
			"conversations: a key file belongs to one of them, not to the agent. Nothing was " +
			"connected. Connect without keyFile, then give the encrypted conversation its key " +
			"with hub_set_key."), nil
	}
	if len(as) > maxAgentNameLen {
		a.Close()
		return mcp.NewToolResultError(fmt.Sprintf("this link is an AGENT connection, and every "+
			"conversation it carries is named after it, so its name may be at most %d characters "+
			"(%q has %d). Nothing was connected; connect again with a shorter `as`.",
			maxAgentNameLen, as, len(as))), nil
	}
	ag := &agentSession{
		hub: h, name: as, link: c.link, display: c.display, target: c.target,
		pusher:      harness.OpenAs(as),
		agent:       a,
		grants:      map[string]wire.Assignment{},
		children:    map[string]*session{},
		userClosed:  map[string]bool{},
		failed:      map[string]string{},
		newWindow:   newConvWindow,
		remindEvery: agentReminderInterval,
	}
	// The name moves from the placeholder to the agent in one step, and
	// only if the placeholder still holds it: a hub_disconnect during the
	// handshake released it, and the caller has left.
	h.mu.Lock()
	if h.sessions[as] != placeholder {
		h.mu.Unlock()
		a.Close()
		return mcp.NewToolResultError(fmt.Sprintf("connected, but %q was disconnected while the "+
			"handshake was completing — the agent connection has been closed rather than left "+
			"running unreachable. Call hub_connect again if that was not what you intended.", as)), nil
	}
	delete(h.sessions, as)
	if h.agents == nil {
		h.agents = map[string]*agentSession{}
	}
	h.agents[as] = ag
	h.restoredLocked(as)
	if len(h.lastMeta) > 0 {
		_ = ag.pusher.Adopt(h.lastMeta)
	}
	h.mu.Unlock()

	_ = connstore.Upsert(c.target, connstore.Entry{
		PeerID: a.AgentID(), Name: a.Name(), LocalName: as, ReconnectSecret: c.secret,
		LastConnectedAt: time.Now().UTC(), Connected: true, Holder: selfHolder(), Agent: true,
	})

	ag.opMu.Lock()
	opened, failed := ag.install(a)
	ag.opMu.Unlock()
	a.OnEvent(func(ev hubconn.AgentEvent) { ag.handleEvent(a, ev) })
	go ag.remindLoop()
	return mcp.NewToolResultText(ag.connectSummary(opened, failed)), nil
}

// install makes a the agent's socket and opens every conversation it
// carries, except those the caller closed. Conversations the server no
// longer lists are ended. Caller holds opMu.
func (ag *agentSession) install(a *hubconn.Agent) (opened []*session, failed map[string]string) {
	items := a.Assignments()
	ag.mu.Lock()
	ag.agent = a
	ag.reconnecting, ag.reconnectAt = false, time.Time{}
	listed := map[string]bool{}
	for _, it := range items {
		listed[it.Conv] = true
		ag.grants[it.Conv] = it
	}
	var gone []*session
	for conv, s := range ag.children {
		if !listed[conv] {
			gone = append(gone, s)
			delete(ag.children, conv)
			delete(ag.grants, conv)
		}
	}
	ag.failed = map[string]string{}
	ag.mu.Unlock()
	for _, s := range gone {
		ag.release(s, "it is no longer shared with this agent (it was withdrawn while the agent "+
			"connection was down)", true)
	}

	var todo []wire.Assignment
	ag.mu.Lock()
	for _, it := range items {
		if !ag.userClosed[it.Conv] {
			todo = append(todo, it)
		}
	}
	ag.mu.Unlock()
	opened, failed = ag.openAll(a, todo, false)
	ag.retryFailed(a, failed)
	return opened, failed
}

// retryFailed keeps trying the conversations that could not be opened.
func (ag *agentSession) retryFailed(a *hubconn.Agent, failed map[string]string) {
	for conv := range failed {
		go ag.retryOpen(a, conv, 1)
	}
}

// retryOpen opens a granted conversation that failed to open, backing off
// between attempts, until it is open, its grant ends, the agent socket is
// replaced, or the attempts run out — then it says so once.
func (ag *agentSession) retryOpen(a *hubconn.Agent, conv string, attempt int) {
	time.Sleep(reconnectBackoff(attempt))
	ag.opMu.Lock()
	defer ag.opMu.Unlock()
	ag.mu.Lock()
	it, granted := ag.grants[conv]
	s := ag.children[conv]
	current := ag.agent == a && !ag.leaving && !ag.userClosed[conv]
	ag.mu.Unlock()
	if !current || !granted {
		return
	}
	if s != nil {
		if c, _ := s.activeConn(); c != nil && c.Connected() {
			return
		}
	}
	firstFailed := "could not be opened at first"
	_, err := ag.openChild(a, it, nil, nil, &firstFailed)
	switch {
	case err == nil:
		ag.mu.Lock()
		delete(ag.failed, conv)
		ag.mu.Unlock()
	case attempt >= maxReopenAttempts:
		ag.mu.Lock()
		ag.failed[conv] = err.Error()
		ag.mu.Unlock()
		ag.alert(fmt.Sprintf("Conversation %q could not be opened after %d attempts (last: %v). It "+
			"stays shared and is tried again when the agent connection next reconnects.",
			it.Title, attempt, err))
	default:
		ag.mu.Lock()
		ag.failed[conv] = err.Error()
		ag.mu.Unlock()
		go ag.retryOpen(a, conv, attempt+1)
	}
}

// openAll opens items concurrently, a bounded number at a time, announcing
// each as new when announce is set.
func (ag *agentSession) openAll(a *hubconn.Agent, items []wire.Assignment, announce bool) ([]*session, map[string]string) {
	var note *string
	if announce {
		note = new(string)
	}
	var mu sync.Mutex
	var opened []*session
	failed := map[string]string{}
	// Every stored entry read once, rather than the whole file once per
	// conversation: with a thousand conversations the file holds a
	// thousand entries, and reading it a thousand times is the slow part.
	var stored map[string]connstore.Entry
	if entries, err := connstore.ListForProject(ag.target.Project); err == nil {
		stored = make(map[string]connstore.Entry, len(entries))
		for _, le := range entries {
			stored[le.Target.Link] = le.Entry
		}
	}
	writes := map[connstore.Target]connstore.Entry{}
	record := func(t connstore.Target, e connstore.Entry) {
		mu.Lock()
		writes[t] = e
		mu.Unlock()
	}
	jobs := make(chan wire.Assignment)
	var wg sync.WaitGroup
	for i := 0; i < openWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range jobs {
				s, err := ag.openChild(a, it, stored, record, note)
				mu.Lock()
				if err != nil {
					failed[it.Conv] = err.Error()
				} else {
					opened = append(opened, s)
				}
				mu.Unlock()
			}
		}()
	}
	for _, it := range items {
		jobs <- it
	}
	close(jobs)
	wg.Wait()
	_ = connstore.UpsertMany(writes)
	ag.mu.Lock()
	for conv, why := range failed {
		ag.failed[conv] = why
	}
	ag.mu.Unlock()
	sort.Slice(opened, func(i, j int) bool { return opened[i].name < opened[j].name })
	return opened, failed
}

// childTarget is where one conversation's identity and read position are
// stored.
func (ag *agentSession) childTarget(conv string) connstore.Target {
	return connstore.AgentConvTarget(ag.target, conv)
}

// openChild opens one conversation and installs it as a session, reusing
// the session already holding it (a reopen) or creating one under the
// conversation's stored or newly derived name. preloaded, when not nil,
// is every stored entry of this project by link, read once by the caller,
// and record takes the entry to write, for the caller to write with all
// the others at once.
//
// A non-nil announce queues the notice that the conversation is new, with
// that note, before anything from it can be delivered (see announceNew).
func (ag *agentSession) openChild(a *hubconn.Agent, it wire.Assignment, preloaded map[string]connstore.Entry,
	record func(connstore.Target, connstore.Entry), announce *string) (*session, error) {
	if record == nil {
		record = func(t connstore.Target, e connstore.Entry) { _ = connstore.Upsert(t, e) }
	}
	target := ag.childTarget(it.Conv)
	stored, found := preloaded[target.Link]
	if preloaded == nil {
		var err error
		stored, found, err = connstore.Get(target)
		if err != nil {
			// Opening without the stored read position would start it from
			// nothing — the same reason hub_connect refuses an unreadable
			// store.
			return nil, fmt.Errorf("its stored read position could not be read: %w", err)
		}
	}
	var err error
	var key []byte
	if stored.KeyFile != "" {
		if key, err = hubenc.ReadKeyFile(stored.KeyFile); err != nil {
			return nil, fmt.Errorf("its key file could not be read: %w", err)
		}
	}
	s, err := ag.childSession(it, stored.LocalName)
	if err != nil {
		return nil, err
	}
	conn, err := a.Open(it.Conv, key)
	if err != nil {
		s.mu.Lock()
		fresh := s.conn == nil && !s.everConnected
		s.mu.Unlock()
		if fresh {
			ag.mu.Lock()
			if ag.children[it.Conv] == s {
				delete(ag.children, it.Conv)
			}
			ag.mu.Unlock()
			ag.hub.close(s.name)
		}
		return nil, err
	}
	var w = ag.hub.currentWaiter()
	if w == nil && !pushOnly() {
		if w, err = ag.hub.ensureWaiter(); err != nil {
			conn.Close()
			return nil, fmt.Errorf("could not start the wait socket: %w", err)
		}
	}
	conn.ShareDeliveryBudget(ag.hub.budget, s.name)
	conn.SetDeliveryBudget(ag.hub.spillDir, 0, 0, 0)
	conn.PushRosterBeforeMessages(rosterQuiet)
	conn.RemindElsewhere()
	s.mu.Lock()
	// The redial fields say "this session wants a connection", which is
	// what the install guard and a disconnect both read. A conversation is
	// never dialled through them; its agent reopens it.
	s.redialLink, s.redialName, s.redialTarget = target.Link, "", target
	s.reconnecting, s.reconnectAt = false, time.Time{}
	gen := s.reconnectGen
	s.mu.Unlock()
	if announce != nil {
		ag.announceNew(s, it.Title, *announce)
	}
	conn.OnActivity(func() {
		w.Poke()
		s.pushToHarness(conn, w)
		if !conn.Connected() {
			ag.childEnded(s, conn)
		}
	})
	if !s.installIfStillWanted(conn, w, target, gen) {
		ag.withdrawNew(s)
		conn.Close()
		return nil, fmt.Errorf("it was disconnected while opening")
	}
	s.adoptCatchUp(target, stored.CatchUp, found, nil)
	if s.inbox == nil {
		s.openReturnPath()
	}
	topic := it.Title
	if t := conn.Topic(); t != nil && *t != "" {
		topic = *t
	}
	if !found || stored.PeerID != conn.PeerID() || stored.LocalName != s.name || stored.Topic != topic {
		record(target, connstore.Entry{
			PeerID: conn.PeerID(), Topic: topic, LocalName: s.name, KeyFile: stored.KeyFile,
			LastConnectedAt: time.Now().UTC(),
		})
	}
	if !conn.Connected() {
		ag.childEnded(s, conn)
	}
	return s, nil
}

// childSession returns the session for a conversation, creating it under
// a name that is unique among everything open: the stored one when it is
// still free, otherwise one derived from the agent's name, the title and
// the handle.
func (ag *agentSession) childSession(it wire.Assignment, storedName string) (*session, error) {
	h := ag.hub
	ag.mu.Lock()
	if s := ag.children[it.Conv]; s != nil {
		ag.mu.Unlock()
		return s, nil
	}
	ag.mu.Unlock()
	h.mu.Lock()
	taken := func(n string) bool {
		_, s := h.sessions[n]
		_, a := h.agents[n]
		return s || a
	}
	name := storedName
	if name == "" || !strings.HasPrefix(name, ag.name+"-") || validSessionName(name) != nil || taken(name) {
		name = childName(ag.name, it.Title, it.Conv, taken)
	}
	if name == "" {
		h.mu.Unlock()
		return nil, fmt.Errorf("no free name could be derived for it")
	}
	s := h.registerLocked(name, ag, it.Conv)
	h.mu.Unlock()
	ag.mu.Lock()
	ag.children[it.Conv] = s
	ag.mu.Unlock()
	return s, nil
}

// childName derives a conversation's name: the agent's name, a slug of the
// title, and enough of a hash of the server's handle to be unique. The
// handle is what makes it stable — titles change and repeat, handles do
// not — and the title is what makes it readable.
func childName(agent, title, conv string, taken func(string) bool) string {
	sum := sha256.Sum256([]byte(conv))
	hash := hex.EncodeToString(sum[:])
	slug := titleSlug(title)
	for n := 6; n <= 16; n += 2 {
		room := 32 - len(agent) - 2 - n
		s := slug
		if room < len(s) {
			s = strings.TrimRight(s[:max(room, 0)], "-")
		}
		name := agent + "-" + hash[:n]
		if s != "" {
			name = agent + "-" + s + "-" + hash[:n]
		}
		if len(name) <= 32 && !taken(name) {
			return name
		}
	}
	return ""
}

// titleSlug turns a title into lowercase letters, digits and single dashes.
func titleSlug(title string) string {
	replace := map[rune]string{'ä': "ae", 'ö': "oe", 'ü': "ue", 'ß': "ss", 'Ä': "ae", 'Ö': "oe", 'Ü': "ue"}
	var b strings.Builder
	dash := false
	for _, r := range title {
		switch {
		case replace[r] != "":
			b.WriteString(replace[r])
			dash = false
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			dash = false
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r - 'A' + 'a')
			dash = false
		default:
			if !dash && b.Len() > 0 {
				b.WriteByte('-')
				dash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

// childEnded decides what a conversation's ended connection means. It runs
// on that connection's own goroutine, possibly before or after the agent
// event that caused it.
func (ag *agentSession) childEnded(s *session, conn *hubconn.Conn) {
	if cur, _ := s.activeConn(); cur != conn {
		// Already replaced (reopened) or torn down: nothing of this
		// connection is left to decide about.
		return
	}
	reason, permanent := conn.PermanentFailure()
	ag.mu.Lock()
	current := ag.children[s.conv] == s
	leaving, closedByUser := ag.leaving, ag.userClosed[s.conv]
	ag.mu.Unlock()
	switch {
	case !current || leaving || closedByUser:
		s.teardown(conn, false)
	case permanent:
		ag.mu.Lock()
		delete(ag.children, s.conv)
		delete(ag.grants, s.conv)
		ag.mu.Unlock()
		ag.release(s, reason, true)
	case conn.AbandonedForBacklog():
		// Nothing reopens it: the agent is up, and reopening would only
		// fill the same unread buffer again.
		ag.mu.Lock()
		delete(ag.children, s.conv)
		ag.mu.Unlock()
		ag.release(s, "far more arrived there than was being read, so it was closed; it is "+
			"still shared and is opened again when the agent connection next reconnects", false)
	default:
		// Coming back. While the agent socket is down, its reconnect
		// reopens every conversation; while it is up, nothing else would,
		// so it is reopened here.
		s.teardown(conn, true)
		ag.mu.Lock()
		a := ag.agent
		ag.mu.Unlock()
		if a != nil && a.Connected() {
			go ag.retryReopen(a, s, 1)
		}
	}
}

// release ends a conversation's session for good and says so. forget also
// deletes its stored entry: a handle is never reused, so nothing could
// ever read it again.
func (ag *agentSession) release(s *session, why string, forget bool) {
	s.mu.Lock()
	s.redialLink, s.redialName, s.redialTarget = "", "", connstore.Target{}
	s.reconnecting, s.reconnectAt = false, time.Time{}
	s.mu.Unlock()
	if conn, w, _ := s.clearActiveConn(); conn != nil {
		conn.Close()
		if w != nil {
			w.Detach(s.name, "it is no longer shared with this agent")
		}
	} else if w := ag.hub.currentWaiter(); w != nil {
		w.Detach(s.name, "it is no longer shared with this agent")
	}
	s.closeReturnPath()
	ag.hub.close(s.name)
	if forget {
		_, _ = connstore.Delete(ag.childTarget(s.conv))
	}
	ag.alert(fmt.Sprintf("Conversation %q ENDED and is gone from this agent: %s. Nothing can be "+
		"sent there any more; do not address %q again.", s.name, why, s.name))
}

// handleEvent applies one agent event. Events of an agent socket that has
// since been replaced are ignored: they describe a connection that no
// longer exists.
func (ag *agentSession) handleEvent(a *hubconn.Agent, ev hubconn.AgentEvent) {
	if ev.Kind == "down" {
		// Outside opMu: giving up takes it, and a reconnect is started
		// rather than run here.
		ag.mu.Lock()
		stale := ag.agent != a
		ag.mu.Unlock()
		if !stale {
			ag.scheduleReconnect(a)
		}
		return
	}
	ag.opMu.Lock()
	defer ag.opMu.Unlock()
	ag.mu.Lock()
	stale := ag.agent != a
	leaving := ag.leaving
	ag.mu.Unlock()
	if stale || leaving {
		return
	}
	switch ev.Kind {
	case "assigned":
		ag.mu.Lock()
		ag.grants[ev.Conv] = ev.Item
		_, open := ag.children[ev.Conv]
		ag.mu.Unlock()
		if open {
			return
		}
		_, err := ag.openChild(a, ev.Item, nil, nil, new(string))
		if err != nil {
			ag.mu.Lock()
			ag.failed[ev.Conv] = err.Error()
			ag.mu.Unlock()
			ag.alert(fmt.Sprintf("A new conversation %q was shared with this agent, but opening it "+
				"failed: %v. It is tried again by itself.", ev.Item.Title, err))
			go ag.retryOpen(a, ev.Conv, 1)
		}
	case "changed":
		ag.mu.Lock()
		ag.grants[ev.Conv] = ev.Item
		ag.mu.Unlock()
	case "closed":
		ag.mu.Lock()
		s := ag.children[ev.Conv]
		it, granted := ag.grants[ev.Conv]
		ag.mu.Unlock()
		if s == nil || !granted {
			return
		}
		if ev.Reopen {
			if _, err := ag.openChild(a, it, nil, nil, nil); err != nil {
				ag.mu.Lock()
				delete(ag.children, ev.Conv)
				ag.mu.Unlock()
				ag.release(s, fmt.Sprintf("its sharing changed and reopening it failed (%v)", err), false)
				return
			}
			ag.alert(fmt.Sprintf("Conversation %q was REOPENED because its sharing changed: it is "+
				"now %s. Anything said while it was being reopened is caught up by "+
				"hub_catch_up(connection: %q).", s.name, accessText(s), s.name))
			return
		}
		if permanentCloseCode(ev.Code) {
			// childEnded has released it, with the reason.
			return
		}
		// Still shared, so it is reopened shortly: the server ends a
		// session this way while something about the conversation is in
		// flux (a hub session being deleted closes with 1001 until its
		// revoked arrives), and a "revoked" in the meantime ends the
		// retrying.
		go ag.retryReopen(a, s, 1)
	case "revoked":
		ag.mu.Lock()
		s := ag.children[ev.Conv]
		delete(ag.children, ev.Conv)
		delete(ag.grants, ev.Conv)
		ag.mu.Unlock()
		if s != nil {
			why := "it was removed from this agent"
			if ev.Reason == "gone" {
				why = "the conversation no longer exists"
			} else if ev.Reason != "" {
				why += " (" + ev.Reason + ")"
			}
			ag.release(s, why, true)
		}
	case "activity":
		ag.mu.Lock()
		it := ag.grants[ev.Conv]
		closed := ag.userClosed[ev.Conv]
		ag.mu.Unlock()
		if closed {
			ag.alert(fmt.Sprintf("Conversation %q, which you closed, has %d unread message(s). It "+
				"stays closed until the agent connection reconnects.", it.Title, ev.Behind))
		}
	case "assignments":
		listed := map[string]wire.Assignment{}
		for _, it := range ev.Items {
			listed[it.Conv] = it
		}
		ag.mu.Lock()
		var gone []*session
		for conv, s := range ag.children {
			if _, ok := listed[conv]; !ok {
				gone = append(gone, s)
				delete(ag.children, conv)
			}
		}
		var fresh []wire.Assignment
		for conv, it := range listed {
			ag.grants[conv] = it
			if _, open := ag.children[conv]; !open && !ag.userClosed[conv] {
				fresh = append(fresh, it)
			}
		}
		ag.mu.Unlock()
		for _, s := range gone {
			ag.release(s, "it is no longer shared with this agent", true)
		}
		_, failedNow := ag.openAll(a, fresh, true)
		ag.retryFailed(a, failedNow)
	case "error":
		ag.alert(fmt.Sprintf("The server reported on the agent connection: %s (%s). Nothing was "+
			"delivered to any conversation by whatever caused it.", ev.Reason, ev.ErrCode))
	case "startAccounts":
		if line := startAccountsLine(a.StartAccounts()); line != "" {
			ag.alert("This agent now " + line + ".")
		} else {
			ag.alert("This agent may no longer start conversations in any account.")
		}
	}
}

// permanentCloseCode reports whether a conversation's close code means it
// cannot come back.
func permanentCloseCode(code int) bool {
	switch code {
	case 4001, 4002, 4003, 4004:
		return true
	}
	return false
}

func closeText(code int, reason string) string {
	if reason == "" {
		return fmt.Sprintf("code %d", code)
	}
	return fmt.Sprintf("code %d: %s", code, reason)
}

// accessText says what a conversation's grant allows, for the reader.
func accessText(s *session) string {
	conn, _ := s.activeConn()
	if conn != nil && conn.ReadOnly() {
		return "READ-ONLY — you can read and catch up, but nothing you send, edit, react, delete " +
			"or pin reaches it"
	}
	return "read-write"
}

func sessionNames(ss []*session) string {
	names := make([]string, len(ss))
	for i, s := range ss {
		names[i] = s.name
	}
	return strings.Join(names, ", ")
}

// alert tells the reader something about the agent connection as a whole.
func (ag *agentSession) alert(text string) {
	text = noticeText(text)
	if ag.pusher != nil {
		if ok, _ := ag.pusher.Available(); ok {
			if _, err := ag.hub.gate().push(ag.pusher, "[hub: on agent "+ag.name+": "+text+"]"); err == nil {
				return
			}
		}
	}
	ag.hub.noteAutoReconnect("on agent " + ag.name + ": " + text)
}

// notAConversation is the answer to a tool call that names the agent
// itself where a conversation is needed.
func (ag *agentSession) notAConversation() error {
	ag.mu.Lock()
	n := len(ag.children)
	ag.mu.Unlock()
	return fmt.Errorf("%q is an AGENT connection carrying %d conversation(s), not a conversation: "+
		"name the conversation you mean. hub_list_connections lists them by name, with their titles", ag.name, n)
}

// scheduleReconnect brings a dropped agent connection back, with every
// conversation on it.
func (ag *agentSession) scheduleReconnect(prev *hubconn.Agent) {
	if ag.hub.shuttingDown.Load() {
		return
	}
	if reason, permanent := prev.PermanentFailure(); permanent {
		ag.end(fmt.Sprintf("Agent connection %q ENDED and will NOT be reconnected: %s. Retrying "+
			"cannot fix that.", ag.name, reason))
		return
	}
	ag.mu.Lock()
	if ag.leaving || ag.reconnecting {
		ag.mu.Unlock()
		return
	}
	delay := prev.SuggestedReconnectDelay()
	scheduled := delay > 0
	if !scheduled {
		delay = reconnectBackoff(1)
	}
	ag.reconnecting = true
	ag.reconnectAt = time.Now().Add(delay)
	gen := ag.gen
	n := len(ag.children)
	ag.mu.Unlock()
	ag.markChildrenReconnecting(time.Now().Add(delay))
	why := "The connection dropped without warning" + prev.DisconnectNote()
	if scheduled {
		why = "The server announced a restart"
	}
	ag.alert(fmt.Sprintf("Agent connection %q is DOWN, and with it all %d of its conversations. %s. "+
		"Reconnecting automatically, first attempt in about %s, and retrying until it succeeds — you "+
		"will be told when it does. Nothing is lost: each conversation's reading position only moves "+
		"on a confirm. hub_disconnect(connection: %q) stops the retrying.",
		ag.name, n, why, delay.Round(time.Second), ag.name))
	go ag.reconnectLoop(delay, scheduled, gen)
}

// markChildrenReconnecting makes each conversation answer "reconnecting,
// in about …" rather than "not connected" while the agent is down.
func (ag *agentSession) markChildrenReconnecting(at time.Time) {
	ag.mu.Lock()
	children := make([]*session, 0, len(ag.children))
	for _, s := range ag.children {
		children = append(children, s)
	}
	ag.mu.Unlock()
	for _, s := range children {
		s.mu.Lock()
		if s.conn == nil {
			s.reconnectAt = at
		}
		s.mu.Unlock()
	}
}

func (ag *agentSession) reconnectLoop(interval time.Duration, scheduled bool, gen int) {
	for attempt := 1; ; attempt++ {
		time.Sleep(interval)
		ag.mu.Lock()
		cancelled := ag.leaving || ag.gen != gen
		ag.mu.Unlock()
		if cancelled {
			return
		}
		done, err := ag.reconnectOnce(gen, interval)
		if done {
			return
		}
		if !scheduled {
			interval = reconnectBackoff(attempt + 1)
		}
		ag.mu.Lock()
		ag.reconnectAt = time.Now().Add(interval)
		ag.mu.Unlock()
		ag.markChildrenReconnecting(time.Now().Add(interval))
		ag.alert(fmt.Sprintf("Automatic reconnect attempt %d of agent connection %q FAILED: %v. "+
			"Another attempt follows in about %s. Either WAIT — you will be told when it succeeds — "+
			"or call hub_disconnect(connection: %q) to stop trying.",
			attempt, ag.name, err, interval.Round(time.Second), ag.name))
	}
}

// reconnectOnce dials the agent again and reopens its conversations. done
// is true when nothing further should be tried: it worked, or it cannot.
func (ag *agentSession) reconnectOnce(gen int, waited time.Duration) (done bool, err error) {
	stored, _, err := connstore.Get(ag.target)
	if err != nil {
		ag.end(fmt.Sprintf("Automatic reconnect of agent connection %q STOPPED: the connection "+
			"store could not be read (%v), so its identity is unknown. Fix the file, then call "+
			"hub_connect.", ag.name, err))
		return true, err
	}
	if stored.ReconnectSecret == "" {
		ag.end(fmt.Sprintf("Automatic reconnect of agent connection %q STOPPED: its stored "+
			"identity is gone. Call hub_connect when ready.", ag.name))
		return true, errors.New("no stored identity")
	}
	conn, a, err := hubconn.DialAny(ag.link, hubconn.DialOptions{
		ReconnectSecret: stored.ReconnectSecret, AgentID: stored.PeerID, Name: ag.display,
	})
	if err != nil {
		var refused *hubconn.HandshakeRefusedError
		if errors.As(err, &refused) && refused.Deliberate() {
			ag.end(fmt.Sprintf("Automatic reconnect of agent connection %q STOPPED: the server "+
				"refused it outright (%v). That does not change on a retry.", ag.name, err))
			return true, err
		}
		return false, err
	}
	if a == nil {
		conn.Close()
		ag.end(fmt.Sprintf("Automatic reconnect of agent connection %q STOPPED: its link now "+
			"answers as a single conversation, not as an agent.", ag.name))
		return true, errors.New("not an agent link any more")
	}
	ag.opMu.Lock()
	ag.mu.Lock()
	cancelled := ag.leaving || ag.gen != gen
	ag.mu.Unlock()
	if cancelled {
		ag.opMu.Unlock()
		a.Close()
		return true, nil
	}
	opened, failed := ag.install(a)
	ag.opMu.Unlock()
	a.OnEvent(func(ev hubconn.AgentEvent) { ag.handleEvent(a, ev) })
	_ = connstore.Upsert(ag.target, connstore.Entry{
		PeerID: a.AgentID(), Name: a.Name(), LocalName: ag.name, ReconnectSecret: stored.ReconnectSecret,
		LastConnectedAt: time.Now().UTC(), Connected: true, Holder: selfHolder(), Agent: true,
	})
	text := fmt.Sprintf("Agent connection %q RECONNECTED AUTOMATICALLY after %s; %d conversation(s) "+
		"are open again. Messages may have arrived meanwhile — call hub_catch_up for each "+
		"conversation you are working in.", ag.name, waited.Round(time.Second), len(opened))
	if unread := unreadNames(opened, 20); unread != "" {
		text += " With unread messages: " + unread + "."
	}
	if len(failed) > 0 {
		text += fmt.Sprintf(" %d could not be opened; hub_list_connections says why.", len(failed))
	}
	ag.alert(text)
	return true, nil
}

// end gives the agent connection up: every conversation is released, the
// stored mark says it is no longer held, and the reader is told why.
func (ag *agentSession) end(why string) {
	ag.opMu.Lock()
	defer ag.opMu.Unlock()
	ag.mu.Lock()
	ag.leaving = true
	ag.gen++
	if ag.newTimer != nil {
		ag.newTimer.Stop()
		ag.newTimer = nil
	}
	ag.pendingNew = nil
	ag.reconnecting, ag.reconnectAt = false, time.Time{}
	children := ag.children
	ag.children = map[string]*session{}
	a := ag.agent
	ag.mu.Unlock()
	if a != nil && a.Connected() {
		a.Close()
	}
	for _, s := range children {
		s.mu.Lock()
		s.redialLink, s.redialName, s.redialTarget = "", "", connstore.Target{}
		s.reconnecting, s.reconnectAt = false, time.Time{}
		s.mu.Unlock()
		if conn, w, _ := s.clearActiveConn(); conn != nil {
			conn.Close()
			if w != nil {
				w.Detach(s.name, "its agent connection ended")
			}
		} else if w := ag.hub.currentWaiter(); w != nil {
			w.Detach(s.name, "its agent connection ended")
		}
		s.closeReturnPath()
		ag.hub.close(s.name)
	}
	ag.hub.mu.Lock()
	if ag.hub.agents[ag.name] == ag {
		delete(ag.hub.agents, ag.name)
	}
	ag.hub.mu.Unlock()
	_ = connstore.MarkDisconnected(ag.target)
	if why != "" {
		ag.alert(why)
	}
}

// unreadNames lists up to limit of the sessions with unread messages at
// connect, with their counts.
func unreadNames(ss []*session, limit int) string {
	var parts []string
	more := 0
	for _, s := range ss {
		conn, _ := s.activeConn()
		if conn == nil || conn.Behind() <= 0 {
			continue
		}
		if len(parts) == limit {
			more++
			continue
		}
		parts = append(parts, fmt.Sprintf("%s (%d)", s.name, conn.Behind()))
	}
	if more > 0 {
		parts = append(parts, fmt.Sprintf("and %d more", more))
	}
	return strings.Join(parts, ", ")
}

// connectSummary is hub_connect's result for an agent connection: what
// was opened, in aggregate, and what to do first. It does not list a
// thousand conversations; hub_list_connections does.
func (ag *agentSession) connectSummary(opened []*session, failed map[string]string) string {
	read := 0
	for _, s := range opened {
		if conn, _ := s.activeConn(); conn != nil && conn.ReadOnly() {
			read++
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Connected an AGENT connection as %q (agent %s). It carries every conversation the "+
		"server has shared with this agent, and %d of them are open now: %d read-write, %d READ-ONLY.\n",
		ag.name, ag.agentID(), len(opened), len(opened)-read, read)
	b.WriteString("Each conversation is its OWN connection, named after this one — every tool call " +
		"names the conversation it acts on, exactly as with ordinary connections, and there is no " +
		"default. A reply goes to the connection the message came from; never answer one " +
		"conversation's message in another. hub_list_connections lists every conversation with its " +
		"title, access and unread count. Calling a tool with the agent's own name is refused.\n")
	b.WriteString("READ-ONLY conversations refuse every write (send, edit, react, delete, pin) " +
		"before anything is sent. Conversations are shared, changed and withdrawn at runtime from " +
		"the server's UI; you are told each time, and a withdrawn one's name stops working.\n")
	if line := startAccountsLine(ag.startAccounts()); line != "" {
		b.WriteString("This agent " + line + ". Only when the user or an operator instruction asks " +
			"you to contact a person.\n")
	}
	if len(opened) > 0 && len(opened) <= 20 {
		b.WriteString("Open conversations:\n")
		for _, s := range opened {
			b.WriteString("  " + ag.childLine(s) + "\n")
		}
	}
	if unread := unreadNames(opened, 20); unread != "" {
		b.WriteString("REQUIRED: call hub_catch_up(connection: <name>) for each conversation with " +
			"unread messages before acting in it: " + unread + ".\n")
	} else if len(opened) > 0 {
		b.WriteString("No conversation reports unread messages. Call hub_catch_up for a " +
			"conversation before acting in it all the same — it resumes from where this identity " +
			"left off.\n")
	}
	if len(failed) > 0 {
		convs := make([]string, 0, len(failed))
		for c := range failed {
			convs = append(convs, c)
		}
		sort.Strings(convs)
		b.WriteString(fmt.Sprintf("%d shared conversation(s) could NOT be opened yet; each is tried "+
			"again by itself, and you are told when it opens:\n", len(failed)))
		for i, c := range convs {
			if i == 10 {
				b.WriteString(fmt.Sprintf("  … and %d more\n", len(convs)-10))
				break
			}
			ag.mu.Lock()
			title := ag.grants[c].Title
			ag.mu.Unlock()
			b.WriteString(fmt.Sprintf("  %q: %s\n", title, failed[c]))
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

func (ag *agentSession) agentID() string {
	ag.mu.Lock()
	defer ag.mu.Unlock()
	if ag.agent == nil {
		return ""
	}
	return ag.agent.AgentID()
}

// childLine describes one conversation for a listing.
func (ag *agentSession) childLine(s *session) string {
	ag.mu.Lock()
	it := ag.grants[s.conv]
	ag.mu.Unlock()
	conn, _ := s.activeConn()
	state := "reconnecting"
	access := it.Access
	extra := ""
	if conn != nil && conn.Connected() {
		state = "open"
		if a := conn.Access(); a != "" {
			access = a
		}
		if conn.Behind() > 0 {
			extra = fmt.Sprintf(", %d unread at open", conn.Behind())
		}
		if conn.CanAddMembers() {
			extra += ", can add members (hub_add_members)"
		}
	}
	kind := strings.TrimSpace(it.Channel + " " + it.Kind)
	if kind != "" {
		kind = ", " + kind
	}
	if access == wire.AccessRead {
		access = "READ-ONLY"
	}
	return fmt.Sprintf("%s — %q%s, %s, %s%s", s.name, it.Title, kind, access, state, extra)
}

// listing describes the agent connection and every conversation on it.
func (ag *agentSession) listing(f listFilter) []string {
	ag.mu.Lock()
	children := make([]*session, 0, len(ag.children))
	for _, s := range ag.children {
		children = append(children, s)
	}
	state := "connected"
	switch {
	case ag.reconnecting:
		state = "DOWN, reconnecting automatically"
	case ag.agent == nil || !ag.agent.Connected():
		state = "down"
	}
	failed := make(map[string]string, len(ag.failed))
	for c, why := range ag.failed {
		failed[c] = why
	}
	closed := 0
	for c := range ag.userClosed {
		if _, ok := ag.grants[c]; ok {
			closed++
		}
	}
	grants := make(map[string]wire.Assignment, len(ag.grants))
	for c, it := range ag.grants {
		grants[c] = it
	}
	ag.mu.Unlock()
	sort.Slice(children, func(i, j int) bool { return children[i].name < children[j].name })
	var matched []string
	for _, s := range children {
		it := grants[s.conv]
		conn, _ := s.activeConn()
		if f.matchSession(s, conn, it.Title, strings.TrimSpace(it.Kind+" "+it.Channel), it.Access) {
			matched = append(matched, "    "+ag.childLine(s))
		}
	}
	header := fmt.Sprintf("%s — AGENT connection, %s, %d conversation(s) open (link=%s). Its "+
		"conversations, each addressed by its own name:", ag.name, state, len(children), ag.link)
	if f.active() {
		header = fmt.Sprintf("%s — AGENT connection, %s, %d conversation(s) open (link=%s), %d "+
			"matching the filter:", ag.name, state, len(children), ag.link, len(matched))
	}
	lines := []string{header}
	if f.limit > 0 && len(matched) > f.limit {
		more := len(matched) - f.limit
		matched = append(matched[:f.limit], fmt.Sprintf("    … and %d more matching; narrow the "+
			"filter or raise limit", more))
	}
	lines = append(lines, matched...)
	for c, why := range failed {
		// Kind, access and unread messages are only known once open.
		if f.kind == "" && f.access == "" && !f.unread && f.matchTitle(grants[c].Title) {
			lines = append(lines, fmt.Sprintf("    (not open) %q — could not be opened: %s", grants[c].Title, why))
		}
	}
	if closed > 0 {
		lines = append(lines, fmt.Sprintf("    %d conversation(s) closed by you; reopened when the agent "+
			"connection next reconnects", closed))
	}
	return lines
}

// disconnect ends the whole agent connection on request.
func (ag *agentSession) disconnect(remove bool) string {
	ag.end("")
	if !remove {
		return fmt.Sprintf("disconnected the agent connection %q and all its conversations", ag.name)
	}
	entries, err := connstore.ListForProject(ag.target.Project)
	removed := 0
	if err == nil {
		for _, le := range entries {
			if strings.HasPrefix(le.Target.Link, ag.link+"#") {
				if ok, _ := connstore.Delete(le.Target); ok {
					removed++
				}
			}
		}
	}
	return fmt.Sprintf("disconnected the agent connection %q and all its conversations", ag.name) +
		removeStored(true, ag.target) +
		fmt.Sprintf("; the stored read positions of %d conversation(s) are removed too", removed)
}

// closeChild records that the caller closed one conversation, so nothing
// reopens it until the agent connection itself reconnects.
func (ag *agentSession) closeChild(s *session) {
	ag.mu.Lock()
	ag.userClosed[s.conv] = true
	if ag.children[s.conv] == s {
		delete(ag.children, s.conv)
	}
	ag.mu.Unlock()
}

// handleSetKey gives one conversation of an agent connection its key file
// and reopens it with the key.
func (h *Hub) handleSetKey(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	name, err := req.RequireString("connection")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	keyFile, err := req.RequireString("keyFile")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	s, err := h.session(name)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if s.agent == nil {
		return mcp.NewToolResultError(fmt.Sprintf("%q is an ordinary connection: its key file is "+
			"given with hub_connect(keyFile: …). hub_set_key is for conversations of an agent "+
			"connection.", name)), nil
	}
	abs, err := filepath.Abs(keyFile)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("keyFile: %v", err)), nil
	}
	if _, err := hubenc.ReadKeyFile(abs); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("keyFile: %v — nothing was changed", err)), nil
	}
	ag := s.agent
	target := ag.childTarget(s.conv)
	stored, _, err := connstore.Get(target)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("the stored entry could not be read (%v) — "+
			"nothing was changed", err)), nil
	}
	stored.KeyFile = abs
	if err := connstore.Upsert(target, stored); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("the key file could not be recorded (%v) — "+
			"nothing was changed", err)), nil
	}
	if err := ag.reopen(s); err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("the key file is recorded, but reopening %q "+
			"with it failed: %v. It is used the next time the conversation opens.", name, err)), nil
	}
	conn, _ := s.activeConn()
	return mcp.NewToolResultText(fmt.Sprintf("%q reopened with the key file.%s Call "+
		"hub_catch_up(connection: %q) to read what you could not read before.",
		name, encryptionNote(conn, abs), name)), nil
}

// reopen closes a conversation's session and opens it again, picking up
// what is stored for it now.
func (ag *agentSession) reopen(s *session) error {
	ag.opMu.Lock()
	defer ag.opMu.Unlock()
	ag.mu.Lock()
	a := ag.agent
	it, granted := ag.grants[s.conv]
	ag.mu.Unlock()
	if a == nil || !a.Connected() {
		return fmt.Errorf("the agent connection is down; it is reopened when it reconnects")
	}
	if !granted {
		return fmt.Errorf("it is no longer shared with this agent")
	}
	if conn, _ := s.activeConn(); conn != nil {
		// Torn down first, so its ending is not mistaken for a drop.
		s.teardown(conn, true)
		conn.Close()
	}
	_, err := ag.openChild(a, it, nil, nil, nil)
	return err
}

// maxReopenAttempts bounds retrying a conversation the server closed while
// keeping it shared. The server announces the end of a grant with
// "revoked"; this only stops a loop against a server that never says.
const maxReopenAttempts = 6

// retryReopen reopens a conversation the server closed without ending its
// grant, backing off between attempts, until it is open, its grant ends,
// or the attempts run out.
func (ag *agentSession) retryReopen(a *hubconn.Agent, s *session, attempt int) {
	delay := reconnectBackoff(attempt)
	s.mu.Lock()
	if s.conn == nil {
		s.reconnectAt = time.Now().Add(delay)
	}
	s.mu.Unlock()
	time.Sleep(delay)
	ag.opMu.Lock()
	defer ag.opMu.Unlock()
	ag.mu.Lock()
	it, granted := ag.grants[s.conv]
	current := ag.agent == a && !ag.leaving && ag.children[s.conv] == s && !ag.userClosed[s.conv]
	ag.mu.Unlock()
	// A socket that has gone down since is the agent reconnect's to
	// reopen, with everything else.
	if !current || !granted || !a.Connected() {
		return
	}
	if conn, _ := s.activeConn(); conn != nil && conn.Connected() {
		return
	}
	_, err := ag.openChild(a, it, nil, nil, nil)
	switch {
	case err == nil:
		ag.alert(fmt.Sprintf("Conversation %q is open again after it briefly closed. "+
			"Call hub_catch_up(connection: %q) for anything said meanwhile.", s.name, s.name))
	case attempt >= maxReopenAttempts:
		ag.mu.Lock()
		if ag.children[s.conv] == s {
			delete(ag.children, s.conv)
		}
		ag.mu.Unlock()
		ag.release(s, fmt.Sprintf("the server closed it and reopening failed %d times (last: %v); "+
			"it is still shared and is opened again when the agent connection next reconnects",
			attempt, err), false)
	default:
		go ag.retryReopen(a, s, attempt+1)
	}
}

// announceNew queues the notice that s was opened, to go out with the
// others opened within newConvWindow — or sooner, in front of s's first
// content (see announcedBefore).
func (ag *agentSession) announceNew(s *session, title, note string) {
	ag.mu.Lock()
	defer ag.mu.Unlock()
	if ag.leaving {
		return
	}
	ag.pendingNew = append(ag.pendingNew, newConv{s: s, title: title, note: note})
	if ag.newTimer == nil {
		ag.newTimer = time.AfterFunc(ag.newWindow, ag.flushNew)
	}
}

// withdrawNew drops the queued notice for s, which did not open after all.
func (ag *agentSession) withdrawNew(s *session) {
	ag.mu.Lock()
	defer ag.mu.Unlock()
	kept := ag.pendingNew[:0]
	for _, n := range ag.pendingNew {
		if n.s != s {
			kept = append(kept, n)
		}
	}
	ag.pendingNew = kept
}

// announcedBefore sends the queued notices when s is among them, so the
// reader learns of a conversation before anything from it arrives.
func (ag *agentSession) announcedBefore(s *session) {
	ag.mu.Lock()
	pending := false
	for _, n := range ag.pendingNew {
		if n.s == s {
			pending = true
			break
		}
	}
	ag.mu.Unlock()
	if pending {
		ag.flushNew()
	}
}

// flushNew sends one notice for every conversation queued so far.
func (ag *agentSession) flushNew() {
	ag.mu.Lock()
	pending := ag.pendingNew
	ag.pendingNew = nil
	if ag.newTimer != nil {
		ag.newTimer.Stop()
		ag.newTimer = nil
	}
	ag.mu.Unlock()
	if len(pending) == 0 {
		return
	}
	lines := make([]string, 0, len(pending))
	for _, n := range pending {
		line := fmt.Sprintf("%q as connection %q (%s)", n.title, n.s.name, accessText(n.s))
		if n.note != "" {
			line += ", which " + n.note
		}
		lines = append(lines, line)
	}
	head := "NEW CONVERSATION shared with this agent, now open: "
	if len(pending) > 1 {
		head = fmt.Sprintf("%d NEW CONVERSATIONS shared with this agent, now open: ", len(pending))
	}
	ag.alert(head + strings.Join(lines, "; ") + ". Call hub_catch_up(connection: <name>) for " +
		"one before acting in it.")
}

// remindLoop reminds, in one notice per interval, about every conversation
// of this agent with live deliveries the reader has not confirmed — in
// place of one reminder per conversation, which with hundreds of them
// would be the noise the reminder is meant to cut through.
func (ag *agentSession) remindLoop() {
	t := time.NewTicker(ag.remindEvery)
	defer t.Stop()
	for range t.C {
		ag.mu.Lock()
		if ag.leaving {
			ag.mu.Unlock()
			return
		}
		children := make([]*session, 0, len(ag.children))
		for _, s := range ag.children {
			children = append(children, s)
		}
		ag.mu.Unlock()
		sort.Slice(children, func(i, j int) bool { return children[i].name < children[j].name })
		var lines []string
		more := 0
		for _, s := range children {
			conn, _ := s.activeConn()
			if conn == nil {
				continue
			}
			cursor, count, since, ok := conn.UnconfirmedState()
			if !ok || time.Since(since) < ag.remindEvery {
				continue
			}
			if len(lines) == 20 {
				more++
				continue
			}
			lines = append(lines, fmt.Sprintf("%s: %d unconfirmed since %s, last cursor %s",
				s.name, count, since.UTC().Format("15:04 UTC"), cursor))
		}
		if len(lines) == 0 {
			continue
		}
		if more > 0 {
			lines = append(lines, fmt.Sprintf("and %d more conversation(s)", more))
		}
		ag.alert(fmt.Sprintf("CONFIRM what you have read: %s. hub_confirm(connection, cursor) for "+
			"each, once you have the message whole; until then a reconnect delivers them again and "+
			"they hold room in the delivery window.", strings.Join(lines, "; ")))
	}
}
