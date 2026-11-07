package mcptools

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"

	"github.com/secforge/mcp-hub/internal/hubconn"
)

// One platform thread can reach this client through several connections:
// an agent following a Teams channel from two accounts holds two copies of
// it, so that it can answer from either. Every message then arrives once
// per copy. Delivering each copy would cost the reader a turn per copy and
// a share of the delivery window per copy, for nothing it did not already
// have. So a message is delivered on the connection it reached first, and
// held on the others: a confirm of it where it was delivered confirms it
// on the others too, but never past anything else handed over there and
// not confirmed.
//
// A message is the same message only on the same thread of the same
// server: an externalId is the platform's own id and unique only within
// its thread, and a thread value is the server's own word, so the key is
// (server, joined.thread, externalId), and a server that names no thread
// makes no copies.

// maxCopies bounds how many messages the ledger remembers.
const maxCopies = 5000

type copyLedger struct {
	mu    sync.Mutex
	first map[string]copyFirst
	order []string
	// told records pairs of connections already said to be copies.
	told map[string]bool
}

// copyFirst is where a message was delivered first.
type copyFirst struct {
	owner, cursor string
}

// handed is one cursor this connection handed over since its last
// confirm. A held copy waits for its first delivery's confirm; an echo of
// the connection's own send was never the reader's to confirm.
type handed struct {
	cursor  string
	copyOf  *copyFirst
	covered bool
	echo    bool
}

// settled says whether a confirm may move past this entry without the
// reader having confirmed it.
func (h handed) settled() bool { return h.echo || (h.copyOf != nil && h.covered) }

func copyKey(server, thread, externalID string) string {
	return server + "\x00" + thread + "\x00" + externalID
}

// serverOf is the server this connection talks to: scheme and host of its
// link, never the fragment that carries the credential.
func (s *session) serverOf() string {
	s.mu.Lock()
	link := s.connTarget.Link
	s.mu.Unlock()
	if link == "" && s.agent != nil {
		link = s.agent.link
	}
	u, err := url.Parse(link)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}

// copyOf reports whether ev reached another open connection to the same
// server first, and which; otherwise this connection is recorded as where
// it was first.
func (s *session) copyOf(conn *hubconn.Conn, ev hubconn.Event) (copyFirst, bool) {
	thread := conn.Thread()
	server := s.serverOf()
	if thread == "" || server == "" || ev.Kind != "msg" || ev.ExternalID == "" || ev.Cursor == "" ||
		ev.Presence != nil {
		return copyFirst{}, false
	}
	key := copyKey(server, thread, ev.ExternalID)
	l := &s.hub.copies
	l.mu.Lock()
	rec, ok := l.first[key]
	l.mu.Unlock()
	if ok && rec.owner != s.name && s.hub.isOpen(rec.owner) {
		return rec, true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.first == nil {
		l.first = map[string]copyFirst{}
	}
	if _, had := l.first[key]; !had {
		l.order = append(l.order, key)
		for len(l.order) > maxCopies {
			delete(l.first, l.order[0])
			l.order = l.order[1:]
		}
	}
	l.first[key] = copyFirst{owner: s.name, cursor: ev.Cursor}
	return copyFirst{}, false
}

// isOpen reports whether a connection of that name is open.
func (h *Hub) isOpen(name string) bool {
	s, err := h.session(name)
	if err != nil {
		return false
	}
	conn, _ := s.activeConn()
	return conn != nil
}

// mark updates cursor's entry in what was handed over since the last
// confirm.
func (s *session) mark(cursor string, f func(*handed)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.sinceConfirm {
		if s.sinceConfirm[i].cursor == cursor {
			f(&s.sinceConfirm[i])
			return
		}
	}
}

// handedLocked adds cursor to what was handed over since the last
// confirm, once. Caller holds s.mu.
func (s *session) handedLocked(cursor string) {
	if cursor == "" {
		return
	}
	for _, h := range s.sinceConfirm {
		if h.cursor == cursor {
			return
		}
	}
	if len(s.sinceConfirm) >= maxRecentCursors {
		// Too much unconfirmed to keep track of: nothing more is
		// confirmed on the reader's behalf until it confirms.
		s.unordered = true
		return
	}
	s.sinceConfirm = append(s.sinceConfirm, handed{cursor: cursor})
}

// handedInOrder records items in the order they came off the connection,
// before any is sent: a push waiting for room is still ahead of what
// arrives after it.
func (s *session) handedInOrder(items []hubconn.PushItem) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, it := range items {
		if len(it.Merged) > 0 {
			for _, ev := range it.Merged {
				s.handedLocked(ev.Cursor)
			}
			continue
		}
		s.handedLocked(it.Cursor)
	}
}

// walkStart and walkEnd bracket a catch-up walk; see session.walkAt.
func (s *session) walkStart() {
	s.mu.Lock()
	s.walkAt = len(s.sinceConfirm)
	s.mu.Unlock()
}

func (s *session) walkEnd() {
	s.mu.Lock()
	s.walkAt = -1
	s.mu.Unlock()
}

// handedWalk records a cursor the walk hands over, in its place.
func (s *session) handedWalk(cursor string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cursor == "" {
		return
	}
	for _, h := range s.sinceConfirm {
		if h.cursor == cursor {
			return
		}
	}
	if s.walkAt < 0 || s.walkAt > len(s.sinceConfirm) || len(s.sinceConfirm) >= maxRecentCursors {
		s.unordered = true
		return
	}
	s.sinceConfirm = append(s.sinceConfirm[:s.walkAt],
		append([]handed{{cursor: cursor}}, s.sinceConfirm[s.walkAt:]...)...)
	s.walkAt++
}

// settledUpTo is the last cursor a confirm can move to without passing
// anything handed over here that the reader has not confirmed: the end
// of the run of settled entries at the front.
func (s *session) settledUpTo() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unordered {
		return ""
	}
	last := ""
	for _, h := range s.sinceConfirm {
		if !h.settled() {
			break
		}
		last = h.cursor
	}
	return last
}

// holdCopy records ev as held here: handed over, so no walk offers it
// again, and waiting for its first delivery to be confirmed.
func (s *session) holdCopy(ev hubconn.Event, first copyFirst) {
	s.noteDelivered(ev.Cursor)
	s.recordHandedOver([]hubconn.Event{ev})
	f := first
	s.mark(ev.Cursor, func(h *handed) { h.copyOf = &f })

	pair := first.owner + "\x00" + s.name
	l := &s.hub.copies
	l.mu.Lock()
	said := l.told[pair]
	if l.told == nil {
		l.told = map[string]bool{}
	}
	l.told[pair] = true
	l.mu.Unlock()
	if !said {
		firstLabel := fmt.Sprintf("%q", first.owner)
		if fs, err := s.hub.session(first.owner); err == nil {
			firstLabel = fs.copyLabel()
		}
		s.note(fmt.Sprintf("%s and %s are copies of the same thread, through two accounts. A message "+
			"both receive is delivered once, on the connection it reached first, and says so; "+
			"confirming it there confirms it on the other as well. To answer, use whichever account "+
			"should answer.", firstLabel, s.copyLabel()))
	}
}

// withoutCopies takes out of items the messages another copy of this
// thread delivered first.
func (s *session) withoutCopies(conn *hubconn.Conn, items []hubconn.PushItem) []hubconn.PushItem {
	kept := items[:0]
	for _, it := range items {
		if len(it.Merged) == 0 {
			if first, dup := s.copyOf(conn, it.Event); dup {
				s.holdCopy(it.Event, first)
				continue
			}
		}
		kept = append(kept, it)
	}
	return kept
}

// deliveredIndex is cursor's place in what this connection handed over,
// or -1.
func (s *session) deliveredIndex(cursor string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, c := range s.deliveredOrder {
		if c == cursor {
			return i
		}
	}
	return -1
}

// confirmedHere drops from what was handed over since the last confirm
// everything up to cursor; latest drops all of it. An unknown cursor
// drops nothing, so nothing unconfirmed is forgotten.
func (s *session) confirmedHere(cursor string, latest bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if latest {
		s.sinceConfirm, s.unordered = nil, false
		if s.walkAt >= 0 {
			s.walkAt = 0
		}
		return
	}
	for i, h := range s.sinceConfirm {
		if h.cursor == cursor {
			s.sinceConfirm = append([]handed(nil), s.sinceConfirm[i+1:]...)
			if s.walkAt >= 0 {
				s.walkAt = max(s.walkAt-(i+1), 0)
			}
			if len(s.sinceConfirm) == 0 {
				s.unordered = false
			}
			return
		}
	}
}

// settleFront confirms this connection up to the end of its settled run,
// if there is one.
func (s *session) settleFront() {
	last := s.settledUpTo()
	if last == "" {
		return
	}
	conn, _ := s.activeConn()
	if conn == nil {
		return
	}
	go func() { _, _ = s.confirmCursor(conn, last) }()
}

// copiesConfirmed follows a confirm of cursor on from: copies held
// elsewhere whose first delivery it covers are covered, and each such
// connection is confirmed as far as its settled run reaches.
func (h *Hub) copiesConfirmed(from *session, cursor string) {
	at := from.deliveredIndex(cursor)
	covers := func(c string) bool {
		if c == cursor {
			return true
		}
		i := from.deliveredIndex(c)
		return at >= 0 && i >= 0 && i <= at
	}
	for _, s := range h.allSessions() {
		if s == from {
			continue
		}
		s.mu.Lock()
		changed := false
		for i := range s.sinceConfirm {
			hc := &s.sinceConfirm[i]
			if hc.copyOf != nil && !hc.covered && hc.copyOf.owner == from.name && covers(hc.copyOf.cursor) {
				hc.covered, changed = true, true
			}
		}
		s.mu.Unlock()
		if changed {
			s.settleFront()
		}
	}
}

// copiesOpen names the other open connections that are copies of this
// connection's thread on the same server.
func (s *session) copiesOpen(conn *hubconn.Conn) []string {
	thread, server := conn.Thread(), s.serverOf()
	if thread == "" || server == "" {
		return nil
	}
	var names []string
	for _, o := range s.hub.allSessions() {
		if o == s {
			continue
		}
		oc, _ := o.activeConn()
		if oc == nil || oc.Thread() != thread || o.serverOf() != server {
			continue
		}
		names = append(names, o.copyLabel())
	}
	sort.Strings(names)
	return names
}

// copyLabel is how a copy is named to the reader: its connection, and the
// account it acts as where that is known.
func (s *session) copyLabel() string {
	if acct := s.accountName(); acct != "" {
		return fmt.Sprintf("%q (as %s)", s.name, noticeText(acct))
	}
	return fmt.Sprintf("%q", s.name)
}

// alsoOn is the line in front of a message that other open copies of its
// thread received too, or "".
func (s *session) alsoOn(conn *hubconn.Conn, ev hubconn.Event) string {
	if ev.Kind != "msg" || ev.Presence != nil || ev.ExternalID == "" {
		return ""
	}
	others := s.copiesOpen(conn)
	if len(others) == 0 {
		return ""
	}
	return "[hub: this message is also in " + strings.Join(others, ", ") + " — the same thread " +
		"through another account; it is delivered once, here. Answer through whichever account " +
		"should answer.]\n"
}

// accountName is the platform account this conversation acts as, as its
// grant names it, or "".
func (s *session) accountName() string {
	if s.agent == nil {
		return ""
	}
	s.agent.mu.Lock()
	defer s.agent.mu.Unlock()
	if a := s.agent.grants[s.conv].Account; a != nil {
		if a.Name != "" {
			return a.Name
		}
		return a.ID
	}
	return ""
}
