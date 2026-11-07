package mcptools

import (
	"fmt"
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
// on the others too, wherever that moves nothing past an unconfirmed
// message.
//
// A message is the same message only on the same thread: an externalId is
// the platform's own id and unique only within its thread, so the key is
// (joined.thread, externalId), and a server that names no thread makes no
// copies.

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

// heldCopy is a message held on one connection because another delivered
// it first.
type heldCopy struct {
	cursor  string
	first   copyFirst
	covered bool
}

func copyKey(thread, externalID string) string { return thread + "\x00" + externalID }

// copyOf reports whether ev reached another open connection first, and
// which; otherwise this connection is recorded as where it was first.
func (s *session) copyOf(conn *hubconn.Conn, ev hubconn.Event) (copyFirst, bool) {
	thread := conn.Thread()
	if thread == "" || ev.Kind != "msg" || ev.ExternalID == "" || ev.Cursor == "" || ev.Presence != nil {
		return copyFirst{}, false
	}
	key := copyKey(thread, ev.ExternalID)
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

// holdCopy records ev as held here: handed over, so no walk offers it
// again, and waiting for its first delivery to be confirmed.
func (s *session) holdCopy(ev hubconn.Event, first copyFirst) {
	s.noteDelivered(ev.Cursor)
	s.recordHandedOver([]hubconn.Event{ev})
	s.mu.Lock()
	for _, h := range s.copiesHeld {
		if h.cursor == ev.Cursor {
			s.mu.Unlock()
			return
		}
	}
	s.copiesHeld = append(s.copiesHeld, heldCopy{cursor: ev.Cursor, first: first})
	s.mu.Unlock()

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
		s.note(fmt.Sprintf("%q and %q are copies of the same thread, through two accounts. A message "+
			"both receive is delivered once, on the connection it reached first; confirming it there "+
			"confirms it on the other as well. To answer, use whichever account should answer.",
			first.owner, s.name))
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

// copiesConfirmed follows a confirm of cursor on from: copies held
// elsewhere whose first delivery it covers are covered, and a connection
// whose unconfirmed messages are all covered copies is confirmed up to
// them. Copies held on from itself and covered by its own position are
// let go.
func (h *Hub) copiesConfirmed(from *session, cursor string) {
	at := from.deliveredIndex(cursor)
	covers := func(c string) bool {
		if c == cursor {
			return true
		}
		i := from.deliveredIndex(c)
		return at >= 0 && i >= 0 && i <= at
	}
	from.mu.Lock()
	kept := from.copiesHeld[:0]
	for _, hc := range from.copiesHeld {
		if !covers(hc.cursor) {
			kept = append(kept, hc)
		}
	}
	from.copiesHeld = kept
	from.mu.Unlock()

	for _, s := range h.allSessions() {
		if s == from {
			continue
		}
		s.mu.Lock()
		changed := false
		for i := range s.copiesHeld {
			hc := &s.copiesHeld[i]
			if !hc.covered && hc.first.owner == from.name && covers(hc.first.cursor) {
				hc.covered, changed = true, true
			}
		}
		prefix := 0
		for prefix < len(s.copiesHeld) && s.copiesHeld[prefix].covered {
			prefix++
		}
		held := len(s.copiesHeld)
		last := ""
		if prefix > 0 {
			last = s.copiesHeld[prefix-1].cursor
		}
		s.mu.Unlock()
		if !changed || last == "" {
			continue
		}
		conn, _ := s.activeConn()
		if conn == nil {
			continue
		}
		// Only where nothing but held copies is unconfirmed there:
		// otherwise a confirm would move past a message the reader has
		// not confirmed. Those copies are covered when it confirms one.
		if _, count, _, ok := conn.UnconfirmedState(); ok && count > held {
			continue
		}
		go func(s *session, conn *hubconn.Conn, cursor string) {
			_, _ = s.confirmCursor(conn, cursor)
		}(s, conn, last)
	}
}
