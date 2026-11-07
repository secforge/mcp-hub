package mcptools

import (
	"github.com/secforge/mcp-hub/internal/hubconn"
)

// wantsOwnEchoes reports whether the reader asked to be delivered what
// this connection itself sent, coming back.
func (s *session) wantsOwnEchoes() bool {
	if s.agent != nil {
		return s.agent.ownEchoes
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ownEchoes
}

// isOwnEcho reports whether ev is a link's echo of something this
// connection did itself: a message it sent, or a reaction, edit or delete
// it made. Someone else writing under the same account is not one.
func isOwnEcho(ev hubconn.Event) bool {
	if !ev.Own || ev.Presence != nil {
		return false
	}
	switch ev.Kind {
	case "msg", "reactionChanged", "messageEdited", "messageDeleted":
		return true
	}
	return false
}

// withoutOwnEchoes takes out of items the echoes the reader did not ask
// for. One that carries a cursor, arriving when nothing before it is left
// unconfirmed, is confirmed here: the reader wrote it, and leaving it
// unread would keep the read position behind the reader's own message
// until something else arrived.
func (s *session) withoutOwnEchoes(conn *hubconn.Conn, items []hubconn.PushItem) []hubconn.PushItem {
	if s.wantsOwnEchoes() {
		return items
	}
	kept := items[:0]
	for _, it := range items {
		if len(it.Merged) > 0 || !isOwnEcho(it.Event) {
			kept = append(kept, it)
			continue
		}
		s.noteDelivered(it.Cursor)
		if it.Cursor == "" {
			continue
		}
		if _, count, _, ok := conn.UnconfirmedState(); ok && count == 1 {
			cursor := it.Cursor
			go func() { _, _ = s.confirmCursor(conn, cursor) }()
		}
	}
	return kept
}
