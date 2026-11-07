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

// skipEcho records an echo the reader did not ask for: handed over, so no
// walk offers it again, and settled, so it never holds the read position
// back. Where nothing unconfirmed precedes it, the position moves past it.
func (s *session) skipEcho(ev hubconn.Event) {
	if ev.Cursor == "" {
		return
	}
	s.recordHandedOver([]hubconn.Event{ev})
	s.mark(ev.Cursor, func(h *handed) { h.echo = true })
	s.settleFront()
}

// withoutOwnEchoes takes out of items the echoes the reader did not ask
// for.
func (s *session) withoutOwnEchoes(items []hubconn.PushItem) []hubconn.PushItem {
	if s.wantsOwnEchoes() {
		return items
	}
	kept := items[:0]
	for _, it := range items {
		if len(it.Merged) > 0 || !isOwnEcho(it.Event) {
			kept = append(kept, it)
			continue
		}
		s.skipEcho(it.Event)
	}
	return kept
}
