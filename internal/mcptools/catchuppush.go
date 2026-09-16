package mcptools

import (
	"fmt"
	"time"

	"github.com/secforge/mcp-hub/internal/connstore"
	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/wire"
)

// Catching up by PUSH exists because the pull contract and the push
// harness want opposite things.
//
// hub_catch_up returns exactly one message per call, deliberately: a
// returned message can never hide beside another one, and the terminator
// is unambiguous. That is right for a model that has to ask. It is wrong
// for a model whose harness already takes deliveries — there, twenty
// unread messages meant twenty tool calls and twenty turns, each carrying
// one message the model could simply have been handed.
//
// So in push mode the model asks ONCE and the backlog is delivered the
// way live traffic is: one push per message, oversized bodies spilled to
// a file exactly as they are live, and a closing message saying what
// happened and what to do next. The per-call contract is unchanged for
// everyone else.
const (
	// catchUpPushBudget caps one run. The figure is the delivery budget's
	// own window, for the reason that budget exists: a receiver that
	// stopped participating had absorbed roughly 1.3 MB, and a backlog is
	// the one place where that much arrives at once without anyone
	// choosing it. Past this the run stops and says so rather than
	// spending the rest of the reader's attention unasked.
	catchUpPushBudget = 500 * 1024
	// catchUpPushMaxMessages bounds a run by count as well as by bytes.
	// Two limits because the evidence cannot separate which exhausted the
	// receiver — 1.29 MB over four messages killed one, 1.00 MB in a
	// single message did not kill another.
	catchUpPushMaxMessages = 200
	// catchUpPushFetchTimeout bounds waiting for one message from the
	// server. A run that stalls should end with a report rather than hold
	// the connection's goroutine indefinitely.
	catchUpPushFetchTimeout = 30 * time.Second
)

// catchUpWant is how much of the backlog the model asked for. It can
// only ever ask for LESS.
//
// The model knows something this code does not — how much room it has
// left, and whether it wants the whole backlog or a look at the next
// few. So it may lower either limit. It may not raise them: the ceiling
// is what stops a backlog spending a reader's whole remaining attention
// in one run, and a limit the reader can raise is not a ceiling. Asking
// for more than the cap gets the cap, silently on the model's side but
// stated in the closing message, so the number it reads is the one that
// applied.
type catchUpWant struct {
	Messages int
	KB       int
}

// limits resolves a request against the hard caps.
func (w catchUpWant) limits() (messages, bytes int) {
	messages, bytes = catchUpPushMaxMessages, catchUpPushBudget
	if w.Messages > 0 && w.Messages < messages {
		messages = w.Messages
	}
	if w.KB > 0 && w.KB*1024 < bytes {
		bytes = w.KB * 1024
	}
	return messages, bytes
}

// catchUpResult is what one push run did, for the closing message.
type catchUpResult struct {
	Delivered int
	Bytes     int
	CaughtUp  bool
	StoppedBy string
	Err       error
}

// runCatchUpPush walks the backlog and pushes each message, stopping at
// the budget or when the server says there is nothing further.
//
// Runs on its own goroutine: the tool call returns immediately, because
// the delivery is the point rather than the answer. Everything it learns
// goes to the model the same way a live message does.
func (s *session) runCatchUpPush(conn *hubconn.Conn, id connstore.Target, anchor wire.Anchor,
	want catchUpWant) {
	var res catchUpResult
	maxMessages, maxBytes := want.limits()

	for res.Delivered < maxMessages && res.Bytes < maxBytes {
		ev, ok, err := conn.RequestMessageAfterAwaiting(anchor)
		if err != nil {
			res.Err = err
			break
		}
		if !ok {
			res.StoppedBy = "the server did not answer in time"
			break
		}
		if ev.Kind == "noMoreMessages" {
			res.CaughtUp = true
			break
		}
		if ev.Cursor == "" {
			// Nothing to anchor the next request on, so continuing would
			// re-fetch this same message forever.
			res.StoppedBy = "a message arrived with no cursor, so the walk could not continue"
			break
		}

		text := conn.ShapeForPush(ev)
		text += s.saveReceivedAttachments(conn, []hubconn.Event{ev})
		if _, err := s.pusher.Push(ev.Cursor, text, true); err != nil {
			// The message is still on the server and the position has not
			// moved, so this is recoverable — but only if it is said.
			res.Err = err
			break
		}

		// The push IS the hand-over, so the position advances here, the
		// same way a synchronous delivery advances it.
		s.mu.Lock()
		s.lastHandedOverCursor = ev.Cursor
		s.mu.Unlock()
		setCatchUpCursor(id, ev.Cursor)
		conn.NoteHandedOver([]hubconn.Event{ev})
		s.noteDelivered(ev.Cursor)

		anchor = wire.Anchor{Cursor: ev.Cursor}
		res.Delivered++
		res.Bytes += len(text)
	}

	if res.StoppedBy == "" && !res.CaughtUp && res.Err == nil {
		res.StoppedBy = fmt.Sprintf("this run reached its limit of %d messages or %d KB",
			maxMessages, maxBytes/1024)
	}
	s.pushCatchUpSummary(res)
}

// pushCatchUpSummary is the last thing a run delivers: what happened and
// what to do about it. Sent as a message rather than returned, because
// the tool call that started the run is long gone — and a run that ended
// without saying so would leave a reader unable to tell "caught up" from
// "stopped", which is the one distinction this whole codebase exists to
// keep.
func (s *session) pushCatchUpSummary(res catchUpResult) {
	var text string
	switch {
	case res.Err != nil:
		text = fmt.Sprintf("[hub: catch-up STOPPED after %d message(s) — %v. Nothing is lost: the "+
			"position only advanced for what was delivered, so calling hub_catch_up again resumes "+
			"exactly where this stopped]", res.Delivered, res.Err)
	case res.CaughtUp:
		text = fmt.Sprintf("[hub: caught up — %d message(s) delivered, nothing further on the "+
			"server. Confirm the last one you have COMPLETE with hub_confirm, or piggyback it on "+
			"your next send; until you do, a reconnect re-walks them]", res.Delivered)
	default:
		text = fmt.Sprintf("[hub: catch-up PAUSED after %d message(s), %d KB — %s. More remains. "+
			"Call hub_catch_up again for the next batch; confirm what you have read first so the "+
			"next run starts from there rather than re-walking]",
			res.Delivered, res.Bytes/1024, res.StoppedBy)
	}
	// No cursor: this is this client's own words about a run, not a
	// message anyone can re-fetch.
	if _, err := s.pusher.Push("", text, false); err != nil {
		s.note(fmt.Sprintf("a catch-up run finished but its summary could not be "+
			"delivered (%v): %d message(s) were pushed.", err, res.Delivered))
	}
}
