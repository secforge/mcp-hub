package mcptools

import (
	"github.com/secforge/harness-transport/deliver"

	"fmt"

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
	// NO catchUpPushFetchTimeout. One was declared here and never used —
	// the fetch waits hubconn.AckWaitTimeout like every other request —
	// so the file documented a 30s bound that did not exist. A constant
	// nobody reads is a claim about behaviour, and this one was false.
	// Removed rather than wired up: the walk's per-request bound belongs
	// with every other request's, not as a second number here that has
	// to be kept in agreement with it. Pointed out by an external
	// reviewer, 2026-09-20.
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
	s.pushCatchUpSummary(s.walkCatchUpPush(conn, id, anchor, want))
}

// walkCatchUpPush is the walk itself, separated from the reporting so a
// test can assert WHICH ending a run reached without a reachable harness
// to push into. Separation suggested by the external reviewer whose
// findings this file's error branch comes from, 2026-09-20.
func (s *session) walkCatchUpPush(conn *hubconn.Conn, id connstore.Target, anchor wire.Anchor,
	want catchUpWant) catchUpResult {
	var res catchUpResult
	maxMessages, maxBytes := want.limits()

	// ONE MESSAGE IS HELD BACK until the next fetch answers, because only
	// that answer says whether anything follows it: each delivery's trailer
	// states "more is waiting" only when another message really is, and the
	// last one before "caught up" says nothing of the kind.
	var held *hubconn.Event
	heldText := ""
	release := func(more bool) {
		ev := *held
		held = nil
		if err := s.pushCatchUpMessage(conn, id, ev, heldText, more); err != nil {
			if res.Err == nil {
				res.Err = err
			}
			return
		}
		res.Delivered++
		res.Bytes += len(heldText)
	}

	for {
		count, bytes := res.Delivered, res.Bytes
		if held != nil {
			count, bytes = count+1, bytes+len(heldText)
		}
		if count >= maxMessages || bytes >= maxBytes {
			break
		}
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
		// A REFUSAL, BEFORE THE CURSOR TEST. An error frame carries no
		// cursor, so testing for a missing cursor first would report every
		// refusal as "a message arrived with no cursor" and never show the
		// reason. bad_anchor is the live case: chat-relay answers it when a
		// stored cursor no longer resolves, which is exactly when a reader
		// most needs to be told.
		if ev.Kind == "error" {
			code := ev.Code
			if code == "" {
				code = "no code"
			}
			res.Err = fmt.Errorf("the server refused the walk (code=%s, retryable=%t): %s",
				code, ev.Retryable, ev.Text)
			break
		}
		if ev.Cursor == "" {
			// Nothing to anchor the next request on, so continuing would
			// re-fetch this same message forever.
			res.StoppedBy = "a message arrived with no cursor, so the walk could not continue"
			break
		}

		if held != nil {
			if release(true); res.Err != nil {
				break
			}
		}
		text := conn.ShapeForPush(ev)
		text += s.saveReceivedAttachments(conn, []hubconn.Event{ev})
		held, heldText = &ev, text
		anchor = wire.Anchor{Cursor: ev.Cursor}
	}
	if held != nil {
		release(false)
	}

	if res.StoppedBy == "" && !res.CaughtUp && res.Err == nil {
		res.StoppedBy = fmt.Sprintf("this run reached its limit of %d messages or %d KB",
			maxMessages, maxBytes/1024)
	}
	return res
}

// pushCatchUpMessage pushes one walked message and records what the
// transport observed.
//
// A PUSH IS NOT A HAND-OVER unless the transport says more than "the bytes
// left": a nil error with ObservedNothing means the write succeeded and
// arrival is unverified, which is the case for every push into Claude. So
// a nil error alone does not move the persisted position; the message goes
// into the "delivered, not confirmed" set instead, which an explicit
// hub_confirm turns into position. A failed push leaves the message on the
// server with the position unmoved.
func (s *session) pushCatchUpMessage(conn *hubconn.Conn, id connstore.Target, ev hubconn.Event,
	text string, more bool) error {
	receipt, err := s.pusher.Push(ev.Cursor, text, more)
	if err != nil {
		return err
	}
	conn.NoteHandedOver([]hubconn.Event{ev})
	if receipt.Observation > deliver.ObservedNothing {
		s.mu.Lock()
		s.lastHandedOverCursor = ev.Cursor
		s.mu.Unlock()
		setCatchUpCursor(id, ev.Cursor)
		s.noteDelivered(ev.Cursor)
	} else {
		s.recordHandedOver([]hubconn.Event{ev})
	}
	return nil
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
		text = fmt.Sprintf("[hub: catch-up on %s STOPPED after %d message(s) — %v. Nothing is lost: the "+
			"position only advanced for what was delivered, so calling hub_catch_up again resumes "+
			"exactly where this stopped]", s.name, res.Delivered, res.Err)
	case res.CaughtUp:
		text = fmt.Sprintf("[hub: caught up on %s — %d message(s) delivered, nothing further on the "+
			"server. Confirm the last one you have COMPLETE with hub_confirm, or piggyback it on "+
			"your next send; until you do, a reconnect re-walks them]", s.name, res.Delivered)
	default:
		text = fmt.Sprintf("[hub: catch-up on %s PAUSED after %d message(s), %d KB — %s. More remains. "+
			"Call hub_catch_up again for the next batch; confirm what you have read first so the "+
			"next run starts from there rather than re-walking]",
			s.name, res.Delivered, res.Bytes/1024, res.StoppedBy)
	}
	// No cursor: this is this client's own words about a run, not a
	// message anyone can re-fetch.
	if _, err := s.pusher.Push("", text, false); err != nil {
		s.note(fmt.Sprintf("a catch-up run finished but its summary could not be "+
			"delivered (%v): %d message(s) were pushed.", err, res.Delivered))
	}
}
