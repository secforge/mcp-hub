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
func (h *Hub) runCatchUpPush(conn *hubconn.Conn, id connstore.Target, anchor wire.Anchor) {
	var res catchUpResult

	for res.Delivered < catchUpPushMaxMessages && res.Bytes < catchUpPushBudget {
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
		text += h.saveReceivedAttachments(conn, []hubconn.Event{ev})
		if _, err := h.pusher.Push(ev.Cursor, text, true); err != nil {
			// The message is still on the server and the position has not
			// moved, so this is recoverable — but only if it is said.
			res.Err = err
			break
		}

		// The push IS the hand-over, so the position advances here, the
		// same way a synchronous delivery advances it.
		h.mu.Lock()
		h.lastHandedOverCursor = ev.Cursor
		h.mu.Unlock()
		setCatchUpCursor(id, ev.Cursor)
		conn.NoteHandedOver([]hubconn.Event{ev})

		anchor = wire.Anchor{Cursor: ev.Cursor}
		res.Delivered++
		res.Bytes += len(text)
	}

	if res.StoppedBy == "" && !res.CaughtUp && res.Err == nil {
		res.StoppedBy = "this run reached its delivery budget"
	}
	h.pushCatchUpSummary(res)
}

// pushCatchUpSummary is the last thing a run delivers: what happened and
// what to do about it. Sent as a message rather than returned, because
// the tool call that started the run is long gone — and a run that ended
// without saying so would leave a reader unable to tell "caught up" from
// "stopped", which is the one distinction this whole codebase exists to
// keep.
func (h *Hub) pushCatchUpSummary(res catchUpResult) {
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
	if _, err := h.pusher.Push("", text, false); err != nil {
		h.noteAutoReconnect(fmt.Sprintf("a catch-up run finished but its summary could not be "+
			"delivered (%v): %d message(s) were pushed.", err, res.Delivered))
	}
}
