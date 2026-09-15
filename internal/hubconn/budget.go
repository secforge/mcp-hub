package hubconn

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// The receiver's context is the scarce resource in this system, and it is
// spent by whoever decides what to deliver. A transport that pushes
// everything it receives is spending someone else's budget without asking.
//
// Measured, 2026-09-15: a receiving session stopped participating after
// roughly 1.3 MB delivered across four messages — still accepting
// connections, never answering again. That failure is invisible from the
// sending side (every send succeeded), indistinguishable from a slow reply
// until it simply does not end, and unreachable by any acknowledgement,
// because an ack says a message was taken, not that the receiver can still
// think.
//
// So delivery is governed by two independent rules:
//
//   - SPILL: no single push may be larger than SpillBytes. Anything bigger
//     is written to a file and announced by head, size, path and cursor.
//     Nothing is lost; the reader decides whether to spend the budget.
//   - WINDOW: the total pushed and not yet confirmed is capped. Past it,
//     bodies stop and a notice says so. Confirmation reopens it.
//
// Both are needed and neither substitutes for the other: the first bounds
// one delivery, the second bounds their sum, and it was the sum that
// killed the receiver.
type budget struct {
	mu sync.Mutex

	// spillBytes is the largest body delivered inline. Deliberately far
	// above ordinary traffic — real messages are a few KB — so it never
	// fires in the normal case and catches only the pathological one.
	spillBytes int
	// spillDir yields the directory oversized bodies are written to. A
	// function rather than a path because the directory is created on
	// first use and torn down with the connection, and a connection that
	// never receives an oversized message should never create it. Nil
	// disables spilling, which degrades the policy to
	// truncation-with-cursor rather than to unbounded delivery.
	spillDir func() (string, error)

	// windowBytes and windowCount cap what may be outstanding. Two limits
	// because the evidence cannot yet distinguish whether volume or
	// message count exhausted the receiver: 1.29 MB over four messages
	// killed one, 1.00 MB in a single message did not kill another. Two
	// data points, two hypotheses, so both are bounded until someone
	// separates them.
	windowBytes int
	windowCount int

	// outstanding is the ledger, in delivery order. A confirm names a
	// CURSOR, and everything at or before it is released — so the ledger
	// is walked as a prefix and the remainder stays charged. Zeroing on
	// confirm would credit back budget that is still occupied: messages
	// delivered after the confirmed cursor are still sitting in the
	// reader's context, unread rather than freed.
	outstanding []charge
	bytes       int
	// held counts what the window refused since it last closed, so the
	// notice can say how much is waiting rather than that something is.
	held int
	// announced records that the closed window has been reported once.
	// Repeating it on every event would spend the budget the gate exists
	// to protect.
	announced bool
}

type charge struct {
	cursor string
	bytes  int
}

// newBudget returns the default policy. Values are deliberately far apart:
// the spill threshold guards one message, the window guards a session, and
// a session is many messages.
func newBudget() *budget {
	return &budget{
		spillBytes:  128 * 1024,
		windowBytes: 500 * 1024,
		windowCount: 200,
	}
}

// SetDeliveryBudget configures the push policy. spillDir is called only
// when something actually has to be spilled, and whatever it creates is
// not cleaned up here — that lifetime belongs to whoever owns the
// connection. A zero limit leaves that limit at its default.
func (c *Conn) SetDeliveryBudget(spillDir func() (string, error), spillBytes, windowBytes, windowCount int) {
	c.budget.mu.Lock()
	defer c.budget.mu.Unlock()
	c.budget.spillDir = spillDir
	if spillBytes > 0 {
		c.budget.spillBytes = spillBytes
	}
	if windowBytes > 0 {
		c.budget.windowBytes = windowBytes
	}
	if windowCount > 0 {
		c.budget.windowCount = windowCount
	}
}

// charge records a delivery against the window. Only pushes are charged:
// what a reader fetches for itself with hub_catch_up or hub_read is its
// own decision to spend, and charging for it would make reading feel
// expensive, which is the opposite of the intent.
func (b *budget) charge(cursor string, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.outstanding = append(b.outstanding, charge{cursor: cursor, bytes: n})
	b.bytes += n
}

// release drops every charge at or before cursor and subtracts exactly
// those bytes. Everything after stays charged — see outstanding.
func (b *budget) release(cursor string) {
	if cursor == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	cut := -1
	for i, ch := range b.outstanding {
		if ch.cursor == cursor {
			cut = i
			break
		}
	}
	if cut < 0 {
		return
	}
	for _, ch := range b.outstanding[:cut+1] {
		b.bytes -= ch.bytes
	}
	if b.bytes < 0 {
		b.bytes = 0
	}
	b.outstanding = append([]charge(nil), b.outstanding[cut+1:]...)
	if b.bytes < b.windowBytes && len(b.outstanding) < b.windowCount {
		b.held, b.announced = 0, false
	}
}

// closed reports whether the window is full.
func (b *budget) closed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bytes >= b.windowBytes || len(b.outstanding) >= b.windowCount
}

// hold records one withheld event and reports whether the closure still
// needs announcing.
func (b *budget) hold() (announce bool, heldNow int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.held++
	if b.announced {
		return false, b.held
	}
	b.announced = true
	return true, b.held
}

// spill writes body to a file and returns its path. Failure is reported
// rather than silently falling back to inlining the body, since inlining
// is the thing the caller was trying to avoid.
func (b *budget) spill(cursor, body string) (string, error) {
	b.mu.Lock()
	dirFn := b.spillDir
	b.mu.Unlock()
	if dirFn == nil {
		return "", fmt.Errorf("no spill directory configured")
	}
	dir, err := dirFn()
	if err != nil {
		return "", err
	}
	name := cursor
	if name == "" {
		name = "message"
	}
	name = filepath.Base(name) + ".txt"
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		return "", err
	}
	return path, nil
}

// shape returns the event as it should be delivered. A body within the
// spill threshold passes through untouched; a larger one is written to a
// file and replaced by a head, with the size, the path and the cursor.
//
// The cursor is what makes this safe rather than lossy, and it is not
// decoration: the spill file's lifetime is the connection's, while the
// reader's intention to open it is not, so the file can be gone before it
// is read. The content is still on the server, reachable by cursor, for as
// long as the server keeps it. Path for speed, cursor for truth.
//
// The reader is asked to delete the file once it has read it, because the
// reader is the only party that knows when that has happened. Nothing else
// can: clearAttachDir runs at teardown, which may be hours of spilled
// messages later, and a size-based sweep would have to guess whether a
// file still pending a read is abandoned. Deleting after reading costs the
// reader nothing — it has the bytes, and the cursor still names the
// durable copy if it turns out to need them again.
func (b *budget) shape(e Event) Event {
	b.mu.Lock()
	limit := b.spillBytes
	b.mu.Unlock()
	if e.Kind != "msg" || len(e.Text) <= limit {
		return e
	}
	size := len(e.Text)
	head := e.Text
	if len(head) > spillHeadBytes {
		head = head[:spillHeadBytes]
	}
	path, err := b.spill(e.Cursor, e.Text)
	if err != nil {
		e.Text = fmt.Sprintf("%s\n\n[hub: this message is %d bytes and was too large to deliver "+
			"inline. Writing it to a file failed (%v), so only the opening is shown. Retrieve the "+
			"whole message with hub_read(after: <the cursor BEFORE this one>)]", head, size, err)
		e.SpillBytes = size
		return e
	}
	e.SpillPath, e.SpillBytes = path, size
	e.Text = fmt.Sprintf("%s\n\n[hub: %d bytes total — too large to deliver inline, so only the "+
		"opening is above. The whole message is at %s; read that file if you need it, then DELETE "+
		"it — nothing else will, until the connection ends. If it is already gone, or you need it "+
		"again later, hub_read by cursor: the server is the durable copy, the file is only a "+
		"shortcut]", head, size, path)
	return e
}

// spillHeadBytes is how much of an oversized body is shown inline. Enough
// to judge whether the rest is worth opening, small enough that a burst of
// them cannot itself become the problem.
const spillHeadBytes = 400
