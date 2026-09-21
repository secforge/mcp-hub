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
// Budget is exported so one process can hold a SINGLE window across
// several connections. The window bounds what a reader can absorb, and a
// reader is a process, not a socket: eight connections with a window each
// is eight times the ceiling that was set by measuring one receiver
// dying. The spill threshold stays per-message and needs no sharing.
type Budget = budget

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
	// owner names the connection this charge belongs to. It exists
	// because one window is now shared by several connections, and a
	// confirm is a statement about ONE of them: releasing a prefix of the
	// whole ledger would credit back budget for messages another
	// connection's reader has not confirmed and which are still occupying
	// the context this window exists to protect. Empty for a client that
	// never shared its window, where the ledger has one owner by
	// construction.
	owner  string
	cursor string
	bytes  int
	// held marks an entry the window refused to deliver. It occupies no
	// budget — nothing was sent — but it holds a POSITION, so a confirm
	// that releases a prefix containing one can report that the reader has
	// just moved past something they were never shown.
	held bool
}

// newBudget returns the default policy. Values are deliberately far apart:
// the spill threshold guards one message, the window guards a session, and
// a session is many messages.
// NewDeliveryBudget builds a window for a caller that intends to share
// one across several connections — see Conn.ShareDeliveryBudget.
func NewDeliveryBudget() *Budget { return newBudget() }

// pushWindowCount is how many unconfirmed messages this client will push
// into a session before holding the rest.
//
// IT BOUNDS THE RECEIVER'S INBOX, NOT THE READER'S CONTEXT — which is
// what the byte window does, and why the two numbers are far apart.
//
// The receiving session queues at most 50 accepted messages for its
// model to read, per Claude Code's own documentation, and also
// rate-limits per sender and drops identical repeats arriving close
// together. That queue is per SESSION and shared by every sender
// reaching it, so a single connection may not spend all of it.
//
// It was 200, chosen against the reader's context, and a busy session
// dropped 28 pushes on 2026-09-21 long before reaching it — this client
// pushing steadily into a queue that was discarding. Nothing came back:
// a successful Deliver returns ObservedNothing, written and unverified,
// so the drop is invisible to the sender. The RECEIVING session is told
// which sender dropped it; the sender is not.
//
// 25 is half the documented cap, which leaves the other half for
// everything else reaching that session. Not a tuning choice: the two
// errors are not symmetric, since too low pauses delivery and says so
// and a confirm reopens it, while too high loses messages silently.
const pushWindowCount = 25

func newBudget() *budget {
	return &budget{
		spillBytes:  128 * 1024,
		windowBytes: 500 * 1024,
		windowCount: pushWindowCount,
	}
}

// ShareDeliveryBudget replaces this connection's own window with a shared
// one, so several connections spend against the same ceiling. Call it
// before SetDeliveryBudget, which configures whatever window is in place.
//
// Deliberately a replacement rather than a second layer: two windows
// would each be satisfied while their sum was not, which is the exact
// arithmetic that made a per-connection window wrong.
func (c *Conn) ShareDeliveryBudget(b *Budget, owner string) {
	if b == nil {
		return
	}
	c.budget = b
	// The owner is this connection's identity INSIDE the shared ledger,
	// so a confirm releases what this connection delivered and nothing
	// else. Unshared windows leave it empty, which is correct: a ledger
	// with one owner needs no filter.
	c.budgetOwner = owner
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
func (b *budget) charge(owner, cursor string, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.outstanding = append(b.outstanding, charge{owner: owner, cursor: cursor, bytes: n})
	b.bytes += n
}

// release drops every charge at or before cursor and subtracts exactly
// those bytes. It reports whether the released prefix contained a HELD
// entry — a message the window refused to deliver — because confirming
// past one moves the read position over content the reader never saw, and
// silence is exactly what that must not produce.
func (b *budget) release(owner, cursor string) (skipped bool) {
	if cursor == "" {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// Release a PREFIX located by position, WITHIN ONE OWNER. The ledger
	// is in delivery order — this client's own arrival sequence — so
	// "everything up to and including that cursor" is an index, and no
	// comparison of one opaque cursor against another is needed. That
	// matters: a cursor's format and precision belong to the server,
	// today's happen to be sortable ticks, and a client that ordered them
	// would be depending on an accident it was told not to rely on.
	//
	// The owner filter is what makes one window safe to share. A confirm
	// says "I have read up to here" about ONE conversation; the entries
	// another connection interleaved before it are still unread and still
	// occupying the reader. Releasing them would credit back budget for
	// messages nobody confirmed — a ceiling that rises exactly when
	// several conversations are busy, which is when it is load-bearing.
	//
	// For the index to exist, every cursor handed to the model must be in
	// the ledger — pulled ones at zero cost (see note) and held ones at
	// zero cost (see hold), not only pushed ones. Without that, a reader
	// that followed the notice's own advice and called hub_catch_up
	// confirmed a cursor this ledger had never heard of, released nothing,
	// and left the window shut forever while hub_confirm reported success.
	cut := -1
	for i, ch := range b.outstanding {
		if ch.owner == owner && ch.cursor == cursor {
			cut = i
			break
		}
	}
	if cut < 0 {
		// An unknown cursor cannot locate a prefix, so nothing is
		// released — under-releasing costs headroom, over-releasing costs
		// the exhaustion this whole file exists to prevent. But it is
		// evidence the reader is reading, so the closure becomes
		// announceable again rather than staying silent forever.
		b.announced = false
		return
	}
	kept := make([]charge, 0, len(b.outstanding))
	for i, ch := range b.outstanding {
		if i > cut || ch.owner != owner {
			kept = append(kept, ch)
			continue
		}
		// At or before the cut AND this owner's: released.
		if ch.held {
			skipped = true
		}
		b.bytes -= ch.bytes
	}
	if b.bytes < 0 {
		b.bytes = 0
	}
	b.outstanding = kept
	if b.bytes < b.windowBytes && len(b.outstanding) < b.windowCount {
		b.held, b.announced = 0, false
	}
	return skipped
}

// adjust corrects an entry's cost once the caller knows the true size.
// The estimate charged at render time is what enforces the window DURING
// a burst — without it a single large batch would pass entirely before
// anything was counted — and this replaces it with what was actually
// delivered, rather than adding a second entry for the same message.
// Matched on OWNER AND CURSOR, like release and hold: a cursor is opaque
// and issued by one server, so two connections sharing this ledger can
// legitimately hold the same value. Matching on the cursor alone moved
// the other connection's charge.
func (b *budget) adjust(owner, cursor string, n int) {
	if cursor == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, ch := range b.outstanding {
		if ch.owner == owner && ch.cursor == cursor {
			b.bytes += n - ch.bytes
			b.outstanding[i].bytes = n
			if b.bytes < 0 {
				b.bytes = 0
			}
			return
		}
	}
	b.outstanding = append(b.outstanding, charge{owner: owner, cursor: cursor, bytes: n})
	b.bytes += n
}

// note records a cursor handed to the model by a path that spends no push
// budget — hub_catch_up, hub_read, a synchronous tool result. It costs
// nothing and exists purely so the cursor has a POSITION in the ledger,
// which is what lets a later confirm of it locate a prefix.
//
// Without this, the two halves of the design contradict each other: the
// held-window notice tells the reader to pull and confirm, and confirming
// what was pulled released nothing at all.
func (b *budget) note(owner, cursor string) {
	if cursor == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.outstanding {
		// Owner-scoped for the same reason release is: the same cursor
		// from a different connection is a different position, and
		// treating it as already noted left this connection's confirm
		// with no entry to locate a prefix from.
		if ch.owner == owner && ch.cursor == cursor {
			return
		}
	}
	b.outstanding = append(b.outstanding, charge{owner: owner, cursor: cursor})
}

// closed reports whether the window is full.
func (b *budget) closed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bytes >= b.windowBytes || len(b.outstanding) >= b.windowCount
}

// hold records one withheld event and reports whether the closure still
// needs announcing.
func (b *budget) hold(owner, cursor string) (announce bool, heldNow int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cursor != "" {
		b.outstanding = append(b.outstanding, charge{owner: owner, cursor: cursor, held: true})
	}
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
	// UNIQUE, not derived from the cursor alone. Every connection in this
	// process shares one spill directory, and a cursor is opaque and
	// scoped to the server that issued it — so two connections can hold
	// the same cursor value, two different cursors can share a basename,
	// and every cursorless message wanted the same name. Each of those
	// overwrote a file whose path had already been handed to a reader,
	// which then resolved to somebody else's message, possibly from
	// another conversation. The cursor stays in the name because it is
	// what makes the file identifiable; the suffix is what makes it one
	// file.
	f, err := os.CreateTemp(dir, filepath.Base(name)+"-*.txt")
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := f.Chmod(0o600); err != nil {
		return "", err
	}
	if _, err := f.WriteString(body); err != nil {
		return "", err
	}
	return f.Name(), nil
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

// SameDeliveryBudget reports whether two connections spend against the
// same window — for a caller that shares one and wants to prove it did,
// rather than assume it from having called the setter.
func (c *Conn) SameDeliveryBudget(other *Conn) bool {
	return c != nil && other != nil && c.budget == other.budget
}
