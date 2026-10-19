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
// connections, never answering again. And the receiving session queues at
// most about 50 messages for its model, shared with every other sender
// reaching it; past that, deliveries are dropped and the sender is not told.
//
// So delivery is governed by two rules:
//
//   - SPILL: no single delivery may be larger than spillBytes. Anything
//     bigger is written to a file and announced by head, size, path and
//     cursor. Nothing is lost; the reader decides whether to spend the
//     budget.
//   - WINDOW: one window for the whole process — every connection, every
//     kind of delivery (messages, rosters, this client's own notices,
//     catch-up deliveries, operator messages and mentions alike). At most
//     windowCount deliveries or windowBytes that the reader has not
//     confirmed. The last slot is kept for one notice saying more is
//     waiting. Only a confirm frees room: it is the one thing that proves
//     the reader took something in.
//   - QUEUE: a push that does not fit joins one queue, in arrival order
//     across every connection and kind, and while anything waits there
//     every new push joins behind it — so nothing overtakes anything. A
//     confirm drains it from the front as far as the window allows.
//
// Budget is exported so one process holds a SINGLE window across all its
// connections, and so the code that pushes this client's own notices can
// spend from it too.
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
	// because the evidence cannot distinguish whether volume or count
	// exhausts a receiver: 1.29 MB over four messages killed one, and a
	// queue of 50 is a count.
	windowBytes int
	windowCount int

	// outstanding is the ledger, in delivery order across every
	// connection. A confirm names a cursor, and everything delivered at or
	// before it is released, whichever connection it came from: the
	// receiving queue is first in, first out, so a reader who took in one
	// delivery has been handed everything before it.
	outstanding []charge
	count       int
	bytes       int
	// paused records that the window is full and its notice has gone out,
	// so the notice is said once until room is freed.
	paused bool
	// waiting is called whenever room is freed, so a connection whose
	// follower deliveries wait in its own buffer drains again.
	waiting map[any]func()

	// queue holds pushes that did not fit, in arrival order. sendMu is
	// held from the decision to send until the send is done, so pushes
	// leave in the order they were decided and a queued one is never
	// overtaken.
	sendMu  sync.Mutex
	queue   []Delivery
	dropped int
}

// Delivery is one push: who it is from, where a confirm locates it, what
// it costs, how to send it, and how to send the pause notice on the same
// way if it is the one that finds the window full.
type Delivery struct {
	Owner  string
	Cursor string
	Bytes  int
	Send   func()
	Pause  func(text string)
}

// maxQueued bounds the queue. Past it the oldest queued message that the
// server still holds is let go — a catch-up recovers it — and the reader
// is told how many; deliveries without a cursor are never let go.
const maxQueued = 5000

type charge struct {
	// owner and cursor locate the entry for a confirm. A cursor is opaque
	// and issued by one server, so two connections can hold the same
	// value; the owner tells them apart.
	owner  string
	cursor string
	bytes  int
	// counted marks a delivery that occupies the window. A cursor handed
	// over by a path the reader asked for — hub_catch_up, hub_read — costs
	// nothing and is in the ledger only so a confirm of it locates a
	// position.
	counted bool
}

// NewDeliveryBudget builds the window a process shares across its
// connections — see Conn.ShareDeliveryBudget.
func NewDeliveryBudget() *Budget { return newBudget() }

// pushWindowCount is how many deliveries this process keeps unconfirmed in
// the receiving session.
//
// The receiving session queues at most 50 accepted messages for its model
// to read, per Claude Code's own documentation, and also rate-limits per
// sender and drops identical repeats arriving close together. That queue
// is per SESSION and shared by every sender reaching it, so this process
// may not spend all of it. A busy session dropped 28 pushes on 2026-09-21
// when this was 200; nothing came back, since a successful push is written
// and unverified.
//
// 25 is half the documented cap. Not a tuning choice: the two errors are
// not symmetric, since too low pauses delivery and says so and a confirm
// reopens it, while too high loses messages silently.
const pushWindowCount = 25

func newBudget() *budget {
	return &budget{
		spillBytes:  128 * 1024,
		windowBytes: 500 * 1024,
		windowCount: pushWindowCount,
		waiting:     map[any]func(){},
	}
}

// ShareDeliveryBudget replaces this connection's own window with a shared
// one, so every connection spends against the same ceiling. Call it
// before SetDeliveryBudget, which configures whatever window is in place.
func (c *Conn) ShareDeliveryBudget(b *Budget, owner string) {
	if b == nil {
		return
	}
	c.budget = b
	// The owner is this connection's identity INSIDE the shared ledger,
	// so a confirm finds this connection's cursor and not another's with
	// the same value.
	c.budgetOwner = owner
	// Woken when a confirm frees room, so what waits in its buffer goes
	// out; a connection that has ended takes itself off instead.
	b.whenFreed(c, func() {
		if c.Connected() {
			c.signalActivity()
			return
		}
		b.Forget(c)
	})
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

// WhenFreed registers f to be called whenever a confirm frees room, under
// key; a later call with the same key replaces it. Exported for the code
// that pushes this client's own notices.
func (b *budget) WhenFreed(key any, f func()) { b.whenFreed(key, f) }

func (b *budget) whenFreed(key any, f func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.waiting == nil {
		b.waiting = map[any]func(){}
	}
	b.waiting[key] = f
}

// Forget stops calling what was registered under key.
func (b *budget) Forget(key any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.waiting, key)
}

// Take charges one delivery of n bytes if it fits, keeping the last slot
// for the pause notice, and reports whether it did. Nothing fits while the
// queue holds anything: taking now would overtake it.
func (b *budget) Take(owner, cursor string, n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.queue) > 0 {
		return false
	}
	return b.takeLocked(owner, cursor, n)
}

func (b *budget) takeLocked(owner, cursor string, n int) bool {
	if b.paused || b.count >= b.windowCount-1 || (b.count > 0 && b.bytes+n > b.windowBytes) {
		return false
	}
	b.outstanding = append(b.outstanding, charge{owner: owner, cursor: cursor, bytes: n, counted: true})
	b.count++
	b.bytes += n
	return true
}

// pauseLocked charges the pause notice to the last slot when it is due,
// and reports whether it is.
func (b *budget) pauseLocked(n int) bool {
	if b.paused {
		return false
	}
	b.paused = true
	b.outstanding = append(b.outstanding, charge{bytes: n, counted: true})
	b.count++
	b.bytes += n
	return true
}

// Deliver sends d now if it fits and nothing is queued, and otherwise
// queues it — saying once, through d's own Pause, that delivery is
// paused. It reports whether d was queued.
func (b *budget) Deliver(d Delivery) (queued bool) {
	b.sendMu.Lock()
	defer b.sendMu.Unlock()
	b.mu.Lock()
	if len(b.queue) == 0 && b.takeLocked(d.Owner, d.Cursor, d.Bytes) {
		b.mu.Unlock()
		d.Send()
		return false
	}
	b.queue = append(b.queue, d)
	if len(b.queue) > maxQueued {
		for i, w := range b.queue {
			if w.Cursor != "" {
				b.queue = append(b.queue[:i], b.queue[i+1:]...)
				b.dropped++
				break
			}
		}
	}
	paused := DeliveryPausedText()
	announce := d.Pause != nil && b.pauseLocked(len(paused))
	b.mu.Unlock()
	if announce {
		d.Pause(paused)
	}
	return true
}

// Queued reports how many pushes wait.
func (b *budget) Queued() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue)
}

// TakeDropped reports how many queued messages were let go since the last
// call, and resets the count.
func (b *budget) TakeDropped() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.dropped
	b.dropped = 0
	return n
}

// flush sends from the front of the queue as far as the window allows.
func (b *budget) flush() {
	b.sendMu.Lock()
	defer b.sendMu.Unlock()
	for {
		b.mu.Lock()
		if len(b.queue) == 0 {
			b.mu.Unlock()
			return
		}
		d := b.queue[0]
		if !b.takeLocked(d.Owner, d.Cursor, d.Bytes) {
			paused := DeliveryPausedText()
			announce := d.Pause != nil && b.pauseLocked(len(paused))
			b.mu.Unlock()
			if announce {
				d.Pause(paused)
			}
			return
		}
		b.queue = b.queue[1:]
		b.mu.Unlock()
		d.Send()
	}
}

// Pause reports whether the pause notice is due — the window is full and
// has not said so yet — and if so charges it to the last slot.
func (b *budget) Pause(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pauseLocked(n)
}

// Free reports how many more deliveries, and bytes, fit before the window
// pauses.
func (b *budget) Free() (count, bytes int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.paused || len(b.queue) > 0 {
		return 0, 0
	}
	return max(b.windowCount-1-b.count, 0), max(b.windowBytes-b.bytes, 0)
}

// note records a cursor handed to the model by a path that spends no
// window — hub_catch_up, hub_read, a synchronous tool result — so a later
// confirm of it locates a position.
func (b *budget) note(owner, cursor string) {
	if cursor == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.outstanding {
		if ch.owner == owner && ch.cursor == cursor {
			return
		}
	}
	b.outstanding = append(b.outstanding, charge{owner: owner, cursor: cursor})
}

// release frees everything delivered at or before owner's cursor, from
// every connection. It reports whether the cursor was in the ledger; an
// unknown one frees nothing, since it locates no position.
func (b *budget) release(owner, cursor string) bool {
	if cursor == "" {
		return false
	}
	b.mu.Lock()
	cut := -1
	for i, ch := range b.outstanding {
		if ch.owner == owner && ch.cursor == cursor {
			cut = i
			break
		}
	}
	if cut < 0 {
		b.mu.Unlock()
		return false
	}
	for _, ch := range b.outstanding[:cut+1] {
		if ch.counted {
			b.count--
			b.bytes -= ch.bytes
		}
	}
	b.outstanding = append([]charge(nil), b.outstanding[cut+1:]...)
	b.afterFreeLocked()
	return true
}

// ReleaseAll frees the whole window: the reader says it has taken in
// everything delivered so far. Positions are not touched — no cursor was
// named — so the ledger keeps the entries a later confirm may locate.
func (b *budget) ReleaseAll() {
	b.mu.Lock()
	kept := b.outstanding[:0]
	for _, ch := range b.outstanding {
		if ch.cursor != "" {
			ch.counted, ch.bytes = false, 0
			kept = append(kept, ch)
		}
	}
	b.outstanding = kept
	b.count, b.bytes = 0, 0
	b.afterFreeLocked()
}

// afterFreeLocked ends the pause and wakes whatever waits. Called with
// b.mu held; releases it.
func (b *budget) afterFreeLocked() {
	if b.count < 0 {
		b.count = 0
	}
	if b.bytes < 0 {
		b.bytes = 0
	}
	b.paused = false
	wake := make([]func(), 0, len(b.waiting))
	for _, f := range b.waiting {
		wake = append(wake, f)
	}
	b.mu.Unlock()
	b.flush()
	for _, f := range wake {
		f()
	}
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
