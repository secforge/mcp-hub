package hubconn

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/wire"
)

func testBudget(t *testing.T) *budget {
	t.Helper()
	dir := t.TempDir()
	b := newBudget()
	b.spillDir = func() (string, error) { return dir, nil }
	return b
}

func msg(cursor string, size int) Event {
	return Event{Kind: "msg", Cursor: cursor, Text: strings.Repeat("x", size)}
}

func TestTheWindowTakesTwentyFourAndKeepsTheLastSlotForThePause(t *testing.T) {
	b := testBudget(t)
	for i := 0; i < pushWindowCount-1; i++ {
		if !b.Take("a", fmt.Sprintf("c%d", i), 10) {
			t.Fatalf("delivery %d refused", i)
		}
	}
	if b.Take("a", "one-too-many", 10) {
		t.Fatal("the slot kept for the pause notice was given to a delivery")
	}
	if !b.Pause(10) {
		t.Fatal("the pause notice was refused its slot")
	}
	if b.Pause(10) {
		t.Fatal("the pause notice was due twice")
	}
	if n, _ := b.Free(); n != 0 {
		t.Fatalf("a paused window reports %d free", n)
	}
}

func TestTheWindowFillsOnBytesTooButNeverRefusesAFirstDelivery(t *testing.T) {
	b := testBudget(t)
	b.windowBytes = 1000
	if !b.Take("a", "c1", 5000) {
		t.Fatal("a first delivery larger than the window was refused, so nothing could ever go out")
	}
	if b.Take("a", "c2", 1) {
		t.Fatal("the window took more after its bytes were spent")
	}
}

// The receiving queue is first in, first out: a reader who confirms one
// delivery has been handed everything before it, whichever connection it
// came from.
func TestAConfirmFreesEverythingDeliveredUpToItFromEveryConnection(t *testing.T) {
	b := testBudget(t)
	b.Take("alpha", "a1", 100)
	b.Take("beta", "b1", 100)
	b.Take("", "", 100) // a notice
	b.Take("alpha", "a2", 100)
	b.Take("beta", "b2", 100)
	if !b.release("alpha", "a2") {
		t.Fatal("the confirmed cursor was not found")
	}
	if b.count != 1 || b.bytes != 100 || len(b.outstanding) != 1 || b.outstanding[0].cursor != "b2" {
		t.Fatalf("after confirming a2: count %d bytes %d ledger %+v", b.count, b.bytes, b.outstanding)
	}
}

func TestAForeignOrUnknownCursorFreesNothing(t *testing.T) {
	b := testBudget(t)
	b.Take("alpha", "a1", 100)
	b.Take("beta", "b1", 100)
	if b.release("beta", "a1") || b.release("alpha", "never-delivered") {
		t.Fatal("a cursor that locates nothing reported a release")
	}
	if b.count != 2 || b.bytes != 200 {
		t.Fatalf("something was freed: count %d bytes %d", b.count, b.bytes)
	}
}

func TestReleasingEverythingFreesTheWindowAndKeepsThePositions(t *testing.T) {
	b := testBudget(t)
	for i := 0; i < pushWindowCount-1; i++ {
		b.Take("a", fmt.Sprintf("c%d", i), 10)
	}
	b.Pause(10)
	woken := 0
	b.WhenFreed("watcher", func() { woken++ })
	b.ReleaseAll()
	if n, _ := b.Free(); n != pushWindowCount-1 {
		t.Fatalf("free %d after releasing everything", n)
	}
	if woken != 1 {
		t.Fatalf("waiters woken %d times, want 1", woken)
	}
	// A later confirm of an earlier cursor still finds its position.
	if !b.release("a", "c3") {
		t.Fatal("releasing everything forgot the positions a confirm locates")
	}
}

// A cursor the reader pulled for itself costs nothing, but a confirm of it
// must still locate a position and free what was delivered before it.
func TestConfirmingAPulledCursorFreesWhatCameBefore(t *testing.T) {
	b := testBudget(t)
	b.Take("a", "pushed", 100)
	b.note("a", "pulled")
	if !b.release("a", "pulled") || b.count != 0 {
		t.Fatalf("confirming a pulled cursor left count %d", b.count)
	}
}

// What does not fit stays in the buffer, in order, and goes out after a
// confirm; the pause is said once. Every kind counts, an operator message
// included.
func TestDeliveriesThatDoNotFitWaitInTheBufferInOrder(t *testing.T) {
	c := &Conn{budget: newBudget()}
	for i := 0; i < 30; i++ {
		e := msg(fmt.Sprintf("c%02d", i), 10)
		if i == 3 {
			e.IsOperator = true
		}
		c.buffer = append(c.buffer, e)
	}
	first, _ := c.drainWithinWindow(deliveredCost, func(Event) bool { return false })
	if len(first) != pushWindowCount || first[len(first)-1].Kind != "deliveryPaused" {
		t.Fatalf("first drain gave %d events ending in %q", len(first), first[len(first)-1].Kind)
	}
	if again, _ := c.drainWithinWindow(deliveredCost, func(Event) bool { return false }); len(again) != 0 {
		t.Fatalf("a full window delivered %d more, the pause notice included", len(again))
	}
	c.budget.release("", first[pushWindowCount-2].Cursor)
	rest, _ := c.drainWithinWindow(deliveredCost, func(Event) bool { return false })
	if len(rest) != 6 {
		t.Fatalf("after the confirm %d delivered, want the 6 that waited", len(rest))
	}
	for i, e := range rest {
		if want := fmt.Sprintf("c%02d", 24+i); e.Cursor != want {
			t.Fatalf("out of order: got %s, want %s", e.Cursor, want)
		}
	}
}

func TestEventsTheCallerWillNotDeliverTakeNoRoom(t *testing.T) {
	c := &Conn{budget: newBudget()}
	c.buffer = []Event{msg("c1", 10), {Kind: "msg", Cursor: "h1", Historical: true}}
	c.drainWithinWindow(deliveredCost, func(e Event) bool { return e.Historical })
	if c.budget.count != 1 {
		t.Fatalf("count %d, want only the delivered message charged", c.budget.count)
	}
}

func TestABurstOfShortMessagesFillsTheWindowOnFramingAlone(t *testing.T) {
	c := &Conn{budget: newBudget()}
	c.budget.windowBytes = 10 * 1024
	c.budget.windowCount = 10000
	for i := 0; i < 60; i++ {
		c.buffer = append(c.buffer, Event{Kind: "msg", Cursor: fmt.Sprintf("c%d", i), PeerID: "peer-a",
			TS: "2026-09-15T10:00:00Z", Text: "ack"})
	}
	out, _ := c.drainWithinWindow(deliveredCost, func(Event) bool { return false })
	if len(out) >= 60 {
		t.Fatal("delivered all 60 one-liners without filling a 10 KB window")
	}
}

func TestAnOversizedBodyIsSpilledToAFileAndAnnouncedByHeadSizeAndPath(t *testing.T) {
	b := testBudget(t)
	b.spillBytes = 1000
	body := strings.Repeat("abcdefghij", 500) // 5000 bytes

	got := b.shape(Event{Kind: "msg", Cursor: "c1", Text: body})

	if got.SpillPath == "" {
		t.Fatal("no spill path recorded")
	}
	if got.SpillBytes != len(body) {
		t.Fatalf("SpillBytes = %d, want %d", got.SpillBytes, len(body))
	}
	onDisk, err := os.ReadFile(got.SpillPath)
	if err != nil {
		t.Fatalf("reading the spill file: %v", err)
	}
	if string(onDisk) != body {
		t.Fatal("the spilled file is not the message")
	}
	if !strings.HasPrefix(got.Text, body[:spillHeadBytes]) {
		t.Fatal("the delivered text does not start with the head of the message")
	}
	if len(got.Text) >= len(body) {
		t.Fatalf("delivered %d bytes for a %d-byte body — nothing was saved",
			len(got.Text), len(body))
	}
	// The file dies with the connection; the cursor does not. A reader
	// that finds the file gone must still be told where the truth lives.
	if !strings.Contains(got.Text, "hub_read") {
		t.Fatalf("the spill notice never names the durable route: %q", got.Text)
	}
	// Only the reader knows when the file has been read, so only the
	// reader can clean it up at the right moment — it has to be asked.
	if !strings.Contains(got.Text, "DELETE") {
		t.Fatalf("the spill notice never asks the reader to delete the file: %q", got.Text)
	}
}

// Failing to write the file must not fall back to inlining the body —
// inlining is precisely what the caller was avoiding.
func TestAFailedSpillTruncatesRatherThanInlining(t *testing.T) {
	b := newBudget()
	b.spillBytes = 1000
	body := strings.Repeat("y", 5000)

	got := b.shape(Event{Kind: "msg", Cursor: "c1", Text: body})

	if len(got.Text) >= len(body) {
		t.Fatalf("delivered %d bytes for a %d-byte body after a failed spill", len(got.Text), len(body))
	}
	if got.SpillBytes != len(body) {
		t.Fatalf("SpillBytes = %d, want the true size %d", got.SpillBytes, len(body))
	}
}

func TestABodyWithinTheThresholdIsUntouched(t *testing.T) {
	b := testBudget(t)
	b.spillBytes = 1000
	e := Event{Kind: "msg", Cursor: "c1", Text: strings.Repeat("z", 1000)}
	if got := b.shape(e); got.Text != e.Text || got.SpillPath != "" {
		t.Fatal("a message at exactly the threshold was spilled")
	}
}

// The charge must cover what is actually written, or the window stops
// being a bound at all. Measured against the real output of the path that
// does the writing — FormatEventsBatch plus the wait socket's own "\n\n"
// separator — rather than against a second copy of the estimate.
func TestTheChargeIsAnUpperBoundOnWhatIsActuallyDelivered(t *testing.T) {
	events := []Event{
		{Kind: "msg", Cursor: "c1", PeerID: "peer-a", TS: "2026-09-15T10:00:00Z", Text: "ok"},
		{Kind: "msg", Cursor: "c2", PeerID: "peer-b", TS: "2026-09-15T10:00:01Z",
			Text: strings.Repeat("longer body ", 100), ExternalID: "x-2", MentionedMe: true,
			Mentions: []wire.Mention{{ID: "m1", Name: "someone"}}},
		{Kind: "msg", Cursor: "c3", PeerID: "peer-c", TS: "2026-09-15T10:00:02Z",
			Text: "third", IsOperator: true},
	}

	charged := 0
	for _, e := range events {
		charged += deliveredCost(e)
	}
	written := 0
	for _, chunk := range FormatEventsBatch(events) {
		written += len(chunk) + len("\n\n")
	}

	if charged < written {
		t.Fatalf("charged %d bytes for %d actually written — the window undercounts", charged, written)
	}
}

// Overwriting a pending claim produced two wrong outcomes at once: the
// displaced caller waited out its timeout having never been told, and the
// surviving one received the FIRST ack of that kind — possibly the answer
// to the displaced caller's request, carrying an outcome a model is shown
// as delivered or refused. One timeout and one confidently wrong answer is
// worse than two timeouts.
func TestASecondClaimOfTheSameKindIsRefusedRatherThanReplacingTheFirst(t *testing.T) {
	c := &Conn{}
	first, cancelFirst, err := c.claimNextAck("sendAck")
	if err != nil {
		t.Fatalf("first claim failed: %v", err)
	}
	defer cancelFirst()

	_, _, err = c.claimNextAck("sendAck")
	if err == nil {
		t.Fatal("a second claim of the same kind was accepted, displacing the first")
	}

	// The first claim must still be the one that gets the answer.
	c.mu.Lock()
	claim := c.pendingAcks["sendAck"]
	c.mu.Unlock()
	if claim == nil {
		t.Fatal("the refused claim removed the one already waiting")
	}
	claim.result <- Event{Kind: "sendAck", ActionOK: true, ActionOKStated: true}
	select {
	case ev := <-first:
		if !ev.ActionOK {
			t.Fatal("the original claimant got the wrong answer")
		}
	default:
		t.Fatal("the original claimant was not delivered to")
	}

	// A different kind is unaffected — the refusal is per kind, not global.
	if _, cancelOther, err := c.claimNextAck("editAck"); err != nil {
		t.Fatalf("an unrelated ack kind was refused: %v", err)
	} else {
		cancelOther()
	}
}

// TestTwoConnectionsWithTheSameCursorKeepTheirOwnCharges is Codex's
// finding, 2026-09-19: cursors are opaque and issued per server, so two
// connections sharing one ledger can legitimately hold the same value.
// note matched on the cursor alone, so one connection's entry answered
// for the other's and a confirm released nothing.
func TestTwoConnectionsWithTheSameCursorKeepTheirOwnCharges(t *testing.T) {
	b := testBudget(t)
	b.Take("alpha", "same-cursor", 100)
	b.Take("beta", "same-cursor", 100)

	// A note for beta must land, rather than being swallowed because
	// alpha already holds that cursor.
	b.note("beta", "noted-cursor")
	b.note("alpha", "noted-cursor")
	owners := map[string]bool{}
	for _, ch := range b.outstanding {
		if ch.cursor == "noted-cursor" {
			owners[ch.owner] = true
		}
	}
	if !owners["alpha"] || !owners["beta"] {
		t.Fatalf("expected both connections to have their own entry for the same cursor, got %v", owners)
	}
}

type sentLog struct {
	mu   sync.Mutex
	sent []string
}

func (l *sentLog) add(s string) {
	l.mu.Lock()
	l.sent = append(l.sent, s)
	l.mu.Unlock()
}

func (l *sentLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.sent...)
}

func delivery(log *sentLog, owner, cursor string) Delivery {
	return Delivery{Owner: owner, Cursor: cursor, Bytes: 10,
		Send:  func() { log.add(owner + ":" + cursor) },
		Pause: func(string) { log.add("PAUSED") }}
}

// One queue, in arrival order across every connection: nothing overtakes
// anything, and a confirm drains it from the front.
func TestPushesThatDoNotFitQueueInArrivalOrderAcrossConnections(t *testing.T) {
	b := testBudget(t)
	log := &sentLog{}
	for i := 0; i < 30; i++ {
		owner := []string{"busy", "quiet"}[min(i/20, 1)]
		b.Deliver(delivery(log, owner, fmt.Sprintf("c%02d", i)))
	}
	if got := log.all(); len(got) != pushWindowCount || got[len(got)-1] != "PAUSED" {
		t.Fatalf("before any confirm: %v", got)
	}
	if b.Queued() != 6 {
		t.Fatalf("%d queued, want 6", b.Queued())
	}
	b.release("busy", "c19")
	got := log.all()[pushWindowCount:]
	if len(got) != 6 {
		t.Fatalf("after the confirm %d sent, want the 6 that waited: %v", len(got), got)
	}
	for i, s := range got {
		owner := "busy"
		if 24+i >= 20 {
			owner = "quiet"
		}
		if s != fmt.Sprintf("%s:c%02d", owner, 24+i) {
			t.Fatalf("out of order at %d: %v", i, got)
		}
	}
}

func TestNothingTakesRoomPastAWaitingQueue(t *testing.T) {
	b := testBudget(t)
	log := &sentLog{}
	for i := 0; i < pushWindowCount; i++ {
		b.Deliver(delivery(log, "a", fmt.Sprintf("c%d", i)))
	}
	b.ReleaseAll()
	b.Deliver(delivery(log, "a", "x"))
	if b.Queued() != 0 {
		t.Fatal("the queue did not drain after the release")
	}
	for i := 0; i < pushWindowCount; i++ {
		b.Deliver(delivery(log, "a", fmt.Sprintf("d%d", i)))
	}
	if b.Take("catchup", "z", 1) {
		t.Fatal("a direct take overtook the waiting queue")
	}
	if n, _ := b.Free(); n != 0 {
		t.Fatalf("free %d with a queue waiting", n)
	}
}

func TestPastTheQueueLimitOnlyRefetchableMessagesAreLetGo(t *testing.T) {
	b := testBudget(t)
	log := &sentLog{}
	for i := 0; i < pushWindowCount-1; i++ {
		b.Deliver(delivery(log, "a", fmt.Sprintf("c%d", i)))
	}
	b.Deliver(delivery(log, "", "")) // a notice: queued, never let go
	for i := 0; i < maxQueued+10; i++ {
		b.Deliver(delivery(log, "a", fmt.Sprintf("q%d", i)))
	}
	if n := b.TakeDropped(); n != 11 {
		t.Fatalf("dropped %d, want 11", n)
	}
	b.mu.Lock()
	first := b.queue[0]
	b.mu.Unlock()
	if first.Cursor != "" {
		t.Fatal("the notice without a cursor was let go")
	}
}

func TestThePauseIsSaidAgainWhilePushesWait(t *testing.T) {
	b := newBudget()
	b.pauseRepeat = 30 * time.Millisecond
	var mu sync.Mutex
	var said []string
	pause := func(text string) {
		mu.Lock()
		said = append(said, text)
		mu.Unlock()
	}
	for i := 0; i < 26; i++ {
		b.Deliver(Delivery{Bytes: 10, Send: func() {}, Pause: pause})
	}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(said)
	}
	deadline := time.Now().Add(2 * time.Second)
	for count() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	if len(said) < 3 || !strings.Contains(said[1], "STILL PAUSED") || !strings.Contains(said[1], "2 push(es)") {
		mu.Unlock()
		t.Fatalf("the pause was not repeated: %q", said)
	}
	mu.Unlock()
	b.ReleaseAll()
	n := count()
	time.Sleep(100 * time.Millisecond)
	if count() != n {
		t.Fatal("the pause was still repeated after a confirm freed the window")
	}
}
