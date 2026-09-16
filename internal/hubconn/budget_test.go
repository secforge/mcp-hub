package hubconn

import (
	"fmt"
	"os"
	"strings"
	"testing"

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

// A confirm names one cursor, and only what came at or before it has been
// read. Crediting back the whole ledger would reopen the window while the
// messages after that cursor are still sitting unread in the receiver's
// context — the exact overspend the window exists to prevent.
func TestConfirmReleasesOnlyThePrefixUpToTheConfirmedCursor(t *testing.T) {
	b := testBudget(t)
	b.charge("", "c1", 100)
	b.charge("", "c2", 200)
	b.charge("", "c3", 400)

	b.release("", "c2")

	if b.bytes != 400 {
		t.Fatalf("bytes after confirming c2 = %d, want 400 (c3 still unread)", b.bytes)
	}
	if len(b.outstanding) != 1 || b.outstanding[0].cursor != "c3" {
		t.Fatalf("outstanding = %+v, want just c3", b.outstanding)
	}
}

// A cursor that was never charged must not silently release anything. It
// means the reader confirmed something it pulled itself, which spends no
// push budget and therefore frees none.
func TestConfirmingAnUnchargedCursorReleasesNothing(t *testing.T) {
	b := testBudget(t)
	b.charge("", "c1", 100)

	b.release("", "pulled-not-pushed")

	if b.bytes != 100 || len(b.outstanding) != 1 {
		t.Fatalf("bytes=%d outstanding=%d, want the ledger untouched", b.bytes, len(b.outstanding))
	}
}

func TestTheWindowClosesOnBytesAndReopensOnConfirm(t *testing.T) {
	b := testBudget(t)
	b.windowBytes = 1000
	b.charge("", "c1", 600)
	if b.closed() {
		t.Fatal("window closed at 600 of 1000")
	}
	b.charge("", "c2", 500)
	if !b.closed() {
		t.Fatal("window still open at 1100 of 1000")
	}
	b.release("", "c1")
	if b.closed() {
		t.Fatal("window still closed at 500 of 1000 after confirming c1")
	}
}

func TestTheWindowClosesOnCountEvenWhenTheBytesAreTiny(t *testing.T) {
	b := testBudget(t)
	b.windowCount = 3
	for _, c := range []string{"c1", "c2", "c3"} {
		b.charge("", c, 1)
	}
	if !b.closed() {
		t.Fatalf("window open at 3 messages of 3 (bytes=%d)", b.bytes)
	}
}

// The closure is announced once, not once per withheld message: repeating
// it would spend the very attention the gate was closed to protect. But
// the count must keep climbing, so the one notice can say how much is
// waiting rather than merely that something is.
func TestAClosedWindowIsAnnouncedOnceAndKeepsCounting(t *testing.T) {
	b := testBudget(t)
	announce, held := b.hold("", "held-cursor")
	if !announce || held != 1 {
		t.Fatalf("first hold = (%v, %d), want (true, 1)", announce, held)
	}
	for i := 0; i < 4; i++ {
		if announce, _ = b.hold("", fmt.Sprintf("held-%d", i)); announce {
			t.Fatal("the closure was announced a second time")
		}
	}
	if _, held = b.hold("", "held-last"); held != 6 {
		t.Fatalf("held = %d, want 6", held)
	}
}

func TestReopeningTheWindowRearmsTheAnnouncement(t *testing.T) {
	b := testBudget(t)
	b.windowBytes = 100
	b.charge("", "c1", 200)
	b.hold("", "held-cursor")
	b.release("", "c1")
	announce, held := b.hold("", "held-cursor")
	if !announce || held != 1 {
		t.Fatalf("hold after reopening = (%v, %d), want (true, 1) — a new closure is news again",
			announce, held)
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

// applyBudget is where the policy meets the live push path. A closed
// window must stop ordinary bodies and say so exactly once.
func TestApplyBudgetHoldsOrdinaryMessagesOnceTheWindowIsClosed(t *testing.T) {
	c := &Conn{budget: newBudget()}
	c.budget.windowBytes = 1000

	out := c.applyBudget([]Event{msg("c1", 1200), msg("c2", 10), msg("c3", 10)}, deliveredCost)

	if len(out) != 2 {
		t.Fatalf("delivered %d events, want the first message plus one notice: %+v", len(out), out)
	}
	if out[0].Cursor != "c1" {
		t.Fatalf("first delivered event = %+v, want the message that fit", out[0])
	}
	if out[1].Kind != "deliveryHeld" || out[1].HeldCount != 1 {
		t.Fatalf("second event = %+v, want a deliveryHeld notice counting 1", out[1])
	}
	if rendered := FormatEvent(out[1]); rendered == "" {
		t.Fatal("the held notice renders as nothing — a closed window would be silent")
	}
}

// A mention or an operator message is the one thing a paused reader must
// still see: the pause exists to protect attention, not to hide the
// messages that are asking for it.
func TestMentionsAndOperatorMessagesCrossAClosedWindow(t *testing.T) {
	c := &Conn{budget: newBudget()}
	c.budget.windowBytes = 100
	c.budget.charge("", "c0", 500)

	mention := msg("c1", 10)
	mention.MentionedMe = true
	operator := msg("c2", 10)
	operator.IsOperator = true

	out := c.applyBudget([]Event{msg("c3", 10), mention, operator}, deliveredCost)

	var cursors []string
	for _, e := range out {
		if e.Kind == "msg" {
			cursors = append(cursors, e.Cursor)
		}
	}
	if len(cursors) != 2 || cursors[0] != "c1" || cursors[1] != "c2" {
		t.Fatalf("delivered messages %v, want the mention and the operator message only", cursors)
	}
}

func TestConfirmingThroughTheConnReopensThePushWindow(t *testing.T) {
	c := &Conn{budget: newBudget()}
	c.budget.windowBytes = 1000
	c.applyBudget([]Event{msg("c1", 1200)}, deliveredCost)
	if !c.budget.closed() {
		t.Fatal("the window did not close")
	}
	c.budget.release("", "c1")
	out := c.applyBudget([]Event{msg("c2", 10)}, deliveredCost)
	if len(out) != 1 || out[0].Kind != "msg" {
		t.Fatalf("after confirming, delivery gave %+v, want the message through", out)
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

// A burst of short messages is the case the body-only charge missed
// entirely: two hundred one-liners are a few KB of text and most of a
// megabyte of envelope.
func TestABurstOfShortMessagesClosesTheWindowOnFramingAlone(t *testing.T) {
	c := &Conn{budget: newBudget()}
	c.budget.windowBytes = 10 * 1024
	c.budget.windowCount = 10000 // take the count limit out of the picture

	body := 0
	events := make([]Event, 0, 60)
	for i := 0; i < 60; i++ {
		e := Event{Kind: "msg", Cursor: fmt.Sprintf("c%d", i), PeerID: "peer-a",
			TS: "2026-09-15T10:00:00Z", Text: "ack"}
		body += len(e.Text)
		events = append(events, e)
	}

	out := c.applyBudget(events, deliveredCost)

	if body >= 10*1024 {
		t.Fatalf("test is not exercising the framing: %d bytes of body already exceeds the window", body)
	}
	held := 0
	for _, e := range out {
		if e.Kind == "deliveryHeld" {
			held = e.HeldCount
		}
	}
	if held == 0 {
		t.Fatalf("delivered all 60 messages (%d bytes of body) without closing a %d-byte window",
			body, 10*1024)
	}
}

// The bug this pins was the worst of the set: the window closed, the
// notice told the reader to call hub_catch_up, the reader did, confirmed
// what the walk returned — and release matched cursors EXACTLY, so a
// pulled cursor was absent from the ledger, nothing was released, the
// window stayed shut, and the closure was never announced again. The
// documented cure silently did nothing, permanently.
func TestConfirmingACursorThatWasPulledRatherThanPushedReopensTheWindow(t *testing.T) {
	b := testBudget(t)
	b.windowBytes = 1000
	b.charge("", "pushed-1", 1200)
	if !b.closed() {
		t.Fatal("window did not close")
	}
	b.hold("", "held-2")

	// What hub_catch_up hands over is recorded at zero cost — it spends no
	// push budget, but it must have a POSITION.
	b.note("", "held-2")
	b.note("", "walked-3")

	if skipped := b.release("", "walked-3"); !skipped {
		t.Error("releasing past a held message did not report the skip")
	}
	if b.closed() {
		t.Fatal("the window is still shut after confirming what the reader actually read")
	}
}

// Confirming past a message the window refused to deliver moves the read
// position over content the reader never saw. That must be reported, not
// silent — it is the same contiguous-watermark rule a live message shown
// mid-gap obeys.
func TestReleasingPastAHeldMessageReportsTheSkip(t *testing.T) {
	b := testBudget(t)
	b.charge("", "a", 1)
	b.hold("", "b")
	b.note("", "c")

	if skipped := b.release("", "a"); skipped {
		t.Error("releasing before the held message reported a skip")
	}
	if skipped := b.release("", "c"); !skipped {
		t.Error("releasing past the held message did not report a skip")
	}
}

// An unknown cursor cannot locate a prefix, so nothing may be released —
// but it is evidence the reader is reading, and a closure that is
// announced once and never again leaves the channel looking alive while
// everything is held.
func TestAnUnknownCursorReleasesNothingButMakesTheClosureAnnounceableAgain(t *testing.T) {
	b := testBudget(t)
	b.windowBytes = 100
	b.charge("", "known", 500)
	if announce, _ := b.hold("", "h1"); !announce {
		t.Fatal("first closure was not announced")
	}
	if announce, _ := b.hold("", "h2"); announce {
		t.Fatal("announced twice in a row")
	}

	b.release("", "a-cursor-this-ledger-never-saw")

	if b.bytes != 500 {
		t.Fatalf("bytes = %d, want 500 — an unlocatable cursor must not release", b.bytes)
	}
	if announce, _ := b.hold("", "h3"); !announce {
		t.Fatal("the closure stayed silent after an unlocatable confirm")
	}
}

// Holding this client's own notices meant the mechanism suppressed the
// only messages that could reopen it: the held-window announcement and
// the confirm reminder are the escape hatch.
func TestAClosedWindowStillDeliversThisClientsOwnNotices(t *testing.T) {
	c := &Conn{budget: newBudget()}
	c.budget.windowBytes = 100
	c.budget.charge("", "c0", 500)

	out := c.applyBudget([]Event{
		msg("m1", 10),
		{Kind: "confirmReminder", Text: "c0"},
		{Kind: "rosterComplete"},
	}, pushDeliveredCost)

	var kinds []string
	for _, e := range out {
		kinds = append(kinds, e.Kind)
	}
	for _, want := range []string{"confirmReminder", "rosterComplete"} {
		found := false
		for _, k := range kinds {
			if k == want {
				found = true
			}
		}
		if !found {
			t.Errorf("a closed window swallowed %q, which is how it reopens; got %v", want, kinds)
		}
	}
	for _, k := range kinds {
		if k == "msg" {
			t.Error("a closed window delivered an ordinary message")
		}
	}
}

// The estimate charged at render time enforces the window during a burst;
// the correction replaces it rather than adding a second entry.
func TestAdjustingACostCorrectsTheEntryRatherThanDoubleCharging(t *testing.T) {
	b := testBudget(t)
	b.charge("", "c1", 100)
	b.adjust("c1", 180)
	if b.bytes != 180 {
		t.Fatalf("bytes = %d, want 180", b.bytes)
	}
	if len(b.outstanding) != 1 {
		t.Fatalf("ledger has %d entries, want 1 — the message was counted twice", len(b.outstanding))
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

// One window shared by several connections must release per CONNECTION,
// not per position. A confirm says "I have read up to here" about one
// conversation; another connection's entries interleaved before it are
// still unread and still occupying the reader. Releasing them credits
// back budget nobody confirmed — and does it exactly when several
// conversations are busy, which is when the ceiling is load-bearing.
func TestASharedWindowReleasesOnlyTheConfirmingConnection(t *testing.T) {
	b := newBudget()
	b.charge("alpha", "a1", 100)
	b.charge("beta", "b1", 100)
	b.charge("alpha", "a2", 100)
	b.charge("beta", "b2", 100)

	// alpha confirms its latest. Only alpha's 200 bytes come back.
	b.release("alpha", "a2")
	b.mu.Lock()
	gotBytes, gotEntries := b.bytes, len(b.outstanding)
	b.mu.Unlock()
	if gotBytes != 200 {
		t.Fatalf("expected beta's 200 bytes to stay charged, window holds %d", gotBytes)
	}
	if gotEntries != 2 {
		t.Fatalf("expected beta's two entries to remain, got %d", gotEntries)
	}
	for _, ch := range b.outstanding {
		if ch.owner != "beta" {
			t.Fatalf("expected only beta's entries to survive, found %q", ch.owner)
		}
	}

	// And beta's own confirm releases beta's.
	b.release("beta", "b2")
	b.mu.Lock()
	gotBytes, gotEntries = b.bytes, len(b.outstanding)
	b.mu.Unlock()
	if gotBytes != 0 || gotEntries != 0 {
		t.Fatalf("expected an empty window after both confirmed, got %d bytes / %d entries",
			gotBytes, gotEntries)
	}
}

// A cursor belonging to another connection locates nothing, so it
// releases nothing — the client refuses such a confirm before it reaches
// here, and this is the second line of that defence.
func TestAForeignCursorReleasesNothing(t *testing.T) {
	b := newBudget()
	b.charge("alpha", "a1", 100)
	b.charge("beta", "b1", 100)

	b.release("beta", "a1")
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bytes != 200 || len(b.outstanding) != 2 {
		t.Fatalf("expected nothing released for a foreign cursor, got %d bytes / %d entries",
			b.bytes, len(b.outstanding))
	}
}
