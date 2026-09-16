package mcptools

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/server"
	"github.com/secforge/harness-transport/deliver"
	"github.com/secforge/mcp-hub/internal/harness"
)

// inPushMode makes this process look like an MCP server launched by a
// Claude Code session, which is the only thing that selects push mode.
func inPushMode(t *testing.T) {
	t.Helper()
	t.Setenv(deliver.EnvClaudeSocket, "/nonexistent/test.sock")
	if !harness.PushMode() {
		t.Fatal("push mode did not engage with a messaging socket set")
	}
}

// toolsListJSON is the tools/list response exactly as the model receives
// it — the only view that settles what the model can see, as opposed to
// what the source appears to register.
func toolsListJSON(t *testing.T) string {
	t.Helper()
	s := server.NewMCPServer("test", "0")
	NewHub().Register(s)
	res := s.HandleMessage(context.Background(),
		[]byte(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	raw, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("marshalling tools/list: %v", err)
	}
	return string(raw)
}

func registeredToolNames(t *testing.T) []string {
	t.Helper()
	raw := toolsListJSON(t)
	var names []string
	for _, part := range strings.Split(raw, `"name":"`)[1:] {
		names = append(names, part[:strings.Index(part, `"`)])
	}
	return names
}

// A tool the model can see is a tool it will eventually call. Where events
// arrive on their own, a blocking wait spends a whole turn on a message
// that would have come anyway.
func TestHubWaitIsNotOfferedWhenTheHarnessTakesDeliveries(t *testing.T) {
	inPushMode(t)
	for _, n := range registeredToolNames(t) {
		if n == "hub_wait" {
			t.Fatal("hub_wait is still registered in push mode")
		}
	}
}

// hub_receive drains the same buffer the push drains. Offering both is
// offering a race, and the tool that loses it answers "nothing here" to a
// model that just saw a message arrive.
func TestHubReceiveIsNotOfferedWhenTheHarnessTakesDeliveries(t *testing.T) {
	inPushMode(t)
	for _, n := range registeredToolNames(t) {
		if n == "hub_receive" {
			t.Fatal("hub_receive is still registered in push mode")
		}
	}
}

// What replaces them must still be there: history is asked of the server,
// not of the drained buffer.
func TestPushModeStillOffersTheServerBackedReads(t *testing.T) {
	inPushMode(t)
	want := map[string]bool{"hub_catch_up": false, "hub_read": false, "hub_confirm": false}
	for _, n := range registeredToolNames(t) {
		if _, ok := want[n]; ok {
			want[n] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("%s is missing in push mode — nothing else can reach history", name)
		}
	}
}

func TestHubWaitIsStillOfferedWithoutAHarness(t *testing.T) {
	restore := harness.ClearEnvForTesting()
	defer restore()
	found := false
	for _, n := range registeredToolNames(t) {
		if n == "hub_wait" {
			found = true
		}
	}
	if !found {
		t.Fatal("hub_wait vanished for a client that has no other way to receive")
	}
}

// The machinery being gone is only half of it: a description that still
// tells the model to background a binary, follow a socket, or call a tool
// that was never registered sends it looking for something that does not
// exist — which is indistinguishable from the thing being broken.
func TestPushModeMentionsNoWaitingMachineryAnywhereTheModelCanSee(t *testing.T) {
	inPushMode(t)
	text := toolsListJSON(t)
	for _, banned := range []string{"hub_wait", "hub_receive", "wait --follow", "Monitor", "wait CLI"} {
		if strings.Contains(text, banned) {
			t.Errorf("tool descriptions still mention %q in push mode", banned)
		}
	}
}

// The connect guidance is where a reader is told how receiving works, so
// it is where a stale instruction does the most damage.
func TestConnectGuidanceInPushModeSaysNothingToStart(t *testing.T) {
	inPushMode(t)
	got := buildWaitBlock(context.Background(), nil, "reconnect somehow")
	for _, banned := range []string{"hub_wait", "--follow", "Monitor", "background"} {
		if strings.Contains(got, banned) {
			t.Errorf("push-mode connect guidance mentions %q:\n%s", banned, got)
		}
	}
	// Silence has to be given a meaning, or the reader supplies their own.
	if !strings.Contains(got, "Silence") {
		t.Errorf("push-mode guidance never says what silence means:\n%s", got)
	}
	if !strings.Contains(got, "hub_catch_up") {
		t.Errorf("push-mode guidance never names the one gap live delivery cannot cover:\n%s", got)
	}
}

// Every tool that hands a cursor to the model must give it a position in
// the delivery ledger, or confirming what it returned releases nothing
// and a closed delivery window never reopens. hub_read is the case that
// was missed, and it is the tool the skipped-hold notice recommends for
// recovering a held range — so the hole sat in the recovery path itself.
//
// Asserted against the SOURCE rather than by driving a server, because
// what matters is that no delivery site is left out: a behavioural test
// passes for the sites that exist and says nothing about the next one.
func TestEveryToolThatHandsOverACursorRecordsALedgerPosition(t *testing.T) {
	src, err := os.ReadFile("tools.go")
	if err != nil {
		t.Fatalf("reading tools.go: %v", err)
	}
	text := string(src)
	recorded := strings.Count(text, "sole(t, h).recordHandedOver(")
	noted := strings.Count(text, "NoteHandedOver(")
	// recordHandedOver marks a cursor as delivered; NoteHandedOver gives
	// it a ledger position. Every site doing the first must do the second,
	// minus recordHandedOver's own definition.
	if noted < recorded-1 {
		t.Fatalf("%d hand-over sites but only %d ledger positions — a tool hands the model a "+
			"cursor that a later confirm cannot locate", recorded-1, noted)
	}
}

// The connect note is assembled from a mode-dependent opening plus shared
// recovery guidance, which is exactly the shape that produces a sentence
// stated twice — as it did, live, the first time the new wording shipped.
func TestTheConnectNoteDoesNotSayTheSameThingTwice(t *testing.T) {
	inPushMode(t)
	got := buildWaitBlock(context.Background(), nil, "reconnect somehow")
	if strings.Count(got, "cut off in transit") > 1 {
		t.Errorf("the truncation rule is stated more than once:\n%s", got)
	}
}

// "Nothing to catch up" describes the SERVER's unread position. The
// client's own buffer is a different store, and a pull-only reader can
// hold undelivered events while the server truthfully reports nothing
// behind. Saying "live traffic will arrive normally" there is the
// opposite of true, since nothing is delivering it.
//
// Found live 2026-09-16: a session followed the connect instructions
// exactly, was told it was caught up, and was holding nine buffered
// events including the message it was being asked about.
func TestCaughtUpDoesNotClaimDeliveryWhileEventsSitInTheBuffer(t *testing.T) {
	restore := harness.ClearEnvForTesting()
	defer restore()

	for _, tc := range []struct {
		name       string
		buffered   bool
		wantPhrase string
		notPhrase  string
	}{
		{"nothing buffered", false, "live traffic will arrive normally", "hub_receive"},
		{"events waiting", true, "hub_receive", "live traffic will arrive normally"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := caughtUpText(tc.buffered)
			if !strings.Contains(text, tc.wantPhrase) {
				t.Errorf("result does not say %q:\n%s", tc.wantPhrase, text)
			}
			if strings.Contains(text, tc.notPhrase) {
				t.Errorf("result wrongly says %q:\n%s", tc.notPhrase, text)
			}
		})
	}
}

// Notes must accumulate. Assignment silently destroyed whichever lost the
// race — a confirm echo and a wire diagnostic landed in the same turn and
// the second overwrote the first, with nothing reporting that something
// had been produced and dropped.
func TestQueuedNotesAccumulateRatherThanOverwrite(t *testing.T) {
	h := &Hub{}
	h.noteAutoReconnect("first thing")
	h.noteAutoReconnect("second thing")
	h.noteAutoReconnect("")

	got := h.takeAutoReconnectNote()
	for _, want := range []string{"first thing", "second thing"} {
		if !strings.Contains(got, want) {
			t.Errorf("note %q lost %q", got, want)
		}
	}
	if again := h.takeAutoReconnectNote(); again != "" {
		t.Errorf("notes survived being taken: %q", again)
	}
}

// A note nobody reads must not grow without limit, and dropping some
// silently is the thing this function exists to stop.
func TestQueuedNotesAreBoundedAndSayWhenTrimmed(t *testing.T) {
	h := &Hub{}
	for i := 0; i < 200; i++ {
		h.noteAutoReconnect(strings.Repeat("x", 100))
	}
	got := h.takeAutoReconnectNote()
	if len(got) > maxQueuedNotes+200 {
		t.Errorf("notes grew to %d bytes, past the cap", len(got))
	}
	if !strings.Contains(got, "dropped") {
		t.Errorf("trimming was silent:\n%s", got[:200])
	}
}

// The push-mode branch must not fall through to the per-call walk, and
// the per-call walk must not disappear for everyone else. Asserted
// against the source because the difference is which branch runs, and a
// behavioural test would need a live server on both paths to show it.
func TestCatchUpDeliversByPushOnlyInPushMode(t *testing.T) {
	src, err := os.ReadFile("tools.go")
	if err != nil {
		t.Fatalf("reading tools.go: %v", err)
	}
	text := string(src)

	idx := strings.Index(text, "s.runCatchUpPush(")
	if idx < 0 {
		t.Fatal("the push-mode catch-up branch is gone")
	}
	// The guard must be the push-mode check, not something else that
	// happens to be nearby.
	before := text[max(0, idx-1200):idx]
	if !strings.Contains(before, "harness.PushMode()") {
		t.Error("the push drain is not gated on push mode")
	}
	// And it must go through the one-at-a-time slot: two backlogs
	// interleaving produce a stream neither conversation reads as its
	// own.
	if !strings.Contains(before, "catchUpSlot") {
		t.Error("the backlog walk is not serialized across connections")
	}
	// The per-call walk still has to exist for pull clients.
	if !strings.Contains(text, "catchUpDedupSkipLimit") {
		t.Error("the one-message-per-call walk is gone, which pull clients depend on")
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// In push mode the buffered-events answer must name no tool. hub_receive
// is not registered there, so the pull wording sends a reader looking for
// a call it does not have — and invites it to conclude the events are
// stuck, when they are already on their way. Two separate clients hit
// this within a minute of each other, live.
func TestBufferedEventsAdviceMatchesTheDeliveryMode(t *testing.T) {
	t.Setenv(harness.EnvClaudeSocketName, "/tmp/does-not-need-to-exist.sock")
	if !harness.PushMode() {
		t.Fatal("expected push mode with the harness socket set")
	}
	text := caughtUpText(true)
	if strings.Contains(text, "hub_receive") {
		t.Errorf("push mode must not name a tool that is not registered:\n%s", text)
	}
	for _, want := range []string{"arrive by push", "Nothing is stuck", "different store"} {
		if !strings.Contains(text, want) {
			t.Errorf("expected the push wording to say %q:\n%s", want, text)
		}
	}
}
