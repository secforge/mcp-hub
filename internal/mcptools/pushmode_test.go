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
	recorded := strings.Count(text, "h.recordHandedOver(")
	noted := strings.Count(text, "NoteHandedOver(")
	// recordHandedOver marks a cursor as delivered; NoteHandedOver gives
	// it a ledger position. Every site doing the first must do the second,
	// minus recordHandedOver's own definition.
	if noted < recorded-1 {
		t.Fatalf("%d hand-over sites but only %d ledger positions — a tool hands the model a "+
			"cursor that a later confirm cannot locate", recorded-1, noted)
	}
}
