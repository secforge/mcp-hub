package mcptools

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/secforge/mcp-hub/internal/connstore"
)

// The gap this closes: when the MCP process ends, every connection ends
// with it and the thing that would report that dies in the same instant.
// Measured live — four connections ended at 15:29 and the model learned
// of it half an hour later, from its user.
//
// "Connected" is the whole of the test: it means a run was holding this
// and did not deliberately let go, and at startup this process has
// connected nothing, so anything still marked belongs to a previous run.
func TestAPreviousRunsConnectionsAreReportedOnce(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	project := connstore.CurrentProject()
	target := connstore.Target{Link: "wss://example/550e8400-e29b-41d4-a716-446655440000", Project: project}
	if err := connstore.Upsert(target, connstore.Entry{
		PeerID: "550e8400-e29b-41d4-a716-446655440000", LocalName: "relay",
		LastConnectedAt: time.Now().UTC(), Connected: true,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	h := &Hub{}
	h.reportAbandonedConnections()
	note := h.takeAutoReconnectNote()
	if !strings.Contains(note, "relay") {
		t.Fatalf("expected the notice to name the connection, got: %s", note)
	}
	if !strings.Contains(note, "NOT restored") {
		t.Fatalf("expected it to say they were not restored, got: %s", note)
	}
	// Said once. A notice repeated on every call is one a reader learns
	// to skip, and this one matters exactly once.
	if again := h.takeAutoReconnectNote(); again != "" {
		t.Fatalf("expected nothing further queued, got: %s", again)
	}
}

// A connection given up ON PURPOSE is not reported: that was a decision,
// and repeating it back is nagging about something already done. An
// explicit disconnect clears the mark, which is what says so.
func TestADeliberateDisconnectIsNotReported(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	project := connstore.CurrentProject()
	target := connstore.Target{Link: "wss://example/550e8400-e29b-41d4-a716-446655440001", Project: project}
	if err := connstore.Upsert(target, connstore.Entry{
		PeerID: "550e8400-e29b-41d4-a716-446655440001", LocalName: "ops",
		LastConnectedAt: time.Now().UTC(), Connected: true,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := connstore.MarkDisconnected(target); err != nil {
		t.Fatalf("MarkDisconnected: %v", err)
	}

	h := &Hub{}
	h.reportAbandonedConnections()
	if note := h.takeAutoReconnectNote(); note != "" {
		t.Fatalf("expected silence about a deliberate disconnect, got: %s", note)
	}
}

// A connection that DROPPED is not reported either: the model was told
// while the process was still running, and the mark was cleared then.
// Only what a run was still holding when it ended is news.
func TestADroppedConnectionIsNotReported(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	project := connstore.CurrentProject()
	target := connstore.Target{Link: "wss://example/550e8400-e29b-41d4-a716-446655440002", Project: project}
	if err := connstore.Upsert(target, connstore.Entry{
		PeerID: "550e8400-e29b-41d4-a716-446655440002", LocalName: "dropped",
		LastConnectedAt: time.Now().UTC(), Connected: false,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	h := &Hub{}
	h.reportAbandonedConnections()
	if note := h.takeAutoReconnectNote(); note != "" {
		t.Fatalf("expected silence about a connection that already ended, got: %s", note)
	}
}

// Shutdown must NOT clear the mark. The process ending is exactly the
// case nothing else can report, so clearing it there would make a clean
// exit indistinguishable from a deliberate departure — which is the bug
// the whole notice exists to fix.
func TestShutdownLeavesTheMarkForTheNextRun(t *testing.T) {
	src, err := os.ReadFile("tools.go")
	if err != nil {
		t.Fatalf("reading tools.go: %v", err)
	}
	text := string(src)
	i := strings.Index(text, "func (h *Hub) Shutdown()")
	if i < 0 {
		t.Fatal("Shutdown is gone")
	}
	j := strings.Index(text[i:], "\n}\n")
	if j < 0 {
		t.Fatal("could not find the end of Shutdown")
	}
	if strings.Contains(text[i:i+j], "MarkDisconnected") {
		t.Error("Shutdown clears the mark, so a process that simply ended reads as one that left " +
			"on purpose and the next run says nothing")
	}
}

// The notice is PUSHED where the harness takes deliveries, and only
// queued where it does not. Queued, it waits for whatever tool is called
// first — which may be an hour away, or never, since the whole point is
// to say something the reader would otherwise have no reason to ask
// about.
//
// Asserted against the source because the difference is which path runs,
// and exercising the push half needs a live harness socket this suite
// deliberately does not have (see TestMain).
func TestTheStartupNoticeIsPushedWhenItCanBe(t *testing.T) {
	src, err := os.ReadFile("startupnotice.go")
	if err != nil {
		t.Fatalf("reading startupnotice.go: %v", err)
	}
	text := string(src)

	push := strings.Index(text, "pusher.Push(")
	queue := strings.Index(text, "h.noteAutoReconnect(note)")
	if push < 0 {
		t.Fatal("the notice is never pushed — it would wait for a tool call that may not come")
	}
	if queue < 0 {
		t.Fatal("the queued fallback is gone — a harness that cannot be pushed to would hear nothing")
	}
	if push > queue {
		t.Error("the queue comes first, so the push is unreachable or duplicates it")
	}
	// A push that worked must not also queue: the same notice twice is
	// worse than late.
	between := text[push:queue]
	if !strings.Contains(between, "return") {
		t.Error("a successful push does not return, so the notice would be delivered twice")
	}
}

// Said once means ACROSS restarts, not within one. An entry stays marked
// until something clears it, so a notice that only reads the mark reports
// the same connection on every start from then on — and ages into a lie,
// as it did live: a connection killed an hour earlier, whose session no
// longer existed, was announced as something a previous run had just been
// holding.
func TestReportingClearsTheMarkSoItIsNotSaidAgain(t *testing.T) {
	t.Setenv("MCP_HUB_CONNSTORE_DIR", t.TempDir())
	project := connstore.CurrentProject()
	target := connstore.Target{Link: "wss://example/550e8400-e29b-41d4-a716-446655440003", Project: project}
	if err := connstore.Upsert(target, connstore.Entry{
		PeerID: "550e8400-e29b-41d4-a716-446655440003", LocalName: "stale",
		LastConnectedAt: time.Now().UTC(), Connected: true,
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	first := &Hub{}
	first.reportAbandonedConnections()
	if note := first.takeAutoReconnectNote(); !strings.Contains(note, "stale") {
		t.Fatalf("expected the first run to report it, got: %s", note)
	}

	// A later start reads the same store and must say nothing.
	second := &Hub{}
	second.reportAbandonedConnections()
	if note := second.takeAutoReconnectNote(); note != "" {
		t.Fatalf("expected silence the second time, got: %s", note)
	}
	// And the entry itself survives — only the mark was cleared, so the
	// link, name and read position are all still there to reconnect with.
	stored, ok, _ := connstore.Get(target)
	if !ok {
		t.Fatal("expected the entry to survive being reported")
	}
	if stored.Connected {
		t.Fatal("expected the mark to be cleared once reported")
	}
	if stored.LocalName != "stale" {
		t.Fatalf("expected the name to survive, got %q", stored.LocalName)
	}
}
